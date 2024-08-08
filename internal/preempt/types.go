package preempt

import (
	"context"
	"errors"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"sync"
	"time"
)

var (
	ErrLeaseNotHold       = errors.New("租约未持有")
	ErrLeaseRetryMaxTimes = errors.New("超过重试次数")
)

type Preempter interface {
	Lease
	Preempt(ctx context.Context) (task.Task, error)

	//Release(ctx context.Context, t task.Task) error

	// AutoRefresh 调用者调用ctx的cancel之后，才会关闭掉自动续约
	// 返回一个Status 的ch,有一定缓存，需要自行取走数据
	AutoRefresh(ctx context.Context, t task.Task) (s <-chan Status, err error)
	//Refresh(ctx context.Context, t task.Task) (err error)
}

// Lease 需要保证不能处理到别人到租约
type Lease interface {
	// Refresh 保证幂等
	Refresh(ctx context.Context, t task.Task) error
	// Release 保证幂等
	Release(ctx context.Context, t task.Task) error
}

type Status interface {
	Err() error
	getStatus() LeaseStatus
	getMsg() string
}

type DefaultLeaseStatus struct {
	s   LeaseStatus
	err error
	msg string
}

func (d DefaultLeaseStatus) Err() error {
	return d.err
}

func (d DefaultLeaseStatus) getStatus() LeaseStatus {
	return d.s
}

func (d DefaultLeaseStatus) getMsg() string {
	return d.msg
}

type LeaseStatus uint8

const (
	LeaseStatusUnknown LeaseStatus = iota
	LeaseStatusStarted
	LeaseStatusWaitNext
	LeaseStatusSuccessAndExit
	LeaseStatusTimeout
	LeaseStatusTimeoutMaxTimes
	LeaseStatusFail
)

type DBPreempter struct {
	dao             storage.TaskDAO
	refreshInterval time.Duration
	logger          *slog.Logger

	// with options
	debug         bool
	buffSize      uint8
	maxRetryTimes uint8
}

func NewDBPreempter(dao storage.TaskDAO, refreshInterval time.Duration, logger *slog.Logger) *DBPreempter {
	return &DBPreempter{
		dao:             dao,
		refreshInterval: refreshInterval,
		logger:          logger,

		debug:         false,
		buffSize:      10,
		maxRetryTimes: 3,
	}
}

func (d *DBPreempter) Preempt(ctx context.Context) (task.Task, error) {
	ctx2, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()
	return d.dao.Preempt(ctx2)
}

func (d *DBPreempter) refreshTask(ctx context.Context, ticker *time.Ticker, t task.Task) error {
	for {
		select {
		case <-ticker.C:
			err := d.Refresh(ctx, t)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (d *DBPreempter) autoRefresh(ctx context.Context, t task.Task) error {

	ticker := time.NewTicker(d.refreshInterval)
	defer ticker.Stop()
	for {
		err := d.refreshTask(ctx, ticker, t)
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			var i = 0
			for i < int(d.maxRetryTimes) {
				err = d.Refresh(ctx, t)
				switch {
				case err == nil:
					break
				case errors.Is(err, context.DeadlineExceeded):
					i++
					continue
				default:
					return err
				}
			}

			if err != nil {
				return err
			}
			ticker.Reset(d.refreshInterval)
		default:
			return err
		}
	}
}

func newDefaultLeaseStatus(err error) DefaultLeaseStatus {
	return DefaultLeaseStatus{
		err: err,
	}
}

func (d *DBPreempter) AutoRefreshV1(ctx context.Context, t task.Task) (s <-chan Status, err error) {
	sch := make(chan Status, d.buffSize)
	go func() {
		err2 := d.autoRefresh(ctx, t)
		sch <- newDefaultLeaseStatus(err2)
	}()

	return sch, nil
}

func (d *DBPreempter) AutoRefresh(ctx context.Context, t task.Task) (s <-chan Status, err error) {

	sch := make(chan Status, d.buffSize)

	go func() {
		retry := make(chan struct{}, 1)
		ticker := time.NewTicker(d.refreshInterval)

		once := sync.Once{}
		defer func() {
			once.Do(func() {
				d.release(ticker, ctx, t)
			})
		}()
		count := 0
		first := true
		for {
			select {
			case <-ticker.C:
				err := d.Refresh(ctx, t)
				switch {
				case err == nil:
					count = 0
					if d.debug {
						var status Status
						if first {
							status = DefaultLeaseStatus{
								s:   LeaseStatusStarted,
								msg: "续约成功,开始自动续约",
							}
							first = false
						} else {
							status = DefaultLeaseStatus{
								s:   LeaseStatusWaitNext,
								msg: "续约成功,等待下次续约",
							}
						}

						select {
						case sch <- status:
						default:
						}
					}
					continue
				case errors.Is(err, ErrLeaseNotHold):
					status := DefaultLeaseStatus{
						s:   LeaseStatusFail,
						msg: "续约失败，当前租约已经被别人所抢占",
						err: err,
					}
					once.Do(func() {
						d.release(ticker, ctx, t)
					})
					sch <- status
					return
				case errors.Is(err, context.DeadlineExceeded):
					count++
					if d.debug {
						status := DefaultLeaseStatus{
							s:   LeaseStatusTimeout,
							msg: "续约超时，待下次重试",
							// ？ 这里要不要放err
						}
						select {
						case sch <- status:
						default:
						}
					}
					retry <- struct{}{}
				default:
					status := DefaultLeaseStatus{
						s:   LeaseStatusUnknown,
						msg: "续约错误，未知异常",
						err: err,
					}
					once.Do(func() {
						d.release(ticker, ctx, t)
					})
					sch <- status
					return
				}
			case <-retry:
				if uint8(count) == d.maxRetryTimes {
					status := DefaultLeaseStatus{
						s:   LeaseStatusTimeoutMaxTimes,
						msg: "自动续约超过最大重试次数",
						err: ErrLeaseRetryMaxTimes,
					}
					once.Do(func() {
						d.release(ticker, ctx, t)
					})
					sch <- status
					return
				}
				// 睡一段时间
				time.Sleep(time.Second)
				err := d.Refresh(ctx, t)
				switch {
				case err == nil:
					count = 0
					if d.debug {
						var status Status
						if first {
							status = DefaultLeaseStatus{
								s:   LeaseStatusStarted,
								msg: "续约成功,开始自动续约",
							}
							first = false
						} else {
							status = DefaultLeaseStatus{
								s:   LeaseStatusWaitNext,
								msg: "续约成功,等待下次续约",
							}
						}
						select {
						case sch <- status:
						default:
						}
					}
					continue
				case errors.Is(err, ErrLeaseNotHold):
					status := DefaultLeaseStatus{
						s:   LeaseStatusFail,
						msg: "续约失败，当前租约已经被别人所抢占",
						err: err,
					}
					sch <- status
					once.Do(func() {
						d.release(ticker, ctx, t)
					})
					return
				case errors.Is(err, context.DeadlineExceeded):
					count++
					if d.debug {
						status := DefaultLeaseStatus{
							s:   LeaseStatusTimeout,
							msg: "续约超时，待下次重试",
							// ？ 这里要不要放err
						}
						select {
						case sch <- status:
						default:
						}
					}
					retry <- struct{}{}
				default:
					status := DefaultLeaseStatus{
						s:   LeaseStatusUnknown,
						msg: "续约错误，未知异常",
						err: err,
					}
					once.Do(func() {
						d.release(ticker, ctx, t)
					})
					sch <- status
					return
				}
			case <-ctx.Done():
				status := DefaultLeaseStatus{
					s:   LeaseStatusSuccessAndExit,
					msg: "续约退出",
					// 这里是否要返回错误
					err: ctx.Err(),
				}
				once.Do(func() {
					d.release(ticker, ctx, t)
				})
				sch <- status
				return
			}

		}
	}()

	return sch, nil
}

func (d *DBPreempter) release(ticker *time.Ticker, ctx context.Context, t task.Task) {
	ticker.Stop()
	err2 := d.Release(ctx, t)
	if err2 != nil {
		time.AfterFunc(d.refreshInterval, func() {
			_ = d.Release(ctx, t)
		})
	}
}

func (d *DBPreempter) Refresh(ctx context.Context, t task.Task) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	err := d.dao.UpdateUtime(ctx, t.ID)
	defer cancel()
	return err
}

func (d *DBPreempter) Release(ctx context.Context, t task.Task) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	err := d.dao.Release(ctx, t)
	if err != nil {
		d.logger.Error("释放任务失败",
			slog.Int64("TaskID", t.ID),
			slog.Any("error", err))
	}
	return err
}

type PreempterUsage interface {
	DoPreempt(ctx context.Context, f func(ctx context.Context, t task.Task) error) error
}

type Middleware func(next HandleFunc) HandleFunc

type HandleFunc func(ctx context.Context, t task.Task) error

type PreempterExample struct {
	p1   Preempter
	mdls []Middleware
}

func NewPreempterExample(p Preempter, mdls ...Middleware) *PreempterExample {
	return &PreempterExample{
		p1:   p,
		mdls: mdls,
	}
}

func (p *PreempterExample) DoPreempt(ctx context.Context, f func(ctx context.Context, t task.Task) error) error {

	if ctx.Err() != nil {
		return ctx.Err()
	}
	myTask, err := p.p1.Preempt(ctx)
	if err != nil {
		return err
	}
	// 这里有bug
	eg, ectx := errgroup.WithContext(ctx)
	cancel, cancelFunc := context.WithCancel(ectx)
	eg.Go(func() error {
		ch, err := p.p1.AutoRefresh(cancel, myTask)
		if err != nil {
			return err
		}
		select {
		case s := <-ch:
			if s.Err() != nil {
				return s.Err()
			}

		case <-ectx.Done():
		}
		return nil
	})

	eg.Go(func() error {

		defer func() { cancelFunc() }()

		for i := len(p.mdls) - 1; i >= 0; i-- {
			f = p.mdls[i](f)
		}

		return f(ectx, myTask)
	})

	return eg.Wait()

}
