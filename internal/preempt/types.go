package preempt

import (
	"context"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"time"
)

type Preempter interface {
	Preempt(ctx context.Context) (task.Task, error)
	// AutoRefresh 返回值 chan用于获取错误，第二个chan用于退出，一般是在go func里调用
	AutoRefresh(ctx context.Context, t task.Task) (errch <-chan error, cancelch chan<- struct{}, err error)
	Release(ctx context.Context, t task.Task) error
}

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

type PreempterUsage interface {
	DoPreempt(ctx context.Context, f func(ctx context.Context, t task.Task) error) error
}

type DBPreempter struct {
	dao             storage.TaskDAO
	refreshInterval time.Duration
	logger          *slog.Logger
}

func NewDBPreempter(dao storage.TaskDAO, refreshInterval time.Duration, logger *slog.Logger) *DBPreempter {
	return &DBPreempter{
		dao:             dao,
		refreshInterval: refreshInterval,
		logger:          logger,
	}
}

type Middleware func(next HandleFunc) HandleFunc

type HandleFunc func(ctx context.Context, t task.Task) error

func (d *DBPreempter) Preempt(ctx context.Context) (task.Task, error) {
	ctx2, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()
	return d.dao.Preempt(ctx2)
}

func (d *DBPreempter) AutoRefresh(ctx context.Context, t task.Task) (errch <-chan error, cancelch chan<- struct{}, err error) {
	ech := make(chan error, 1)
	cch := make(chan struct{}, 1)
	ticker := time.NewTicker(d.refreshInterval)
	go func() {
		defer func() { ticker.Stop() }()
		for {
			select {
			case <-ticker.C:
				ctx2, cancel := context.WithTimeout(context.Background(), time.Second*3)
				err := d.dao.UpdateUtime(ctx2, t.ID)
				cancel()
				if err != nil {
					ech <- err
				}
			case <-ctx.Done():
				ech <- ctx.Err()
			case <-cch:
				return
			}

		}
	}()

	return ech, cch, nil
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

func (p *PreempterExample) DoPreempt(ctx context.Context, f func(ctx context.Context, t task.Task) error) error {

	if ctx.Err() != nil {
		return ctx.Err()
	}
	myTask, err := p.p1.Preempt(ctx)
	if err != nil {
		return err
	}

	eg, ectx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		ch, cancel, err := p.p1.AutoRefresh(ectx, myTask)
		defer func() { close(cancel) }()
		if err != nil {
			return err
		}
		select {
		case <-ch:
			return <-ch
		case <-ectx.Done():
		}
		return nil
	})

	eg.Go(func() error {

		for i := len(p.mdls) - 1; i >= 0; i-- {
			f = p.mdls[i](f)
		}

		return f(ectx, myTask)
	})

	defer func() {
		_ = p.p1.Release(ctx, myTask)
	}()

	return eg.Wait()

}
