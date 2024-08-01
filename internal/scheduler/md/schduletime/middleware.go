package schduletime

import (
	"context"
	"github.com/ecodeclub/ecron/internal/preempt"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"log/slog"
	"time"
)

type NextTimeMiddlewareBuilder struct {
	dao    storage.TaskDAO
	logger *slog.Logger
}

func NewNextTimeMiddlewareBuilder(dao storage.TaskDAO, logger *slog.Logger) *NextTimeMiddlewareBuilder {
	return &NextTimeMiddlewareBuilder{
		dao:    dao,
		logger: logger,
	}
}

func (m *NextTimeMiddlewareBuilder) Build() preempt.Middleware {
	return func(next preempt.HandleFunc) preempt.HandleFunc {
		return func(ctx context.Context, t task.Task) error {

			// todo 这里可以保存开始时间，做定长周期执行的job
			err := next(ctx, t)

			defer func() {
				m.setNextTime(t)
			}()
			return err
		}
	}
}

func (p *NextTimeMiddlewareBuilder) setNextTime(t task.Task) {
	next, err := t.NextTime()
	if err != nil {
		p.logger.Error("计算任务下一次执行时间失败",
			slog.Int64("TaskID", t.ID),
			slog.Any("error", err))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	if next.IsZero() {
		err := p.dao.Stop(ctx, t.ID)
		if err != nil {
			p.logger.Error("停止任务调度失败",
				slog.Int64("TaskID", t.ID),
				slog.Any("error", err))
		}
	}
	err = p.dao.UpdateNextTime(ctx, t.ID, next)
	if err != nil {
		p.logger.Error("更新下一次执行时间出错",
			slog.Int64("TaskID", t.ID),
			slog.Any("error", err))
	}
}
