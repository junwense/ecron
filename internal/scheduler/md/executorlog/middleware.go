package executorlog

import (
	"context"
	"errors"
	"github.com/ecodeclub/ecron/internal/preempt"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"log/slog"
	"time"
)

type ExecutorHisMiddlewareBuilder struct {
	executionDAO storage.ExecutionDAO
	logger       *slog.Logger
}

func NewExecutorHisMiddlewareBuilder(executionDAO storage.ExecutionDAO, logger *slog.Logger) *ExecutorHisMiddlewareBuilder {
	return &ExecutorHisMiddlewareBuilder{
		executionDAO: executionDAO,
		logger:       logger,
	}
}

func (m *ExecutorHisMiddlewareBuilder) Build() preempt.Middleware {
	return func(next preempt.HandleFunc) preempt.HandleFunc {
		return func(ctx context.Context, t task.Task) error {
			m.markStatus(t.ID, task.ExecStatusStarted)
			err := next(ctx, t)

			defer func() {
				switch {
				case errors.Is(err, context.DeadlineExceeded):
					m.markStatus(t.ID, task.ExecStatusDeadlineExceeded)
				case errors.Is(err, context.Canceled):
					m.markStatus(t.ID, task.ExecStatusCancelled)
				case err == nil:
					m.markStatus(t.ID, task.ExecStatusSuccess)
				default:
					m.markStatus(t.ID, task.ExecStatusFailed)
				}
			}()

			return err
		}
	}
}

func (p *ExecutorHisMiddlewareBuilder) markStatus(tid int64, status task.ExecStatus) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	err := p.executionDAO.InsertExecStatus(ctx, tid, status)
	if err != nil {
		p.logger.Error("记录任务执行失败",
			slog.Int64("TaskID", tid),
			slog.Any("error", err))
	}
}
