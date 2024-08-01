package scheduler

import (
	"context"
	"errors"
	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/ecodeclub/ecron/internal/preempt"
	"github.com/ecodeclub/ecron/internal/task"
	"golang.org/x/sync/semaphore"
	"log/slog"
)

type PreemptScheduler struct {
	executors map[string]executor.Executor
	limiter   *semaphore.Weighted
	logger    *slog.Logger

	pe preempt.PreempterUsage
}

func (p *PreemptScheduler) RegisterExecutor(execs ...executor.Executor) {
	for _, exec := range execs {
		p.executors[exec.Name()] = exec
	}
}

func (p *PreemptScheduler) ScheduleV1(ctx context.Context) error {
	for {
		err := p.limiter.Acquire(ctx, 1)
		if err != nil {
			return err
		}
		go func() {
			// 这里错误要不要处理 ，可以直接返回，
			var t1 task.Task
			err = p.pe.DoPreempt(ctx, func(ctx context.Context, t task.Task) error {

				exec, ok := p.executors[t.Executor]
				if !ok {
					p.logger.Error("找不到任务的执行器",
						slog.Int64("TaskID", t.ID),
						slog.String("Executor", t.Executor))
					//p.releaseTask(t)
					return errors.New("找不到任务的执行器")
				}

				t1 = t
				err := exec.Run(ctx, t)
				return err
			})

			defer func() { p.limiter.Release(1) }()

			if err != nil {
				p.logger.Error(err.Error(),
					slog.Int64("TaskID", t1.ID),
					slog.String("Executor", t1.Executor))
			}
		}()

	}
}
