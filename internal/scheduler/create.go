package scheduler

import (
	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/ecodeclub/ecron/internal/preempt"
	"github.com/ecodeclub/ecron/internal/scheduler/md/executorlog"
	"github.com/ecodeclub/ecron/internal/scheduler/md/schduletime"
	"github.com/ecodeclub/ecron/internal/storage"
	"golang.org/x/sync/semaphore"
	"log/slog"
	"time"
)

func NewPreemptScheduler(dao storage.TaskDAO, executionDAO storage.ExecutionDAO,
	refreshInterval time.Duration, limiter *semaphore.Weighted, logger *slog.Logger) *PreemptScheduler {

	md1 := executorlog.NewExecutorHisMiddlewareBuilder(executionDAO, logger)
	md2 := schduletime.NewNextTimeMiddlewareBuilder(dao, logger)
	preempter := preempt.NewDBPreempter(dao, refreshInterval, logger)
	example := preempt.NewPreempterExample(preempter, md1.Build(), md2.Build())
	return &PreemptScheduler{
		limiter:   limiter,
		executors: make(map[string]executor.Executor),
		logger:    logger,

		pe: example,
	}
}
