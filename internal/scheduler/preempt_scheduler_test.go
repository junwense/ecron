package scheduler

import (
	"errors"
	"github.com/ecodeclub/ecron/internal/executor"
	executormocks "github.com/ecodeclub/ecron/internal/executor/mocks"
	"github.com/ecodeclub/ecron/internal/preempt"
	"github.com/ecodeclub/ecron/internal/storage"
	daomocks "github.com/ecodeclub/ecron/internal/storage/mocks"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"golang.org/x/net/context"
	"golang.org/x/sync/semaphore"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestPreemptScheduler_refreshTask(t *testing.T) {
	testCases := []struct {
		name            string
		mock            func(ctrl *gomock.Controller) (storage.TaskDAO, storage.ExecutionDAO, executor.Executor)
		refreshInterval time.Duration
		limiter         *semaphore.Weighted
		wantErr         error
		wantStatus      preempt.LeaseStatus
		ctxFn           func() context.Context
	}{
		{
			name: "UpdateUtime error",
			mock: func(ctrl *gomock.Controller) (storage.TaskDAO, storage.ExecutionDAO, executor.Executor) {
				td := daomocks.NewMockTaskDAO(ctrl)
				hd := daomocks.NewMockExecutionDAO(ctrl)
				exec := executormocks.NewMockExecutor(ctrl)

				td.EXPECT().UpdateUtime(gomock.Any(), int64(1)).Return(errors.New("UpdateUtime error"))
				td.EXPECT().Release(gomock.Any(), gomock.Any()).Return(nil)

				return td, hd, exec
			},
			refreshInterval: time.Second * 1,
			limiter:         semaphore.NewWeighted(10),
			ctxFn: func() context.Context {
				return context.Background()
			},
			wantErr:    errors.New("UpdateUtime error"),
			wantStatus: preempt.LeaseStatusUnknown,
		},
		{
			name: "context被取消了",
			mock: func(ctrl *gomock.Controller) (storage.TaskDAO, storage.ExecutionDAO, executor.Executor) {
				td := daomocks.NewMockTaskDAO(ctrl)
				hd := daomocks.NewMockExecutionDAO(ctrl)
				exec := executormocks.NewMockExecutor(ctrl)

				td.EXPECT().UpdateUtime(gomock.Any(), int64(1)).AnyTimes().Return(nil)
				td.EXPECT().Release(gomock.Any(), gomock.Any()).Return(nil)

				return td, hd, exec
			},
			refreshInterval: time.Second * 1,
			limiter:         semaphore.NewWeighted(10),
			wantErr:         context.Canceled,
			wantStatus:      preempt.LeaseStatusSuccessAndExit,
			ctxFn: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 3)
					cancel()
				}()
				return ctx
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			td, _, _ := tc.mock(ctrl)
			logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
			preempter := preempt.NewDBPreempter(td, tc.refreshInterval, logger)
			sch, err2 := preempter.AutoRefresh(tc.ctxFn(), task.Task{
				ID: 1,
			})
			assert.NoError(t, err2)
			select {
			case s := <-sch:
				assert.Equal(t, tc.wantErr, s.Err())
				//assert.Equal(t, tc.wantStatus, s.getStatus())
			}
			//s := NewPreemptScheduler(td, hd, tc.refreshInterval, tc.limiter, logger)
			//ticker := time.NewTicker(time.Second)
			//err := preempter.refreshTask(tc.ctxFn(), ticker, 1)

		})
	}
}
