package gds

import (
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/integration-system/gds/cluster"
	"github.com/integration-system/gds/jobs"
	"github.com/integration-system/gds/utils"
	"sync"
	"time"
)

const (
	DefaultJobExecutionTimeout = 5 * time.Second
)

var (
	ErrJobTimeoutExceeded = errors.New("job execution timeout exceeded")
)

type executor interface {
	Shutdown(ctx context.Context) error
	AddJob(job jobs.Job)
	CancelJob(key string) bool
}

type defaultExecutor struct {
	registry         executorRegistry
	wg               sync.WaitGroup
	lock             sync.Mutex
	futures          map[string]*future
	executedJobsCh   chan<- cluster.JobExecuted
	executionTimeout time.Duration

	logger hclog.Logger
}

func (e *defaultExecutor) CancelJob(job string) bool {
	e.lock.Lock()
	defer e.lock.Unlock()

	fu, ok := e.futures[job]
	if !ok {
		return false
	}

	fu.Cancel()
	delete(e.futures, job)
	return true
}

func (e *defaultExecutor) Shutdown(ctx context.Context) error {
	// try release not running triggers
	e.lock.Lock()
	for key, f := range e.futures {
		if !f.IsRunning() {
			f.Cancel()
			delete(e.futures, key)
		}
	}
	e.lock.Unlock()

	// await all running triggers
	awaitRunning := make(chan struct{})
	go func() {
		defer close(awaitRunning)
		e.wg.Wait()
	}()

	select {
	case <-ctx.Done():
	case <-awaitRunning:
		close(e.executedJobsCh)
	}
	return nil
}

func (e *defaultExecutor) AddJob(job jobs.Job) {
	// Create a new future for the job
	fu := &future{
		job:      job,
		running:  utils.NewAtomicBool(false),
		canceled: utils.NewAtomicBool(false),
	}
	f := e.makeF(fu)

	// Calculate the delay until the job should be triggered
	dur := calculateDurationUntilNextTrigger(job)

	// Schedule the job
	fu.timer = time.AfterFunc(dur, f)

	// Add the future to the map
	e.lock.Lock()
	defer e.lock.Unlock()
	e.futures[job.Key()] = fu
}

// calculateDurationUntilNextTrigger computes the duration from now until the next trigger time
func calculateDurationUntilNextTrigger(job jobs.Job) time.Duration {
	now := time.Now()
	nextTime := job.NextTriggerTime()
	if now.After(nextTime) {
		return 0
	}
	return nextTime.Sub(now)
}

func (e *defaultExecutor) makeF(f *future) func() {
	return func() {
		if f.IsCanceled() {
			return
		}

		now := time.Now()
		e.runJob(f, now)
	}
}

func (e *defaultExecutor) runJob(f *future, now time.Time) {
	defer e.cleanup(f)
	f.SetRunning()

	// 获取 executor
	exec, ok := e.registry.GetExecutor(f.job.Type())
	if !ok {
		e.logger.Error(fmt.Sprintf("defaultExecutor: not found executor for job type: %v", f.job.Type()))
		return
	}

	// 执行
	e.wg.Add(1)
	defer e.wg.Done()
	err := e.executeWithTimeout(exec, f.job)
	if err != nil {
		e.logger.Warn(fmt.Sprintf("defaultExecutor: job %s type %s has finished with err '%v'", f.job.Key(), f.job.Type(), err))
	}

	// Job 执行完毕后，结果发送到 ch
	e.executedJobsCh <- cluster.JobExecuted{
		JobKey:       f.job.Key(),
		Error:        err.Error(),
		ExecutedTime: now,
	}
}

func (e *defaultExecutor) executeWithTimeout(exec JobExecutor, job jobs.Job) error {
	doneChan := make(chan error, 1)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				doneChan <- fmt.Errorf("%v", err)
			}
		}()
		doneChan <- exec(job)
	}()

	if e.executionTimeout > 0 {
		timeout := time.NewTimer(e.executionTimeout)
		defer timeout.Stop()
		select {
		case <-timeout.C:
			return ErrJobTimeoutExceeded
		case err := <-doneChan:
			return err
		}
	}

	return <-doneChan
}

func (e *defaultExecutor) cleanup(f *future) {
	e.lock.Lock()
	defer e.lock.Unlock()
	delete(e.futures, f.job.Key())
	f.running.Set(false)
}

func newDefaultExecutor(
	registry executorRegistry,
	executedJobsCh chan<- cluster.JobExecuted,
	executionTimeout time.Duration,
	logger hclog.Logger,
) executor {
	if executionTimeout == 0 {
		executionTimeout = DefaultJobExecutionTimeout
	}
	return &defaultExecutor{
		registry:         registry,
		futures:          make(map[string]*future),
		executedJobsCh:   executedJobsCh,
		executionTimeout: executionTimeout,
		logger:           logger,
	}
}
