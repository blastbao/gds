package gds

import (
	"github.com/integration-system/gds/jobs"
	"sync"
)

type JobExecutor func(jobs.Job) error

type executorRegistry interface {
	Register(typ string, executor JobExecutor)
	Unregister(typ string)
	GetExecutor(typ string) (JobExecutor, bool)
}

type defaultExecutorRegistry struct {
	lock     sync.RWMutex
	registry map[string]JobExecutor
}

func (r *defaultExecutorRegistry) Register(typ string, executor JobExecutor) {
	r.lock.Lock()
	r.registry[typ] = executor
	r.lock.Unlock()
}

func (r *defaultExecutorRegistry) Unregister(typ string) {
	r.lock.Lock()
	delete(r.registry, typ)
	r.lock.Unlock()
}

func (r *defaultExecutorRegistry) GetExecutor(typ string) (JobExecutor, bool) {
	r.lock.RLock()
	e, ok := r.registry[typ]
	r.lock.RUnlock()
	return e, ok
}

func newDefaultExecutorRegistry() executorRegistry {
	return &defaultExecutorRegistry{
		registry: make(map[string]JobExecutor),
	}
}
