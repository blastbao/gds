package provider

import (
	"github.com/integration-system/gds/jobs"
)

type NewJobFunc func() jobs.Job

type TypeProvider interface {
	RegisterJobProvider(name string, newJob NewJobFunc)
	UnregisterJobProvider(name string)
	Get(name string) NewJobFunc
}

type typeProvider struct {
	factory map[string]NewJobFunc
}

func (tp *typeProvider) RegisterJobProvider(typ string, newJob NewJobFunc) {
	tp.factory[typ] = newJob
}

func (tp *typeProvider) UnregisterJobProvider(typ string) {
	delete(tp.factory, typ)
}

func (tp *typeProvider) Get(typ string) NewJobFunc {
	return tp.factory[typ]
}

func NewTypeProvider() TypeProvider {
	return &typeProvider{
		factory: make(map[string]NewJobFunc),
	}
}
