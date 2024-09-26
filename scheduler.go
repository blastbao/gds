package gds

import (
	"context"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/integration-system/gds/cluster"
	"github.com/integration-system/gds/config"
	"github.com/integration-system/gds/jobs"
	"github.com/integration-system/gds/provider"
	"github.com/integration-system/gds/store"
	"github.com/integration-system/gds/utils"
)

type Scheduler interface {
	WaitCluster(context.Context) bool
	Shutdown(ctx context.Context) error
	RegisterExecutor(jobType string, executor JobExecutor, newJobFunc provider.NewJobFunc) Scheduler
	UnregisterExecutor(jobType string)

	AddJob(job jobs.Job) error
	GetJob(key string) (*store.JobInfo, error)
	DelJob(key string) error
	GetAllJobs() []store.JobInfo
	GetJobsByType(jobType string) []store.JobInfo
}

type scheduler struct {
	store        *store.Store
	registry     executorRegistry
	executor     executor
	typeProvider provider.TypeProvider
	raftAdapter  *RaftAdapter
}

func (s *scheduler) AddJob(job jobs.Job) error {
	// 已经存在？
	var err error
	s.store.VisitReadonlyState(func(state store.ReadonlyState) {
		_, err = state.GetJob(job.Key())
	})
	if err == nil {
		return store.ErrJobAlreadyExists
	}

	// 序列化
	b, err := job.Marshal()
	if err != nil {
		return err
	}

	// 构造 insert job cmd
	cmd := cluster.PrepareInsertJobCommand(job.Type(), b)

	// 提交到 raft
	_, err = s.raftAdapter.ClusterClient.SyncApplyHelper(cmd, "InsertJobCommand")
	return err
}

func (s *scheduler) GetJob(key string) (*store.JobInfo, error) {
	var (
		job *store.JobInfo
		err error
	)
	s.store.VisitReadonlyState(func(state store.ReadonlyState) {
		job, err = state.GetJob(key)
	})
	return job, err
}

func (s *scheduler) DelJob(key string) error {
	var err error
	s.store.VisitReadonlyState(func(state store.ReadonlyState) {
		_, err = state.GetJob(key)
	})
	if err != nil {
		return err
	}

	cmd := cluster.PrepareDeleteJobCommand(key)
	_, err = s.raftAdapter.ClusterClient.SyncApplyHelper(cmd, "DeleteJobCommand")
	return err
}

func (s *scheduler) GetAllJobs() []store.JobInfo {
	var result []store.JobInfo
	s.store.VisitReadonlyState(func(state store.ReadonlyState) {
		result = state.GetAllJobs()
	})
	return result
}

func (s *scheduler) GetJobsByType(jobType string) []store.JobInfo {
	var result []store.JobInfo
	s.store.VisitReadonlyState(func(state store.ReadonlyState) {
		result = state.GetJobsByType(jobType)
	})
	return result
}

func (s *scheduler) RegisterExecutor(jType string, executor JobExecutor, newJobFunc provider.NewJobFunc) Scheduler {
	s.registry.Register(jType, executor)
	s.typeProvider.RegisterJobProvider(jType, newJobFunc)
	return s
}

func (s *scheduler) UnregisterExecutor(jType string) {
	s.registry.Unregister(jType)
	s.typeProvider.UnregisterJobProvider(jType)
}

func (s *scheduler) WaitCluster(ctx context.Context) bool {
	return utils.Wait(ctx, func() bool {
		return s.raftAdapter.ClusterClient.Leader() != ""
	}, 60*time.Millisecond)
}

func (s *scheduler) Shutdown(ctx context.Context) error {
	errs := new(multierror.Error)

	if err := s.raftAdapter.Shutdown(ctx); err != nil {
		err = fmt.Errorf("shutdown raft: %v", err)
		errs = multierror.Append(errs, err)
	}
	if err := s.executor.Shutdown(ctx); err != nil {
		errs = multierror.Append(errs, err)
	}
	return errs.ErrorOrNil()
}

func NewScheduler(config config.ClusterConfiguration) (Scheduler, error) {
	var logger hclog.Logger
	if config.Logger != nil {
		logger = config.Logger
	} else {
		logger = hclog.Default().Named("gds")
	}

	// Job 执行完毕后，结果发送到此 ch
	executedJobsCh := make(chan cluster.JobExecuted, 100)
	executorRegistry := newDefaultExecutorRegistry()
	executor := newDefaultExecutor(executorRegistry, executedJobsCh, config.JobExecutionTimeout, logger)

	typeProvider := provider.NewTypeProvider()
	clusterHandler := NewClusterHandler(typeProvider, executor, logger)

	//
	raftAdapter, err := NewRaftAdapter(config, clusterHandler, typeProvider, logger)
	if err != nil {
		return nil, err
	}

	s := &scheduler{
		registry:     executorRegistry,
		executor:     executor,
		typeProvider: typeProvider,
		raftAdapter:  raftAdapter,
		store:        raftAdapter.RaftStore,
	}

	clusterHandler.cluster = raftAdapter.ClusterClient
	go clusterHandler.listenLeaderCh(raftAdapter.RaftStore)
	go clusterHandler.handleExecutedJobs(executedJobsCh) // 将已完成任务状态保存到 raft lsm 上

	return s, nil
}
