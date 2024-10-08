package store

import (
	"errors"
	"github.com/integration-system/gds/jobs"
	"time"
)

var (
	ErrJobAlreadyExists = errors.New("job with same key already exists")
	ErrJobNotFound      = errors.New("job not found")
)

var _ WritableState = (*state)(nil)

type ReadonlyState interface {
	GetJob(key string) (*JobInfo, error)
	GetJobsByType(jobType string) []JobInfo
	GetAllJobs() []JobInfo
	GetPeerJobsKeys(peerID string) []string
	GetUnassignedJobsKeys() []string
	GetPeers() []string
}

type WritableState interface {
	ReadonlyState
	InsertJob(job jobs.Job) error
	AssignJob(key string, peerID string)
	DeleteJob(key string)
	UnassignPeer(peerID string)
	ApplyPostExecution(jobKey, err string, executedTime time.Time)
	AddPeer(peerID string)
	RemovePeer(peerID string)
}

type JobInfo struct {
	jobInfo            // 任务信息
	State   jobs.State // 任务状态
}

type jobInfo struct {
	Job            jobs.Job // 任务基本信息：名称、类型、触发时间、数据
	AssignedPeerID string   // 派发节点
}

type state struct {
	Jobs        map[string]jobInfo
	OnlinePeers []string
}

func (s *state) InsertJob(job jobs.Job) error {
	if _, exists := s.Jobs[job.Key()]; exists {
		return ErrJobAlreadyExists
	}
	s.Jobs[job.Key()] = jobInfo{
		Job: job,
	}
	return nil
}

func (s *state) GetJob(key string) (*JobInfo, error) {
	j, ok := s.Jobs[key]
	if !ok {
		return nil, ErrJobNotFound
	}
	info := &JobInfo{
		jobInfo: j,
		State:   calcJobState(j),
	}
	return info, nil
}

func (s *state) DeleteJob(key string) {
	delete(s.Jobs, key)
}

func (s *state) GetJobsByType(jobType string) []JobInfo {
	result := make([]JobInfo, 0)
	for _, info := range s.Jobs {
		if jobType == info.Job.Type() {
			jobInfo := JobInfo{
				jobInfo: info,
				State:   calcJobState(info),
			}
			result = append(result, jobInfo)
		}
	}
	return result
}

func (s *state) GetAllJobs() []JobInfo {
	result := make([]JobInfo, 0, len(s.Jobs))
	for _, info := range s.Jobs {
		jobInfo := JobInfo{
			jobInfo: info,
			State:   calcJobState(info),
		}
		result = append(result, jobInfo)
	}
	return result
}

func (s *state) UnassignPeer(peerID string) {
	for key, info := range s.Jobs {
		if info.AssignedPeerID == peerID {
			info.AssignedPeerID = ""
			s.Jobs[key] = info
		}
	}
}

func (s *state) GetPeerJobsKeys(peerID string) []string {
	var keys []string
	for key, info := range s.Jobs {
		if info.AssignedPeerID == peerID {
			keys = append(keys, key)
		}
	}
	return keys
}

func (s *state) GetUnassignedJobsKeys() []string {
	var keys []string
	for key, info := range s.Jobs {
		if info.AssignedPeerID == "" {
			keys = append(keys, key)
		}
	}
	return keys
}

func (s *state) AssignJob(key string, peerID string) {
	info, exists := s.Jobs[key]
	if exists {
		info.AssignedPeerID = peerID
		s.Jobs[key] = info
	}
}

func (s *state) ApplyPostExecution(jobKey, errStr string, executedTime time.Time) {
	info := s.Jobs[jobKey]

	var err error
	if errStr != "" {
		err = errors.New(errStr)
	}
	info.Job.PostExecution(executedTime, err)

	s.Jobs[jobKey] = info
}

func (s *state) GetPeers() []string {
	cpy := make([]string, len(s.OnlinePeers))
	copy(cpy, s.OnlinePeers)
	return cpy
}

func (s *state) AddPeer(peerID string) {
	for _, onlinePeer := range s.OnlinePeers {
		if onlinePeer == peerID {
			return
		}
	}
	s.OnlinePeers = append(s.OnlinePeers, peerID)
}

func (s *state) RemovePeer(peerID string) {
	for i, onlinePeer := range s.OnlinePeers {
		if onlinePeer == peerID {
			s.OnlinePeers = append(s.OnlinePeers[:i], s.OnlinePeers[i+1:]...)
			return
		}
	}
}

func newEmptyState() *state {
	return &state{
		Jobs:        make(map[string]jobInfo),
		OnlinePeers: make([]string, 0),
	}
}

func calcJobState(info jobInfo) jobs.State {
	if info.Job.NextTriggerTime().IsZero() {
		return jobs.StateExhausted
	}
	if info.AssignedPeerID == "" {
		return jobs.StateUnacquired
	}
	return jobs.StateScheduled
}
