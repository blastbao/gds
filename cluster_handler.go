package gds

import (
	"fmt"
	"github.com/hashicorp/go-hclog"
	"sync"
	"time"

	"github.com/integration-system/gds/cluster"
	"github.com/integration-system/gds/jobs"
	"github.com/integration-system/gds/provider"
	"github.com/integration-system/gds/store"
	"github.com/integration-system/gds/utils"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

const (
	assigningInterval = 100 * time.Millisecond
	batchSize         = 1000
)

var (
	json = jsoniter.ConfigFastest
)

type ClusterHandler struct {
	cluster      *cluster.Client
	executor     executor
	typeProvider provider.TypeProvider

	nextPeer         *utils.RoundRobinStrings
	assignJobsChLock sync.RWMutex
	assignJobsCh     chan []string

	logger hclog.Logger
}

func (cl *ClusterHandler) HandleAddPeerCommand(state store.WritableState, data []byte) (interface{}, error) {
	payload := cluster.AddPeer{}
	err := json.Unmarshal(data, &payload)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal cluster.AddPeer")
	}

	state.AddPeer(payload.PeerID)
	cl.nextPeer.Update(state.GetPeers())

	return nil, nil
}

func (cl *ClusterHandler) HandleRemovePeerCommand(state store.WritableState, data []byte) (interface{}, error) {
	payload := cluster.RemovePeer{}
	err := json.Unmarshal(data, &payload)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal cluster.RemovePeer")
	}
	state.RemovePeer(payload.PeerID)
	cl.nextPeer.Update(state.GetPeers())

	if cl.cluster.IsLeader() {
		jobs := state.GetPeerJobsKeys(payload.PeerID)
		cl.assignJobs(jobs)
	}
	state.UnassignPeer(payload.PeerID)

	return nil, nil
}

// HandleInsertJobCommand
//
// [重要]
// raft 集群中每个节点在收到 "InsertJobCommand" 消息时，都会执行此函数；
// 但仅当前节点为 leader 时，才会执行 job assign 逻辑，其会提交 "AssignJobCommand" 到状态机中，
// 每个节点在收到 "AssignJobCommand" 消息时，检查是否有自己的任务，有则进行处理。
func (cl *ClusterHandler) HandleInsertJobCommand(state store.WritableState, data []byte) (interface{}, error) {
	// 参数解析
	payload := cluster.InsertJob{}
	err := json.Unmarshal(data, &payload)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal cluster.InsertJob")
	}

	// 根据 typ 创建对应 job (工厂模式)，并反序列化
	f := cl.typeProvider.Get(payload.Type)
	job := f()
	err = job.Unmarshal(payload.Job)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal job data")
	}

	// 把 job 保存到 raft fsm (保存到内存 map 中)
	err = state.InsertJob(job)
	if err != nil {
		return nil, err
	}

	// [重要] 收到 insert job msg 后，leader 执行任务分发。
	if cl.cluster.IsLeader() {
		cl.assignJobs([]string{job.Key()})
	}
	return nil, nil
}

func (cl *ClusterHandler) HandleDeleteJobCommand(state store.WritableState, data []byte) (interface{}, error) {
	payload := cluster.DeleteJob{}
	err := json.Unmarshal(data, &payload)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal cluster.DelJob")
	}

	// 取 job
	jobInfo, err := state.GetJob(payload.Key)
	if err != nil {
		return nil, err
	}

	// 如果该 job 归属于本机，则 cancel 掉
	if cl.cluster.LocalID() == jobInfo.AssignedPeerID {
		cl.executor.CancelJob(payload.Key)
	}

	// 从 raft fsm 中删除 job
	state.DeleteJob(payload.Key)
	return nil, nil
}

func (cl *ClusterHandler) HandleAssignJobCommand(state store.WritableState, data []byte) (interface{}, error) {
	// 解析消息，包含 jobs 和其分配的 peerid
	payload := cluster.AssignJob{}
	err := json.Unmarshal(data, &payload)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal cluster.AssignJob")
	}
	// 获取新提交的 jobs
	for _, key := range payload.JobKeys {
		// 获取 job info
		jobInfo, err := state.GetJob(key)
		if err != nil {
			continue
		}
		// if duplicate assignment
		// 如果 job 已经分配给某个 peer ，skip
		if jobInfo.AssignedPeerID == payload.PeerID {
			continue
		}
		if jobInfo.State == jobs.StateExhausted {
			continue
		}
		// 更新状态机
		state.AssignJob(key, payload.PeerID)
		// [重要] 如果 job 分配给自己，就保存下来
		if cl.cluster.LocalID() == payload.PeerID {
			cl.executor.AddJob(jobInfo.Job)
		}
	}

	return nil, nil
}

func (cl *ClusterHandler) HandleJobExecutedCommand(state store.WritableState, data []byte) (interface{}, error) {
	payload := cluster.JobExecuted{}
	err := json.Unmarshal(data, &payload)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal cluster.JobExecuted")
	}

	state.ApplyPostExecution(payload.JobKey, payload.Error, payload.ExecutedTime)

	jobInfo, err := state.GetJob(payload.JobKey)
	if err != nil {
		return nil, err
	}
	if cl.cluster.LocalID() == jobInfo.AssignedPeerID {
		if jobInfo.State != jobs.StateExhausted {
			cl.executor.AddJob(jobInfo.Job)
		}
	}

	return nil, nil
}

func (cl *ClusterHandler) GetHandlers() map[uint64]func(store.WritableState, []byte) (interface{}, error) {
	return map[uint64]func(store.WritableState, []byte) (interface{}, error){
		// peer manage
		cluster.AddPeerCommand:    cl.HandleAddPeerCommand,
		cluster.RemovePeerCommand: cl.HandleRemovePeerCommand,
		// job manage
		cluster.InsertJobCommand:   cl.HandleInsertJobCommand,
		cluster.DeleteJobCommand:   cl.HandleDeleteJobCommand,
		cluster.AssignJobCommand:   cl.HandleAssignJobCommand,
		cluster.JobExecutedCommand: cl.HandleJobExecutedCommand,
	}
}

func (cl *ClusterHandler) listenLeaderCh(mainStore *store.Store) {
	closeCh := make(chan struct{})
	for isLeader := range cl.cluster.LeaderCh() {
		// 非 leader ，直接 continue 等待下一次变更
		if !isLeader {
			close(closeCh)
			continue
		}
		// 成为 leader ，启动 leader 协程
		// 1.
		// 2.
		// 3. 监听新任务提交，绑定到指定 peer 上；
		closeCh = make(chan struct{})
		assignJobsCh := make(chan []string, batchSize)
		// TODO get rid of mutex
		cl.assignJobsChLock.Lock()
		cl.assignJobsCh = assignJobsCh
		cl.assignJobsChLock.Unlock()
		go cl.checkPeers(closeCh, mainStore.VisitReadonlyState)
		go cl.checkJobs(closeCh, mainStore.VisitReadonlyState)
		go cl.assigningJobs(closeCh, assignJobsCh)
	}
}

// Used to detect changes in onlinePeers while there was no leader in cluster (e.g. another peer got down).
func (cl *ClusterHandler) checkPeers(closeCh chan struct{}, visitState func(f func(store.ReadonlyState))) {
	defer func() {
		if err := recover(); err != nil {
			cl.logger.Error(fmt.Sprintf("panic on check peers: %v", err))
		}
	}()
	time.Sleep(200 * time.Millisecond)

	var oldServers []string
	visitState(func(state store.ReadonlyState) {
		oldServers = state.GetPeers()
	})

	var newServers []string
	newServers, err := cl.cluster.Servers()
	if err != nil {
		// error only occurs during leadership transferring, so there will be new leader soon
		return
	}

	_, deleted := utils.CompareSlices(oldServers, newServers)
	for _, peerID := range deleted {
		select {
		case <-closeCh:
			return
		default:
		}
		cmd := cluster.PrepareRemovePeerCommand(peerID)
		_, _ = cl.cluster.SyncApplyHelper(cmd, "RemovePeerCommand")
	}
}

// Checks all jobs to have assignedPeer.
func (cl *ClusterHandler) checkJobs(closeCh chan struct{}, visitState func(f func(store.ReadonlyState))) {
	time.Sleep(300 * time.Millisecond)

	var unassignedJobsKeys []string
	visitState(func(state store.ReadonlyState) {
		unassignedJobsKeys = state.GetUnassignedJobsKeys()
	})

	select {
	case <-closeCh:
		return
	default:
	}

	cl.assignJobs(unassignedJobsKeys)
}

func (cl *ClusterHandler) assigningJobs(closeCh chan struct{}, assignJobsCh chan []string) {
	defer func() {
		if err := recover(); err != nil {
			cl.logger.Error(fmt.Sprintf("panic on assigning jobs: %v", err))
		}
	}()
	ticker := time.NewTicker(assigningInterval)
	defer ticker.Stop()

	assignJobsFn := func(keys []string) {
		// round robin 取下一个 peer
		peerID := cl.nextPeer.Get()
		// 构造 `AssignJob` cmd ，用于把 jobs 绑定到 peer 上
		cmd := cluster.PrepareAssignJobCommand(keys, peerID)
		// 提交到 raft 状态机
		_, _ = cl.cluster.SyncApplyHelper(cmd, "AssignJobCommand")
	}

	jobs := make([]string, 0, batchSize)

	// 这里是聚合 assign cmd ，用于批量分配任务，避免提交 assign cmd 到 raft 过于频繁，加重负载
	for {
		select {
		case assignJobs := <-assignJobsCh:
			jobs = append(jobs, assignJobs...)
			if len(jobs) >= 2*batchSize {
				// TODO send batches async in select?
				var batches [][]string
				for batchSize < len(jobs) {
					jobs, batches = jobs[batchSize:], append(batches, jobs[0:batchSize:batchSize])
				}
				batches = append(batches, jobs)
				for _, batch := range batches {
					assignJobsFn(batch)
				}
				// create new slice to allow gc previous big one
				jobs = make([]string, 0, batchSize)
			} else if len(jobs) >= batchSize {
				assignJobsFn(utils.MakeUnique(jobs))
				jobs = jobs[:0]
			}
		case <-ticker.C:
			if len(jobs) == 0 {
				continue
			}
			assignJobsFn(utils.MakeUnique(jobs))
			jobs = jobs[:0]
		case <-closeCh:
			return
		}
	}
}

func (cl *ClusterHandler) assignJobs(keys []string) {
	if !cl.cluster.IsLeader() {
		return
	}
	if keys == nil {
		return
	}
	cl.assignJobsChLock.RLock()
	cl.assignJobsCh <- keys
	cl.assignJobsChLock.RUnlock()
}

func (cl *ClusterHandler) handleExecutedJobs(executedJobsCh <-chan cluster.JobExecuted) {
	defer func() {
		if err := recover(); err != nil {
			cl.logger.Error(fmt.Sprintf("panic on executed jobs: %v", err))
		}
	}()

	// Job 执行完毕后，结果发送到此 ch ，需要 apply 到 lsm 上记录下来。
	for payload := range executedJobsCh {
		cmd := cluster.PrepareJobExecutedCommand(payload.JobKey, payload.Error, payload.ExecutedTime)
		// TODO handle errors. retry?
		_, _ = cl.cluster.SyncApplyHelper(cmd, "JobExecutedCommand")
	}
}

func NewClusterHandler(typeProvider provider.TypeProvider, executor executor, logger hclog.Logger) *ClusterHandler {
	return &ClusterHandler{
		typeProvider: typeProvider,
		executor:     executor,
		nextPeer:     utils.NewRoundRobinStrings(make([]string, 0)),
		logger:       logger,
	}
}
