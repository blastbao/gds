package raft

import (
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/integration-system/gds/config"
	"github.com/pkg/errors"
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	defaultSyncTimeout = 3 * time.Second
	dbFile             = "raft_db"
	connectIOTimeout   = 2 * time.Second
)

type LeaderChangeNotification struct {
	CurrentLeaderAddress string
	IsLeader             bool
	LeaderElected        bool
}

type Raft struct {
	r              *raft.Raft
	cfg            config.ClusterConfiguration
	leaderObs      *raft.Observer
	leaderObsCh    chan raft.Observation
	leaderChangeCh chan LeaderChangeNotification
	closer         chan struct{}
}

func (r *Raft) LocalID() string {
	return r.cfg.OuterAddress
}

func (r *Raft) Leader() raft.ServerAddress {
	return r.r.Leader()
}

func (r *Raft) Servers() ([]raft.Server, error) {
	f := r.r.GetConfiguration()
	if err := f.Error(); err != nil {
		return nil, err
	}
	configuration := f.Configuration()
	return configuration.Servers, nil
}

func (r *Raft) BootstrapCluster() error {
	peers := makeRaftConfig(r.cfg.Peers)
	if f := r.r.BootstrapCluster(peers); f.Error() != nil {
		return errors.WithMessage(f.Error(), "bootstrap cluster")
	}
	return nil
}

func (r *Raft) SyncApply(cmd []byte) (interface{}, error) {
	f := r.r.Apply(cmd, defaultSyncTimeout)
	if err := f.Error(); err != nil {
		return nil, err
	}
	// f.Response 返回 fsm 对应 cmd 的 result
	return f.Response(), nil
}

func (r *Raft) LeaderChangeCh() <-chan LeaderChangeNotification {
	return r.leaderChangeCh
}

func (r *Raft) Shutdown() error {
	r.r.DeregisterObserver(r.leaderObs)
	close(r.closer)
	close(r.leaderObsCh)
	return r.r.Shutdown().Error()
}

func (r *Raft) listenLeader() {
	defer close(r.leaderChangeCh)
	for {
		select {
		case _, ok := <-r.leaderObsCh:
			if !ok {
				return
			}
			// 当前 leader
			currentLeader := r.r.Leader()
			// 发布 leader 变更事件
			select {
			case r.leaderChangeCh <- LeaderChangeNotification{
				IsLeader:             r.r.State() == raft.Leader,
				CurrentLeaderAddress: string(currentLeader),
				LeaderElected:        currentLeader != "",
			}:
			case <-r.closer:
				return
			default:
				continue
			}
		case <-r.closer:
			return
		}
	}
}

func NewRaft(tcpListener net.Listener, configuration config.ClusterConfiguration, state raft.FSM, logger hclog.Logger) (*Raft, error) {
	logStore, store, snapshotStore, err := makeRaftStores(configuration)
	if err != nil {
		return nil, err
	}

	netLogger := logger.Named("RAFT-NET")
	streamLayer := &StreamLayer{Listener: tcpListener}
	config := &raft.NetworkTransportConfig{
		Stream:                streamLayer,
		MaxPool:               len(configuration.Peers),
		Timeout:               connectIOTimeout,
		Logger:                netLogger,
		ServerAddressProvider: transparentAddressProvider{},
	}
	trans := raft.NewNetworkTransportWithConfig(config)

	cfg := raft.DefaultConfig()
	cfg.Logger = logger.Named("RAFT")
	cfg.LocalID = raft.ServerID(configuration.OuterAddress)
	r, err := raft.NewRaft(cfg, state, logStore, store, snapshotStore, trans)
	if err != nil {
		return nil, errors.WithMessage(err, "create raft")
	}

	leaderObs, leaderObsCh := makeLeaderObserver()
	r.RegisterObserver(leaderObs)
	raft := &Raft{
		r:              r,
		cfg:            configuration,
		leaderObs:      leaderObs,
		leaderObsCh:    leaderObsCh,
		leaderChangeCh: make(chan LeaderChangeNotification, 10),
		closer:         make(chan struct{}),
	}
	go raft.listenLeader()

	return raft, nil
}

func makeLeaderObserver() (*raft.Observer, chan raft.Observation) {
	ch := make(chan raft.Observation, 5)
	obs := raft.NewObserver(ch, false, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	return obs, ch
}

func makeRaftConfig(peers []string) raft.Configuration {
	servers := make([]raft.Server, len(peers))
	for i, peer := range peers {
		servers[i] = raft.Server{
			ID:      raft.ServerID(peer),
			Address: raft.ServerAddress(peer),
		}
	}
	return raft.Configuration{
		Servers: servers,
	}
}

func makeRaftStores(cfg config.ClusterConfiguration) (raft.LogStore, raft.StableStore, raft.SnapshotStore, error) {
	if cfg.InMemory {
		dbStore := raft.NewInmemStore()
		snapStore := raft.NewInmemSnapshotStore()
		return dbStore, dbStore, snapStore, nil
	}
	dbStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, dbFile))
	if err != nil {
		return nil, nil, nil, errors.WithMessage(err, "create db")
	}
	snapStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stdout)
	if err != nil {
		return nil, nil, nil, errors.WithMessage(err, "create snapshot store")
	}
	return dbStore, dbStore, snapStore, nil
}
