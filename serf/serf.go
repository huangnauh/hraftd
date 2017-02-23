package serf

import (
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/hashicorp/serf/serf"
	"github.com/huangnauh/hraftd/store"
)

const (
	statusReap = serf.MemberStatus(-1)
)

// Serf manages membership of Jocko cluster using Hashicorp Serf
type Serf struct {
	serf        *serf.Serf
	addr        string
	reconcileCh chan<- *store.ClusterMember
	eventCh     chan serf.Event
	initMembers []string
	shutdownCh  chan struct{}

	peers    map[int64]*store.ClusterMember
	peerLock sync.RWMutex
}

// New Serf object
func New(serfMembers []string, serfAddr string) (*Serf, error) {
	b := &Serf{
		peers:       make(map[int64]*store.ClusterMember),
		eventCh:     make(chan serf.Event, 256),
		shutdownCh:  make(chan struct{}),
		initMembers: serfMembers,
		addr:		 serfAddr,
	}

	return b, nil
}

// Bootstrap saves the node metadata and starts the serf agent
// Info of node updates is returned on reconcileCh channel
func (b *Serf) Bootstrap(node *store.ClusterMember, reconcileCh chan<- *store.ClusterMember) error {
	addr, strPort, err := net.SplitHostPort(b.addr)
	if err != nil {
		return err
	}

	port, err := strconv.Atoi(strPort)
	if err != nil {
		return err
	}
	conf := serf.DefaultConfig()
	conf.Init()
	conf.MemberlistConfig.BindAddr = addr
	conf.MemberlistConfig.BindPort = port
	conf.EventCh = b.eventCh
	conf.EnableNameConflictResolution = false
	conf.NodeName = fmt.Sprintf("hraftd-%03d", node.ID)
	conf.Tags["id"] = strconv.Itoa(int(node.ID))
	conf.Tags["raft_port"] = strconv.Itoa(node.RaftPort)
	s, err := serf.Create(conf)
	if err != nil {
		return err
	}
	b.serf = s
	b.reconcileCh = reconcileCh
	if _, err := b.Join(b.initMembers...); err != nil {
		// b.Shutdown()
		return err
	}

	// ingest events for serf
	go b.serfEventHandler()

	return nil
}

// serfEventHandler is used to handle events from the serf cluster
func (b *Serf) serfEventHandler() {
	for {
		select {
		case e := <-b.eventCh:
			switch e.EventType() {
			case serf.EventMemberJoin:
				b.nodeJoinEvent(e.(serf.MemberEvent))
				b.localMemberEvent(e.(serf.MemberEvent))
			case serf.EventMemberLeave, serf.EventMemberFailed:
				b.nodeFailedEvent(e.(serf.MemberEvent))
				b.localMemberEvent(e.(serf.MemberEvent))
			case serf.EventMemberUpdate, serf.EventMemberReap, serf.EventUser, serf.EventQuery:
				// ignore
			default:
			}
		case <-b.shutdownCh:
			return
		}
	}
}

// nodeJoinEvent is used to handle join events on the serf cluster
func (b *Serf) nodeJoinEvent(me serf.MemberEvent) {
	for _, m := range me.Members {
		// TODO: need to change these parts
		peer, err := clusterMember(m)
		if err != nil {
			continue
		}
		b.peerLock.Lock()
		b.peers[peer.ID] = peer
		b.peerLock.Unlock()
	}
}

// nodeFailedEvent is used to handle fail events on the serf cluster.
func (b *Serf) nodeFailedEvent(me serf.MemberEvent) {
	for _, m := range me.Members {
		peer, err := clusterMember(m)
		if err != nil {
			continue
		}
		b.peerLock.Lock()
		delete(b.peers, peer.ID)
		b.peerLock.Unlock()
	}
}

// localMemberEvent is used to reconcile Serf events with the store if we are the leader.
func (b *Serf) localMemberEvent(me serf.MemberEvent) error {
	isReap := me.EventType() == serf.EventMemberReap
	for _, m := range me.Members {
		if isReap {
			m.Status = statusReap
		}
		conn, err := clusterMember(m)
		if err != nil {
			continue
		}
		b.reconcileCh <- conn
	}
	return nil
}

// Addr of serf agent
func (b *Serf) Addr() string {
	return b.addr
}

// Join an existing serf cluster
func (b *Serf) Join(addrs ...string) (int, error) {
	if len(addrs) == 0 {
		return 0, nil
	}
	return b.serf.Join(addrs, true)
}

// Cluster is the list of all nodes connected to Serf
func (b *Serf) Cluster() []*store.ClusterMember {
	b.peerLock.RLock()
	defer b.peerLock.RUnlock()

	cluster := make([]*store.ClusterMember, 0, len(b.peers))
	for _, v := range b.peers {
		cluster = append(cluster, v)
	}
	return cluster
}

// Member returns broker details of node with given ID
func (b *Serf) Member(memberID int64) *store.ClusterMember {
	b.peerLock.RLock()
	defer b.peerLock.RUnlock()
	return b.peers[memberID]
}

// Leave the serf cluster
func (b *Serf) Leave() error {
	if err := b.serf.Leave(); err != nil {
		return err
	}
	return nil
}

// Shutdown Serf agent
func (b *Serf) Shutdown() error {
	close(b.shutdownCh)
	if err := b.serf.Shutdown(); err != nil {
		return err
	}
	return nil
}

func clusterMember(m serf.Member) (*store.ClusterMember, error) {
	idStr := m.Tags["id"]
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return nil, err
	}

	raftPortStr := m.Tags["raft_port"]
	raftPort, err := strconv.Atoi(raftPortStr)
	if err != nil {
		return nil, err
	}

	conn := &store.ClusterMember{
		IP:       m.Addr.String(),
		ID:       int64(id),
		RaftPort: raftPort,
		Status:   status(m.Status),
	}

	return conn, nil
}

func status(s serf.MemberStatus) store.MemberStatus {
	switch s {
	case serf.StatusAlive:
		return store.StatusAlive
	case serf.StatusFailed:
		return store.StatusFailed
	case serf.StatusLeaving:
		return store.StatusLeaving
	case serf.StatusLeft:
		return store.StatusLeft
	case statusReap:
		return store.StatusReap
	default:
		return store.StatusNone
	}
}
