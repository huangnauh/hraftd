// Package store provides a simple distributed key-value store. The keys and
// associated values are changed via distributed consensus, meaning that the
// values are changed only when a majority of nodes in the cluster agree on
// the new value.
//
// Distributed consensus is provided via the Raft algorithm.
package store

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
	"strconv"

	"github.com/huangnauh/hraftd/serf"
	"github.com/huangnauh/hraftd/member"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}



// Store is a simple key-value store, where all changes are made via Raft consensus.
type Store struct {
	id       int64
	RaftDir  string
	RaftBind string

	mu sync.Mutex
	m  map[string]string // The key-value store for the system.

	raft *raft.Raft // The consensus mechanism
	serf *serf.Serf
	peers    map[int64]*member.ClusterMember

	reconcileCh chan *member.ClusterMember
	reconcileInterval time.Duration

	shutdownCh   chan struct{}

	logger *log.Logger
}

// New returns a new Store.
func New(id int64) *Store {
	return &Store{
		id:		id,
		m:      make(map[string]string),
		logger: log.New(os.Stderr, "[hraftd-store] ", log.LstdFlags),
	}
}


func getPortFromAddr(addr string) (int, error) {
	_, strPort, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}

	port, err := strconv.Atoi(strPort)
	if err != nil {
		return 0, err
	}
	return port, nil
}


// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
func (s *Store) Open(enableSingle bool, serfMembers []string, serfAddr string) error {

	raftPort, err := getPortFromAddr(s.RaftBind)
	if err != nil {
		return err
	}

	conn := &member.ClusterMember{
		ID:       s.id,
		RaftPort: raftPort,
	}

	s.serf, err = serf.New(serfMembers, serfAddr)

	if err := s.serf.Bootstrap(conn, s.reconcileCh); err != nil {
		return err
	}

	// Setup Raft configuration.
	config := raft.DefaultConfig()

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.RaftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create peer storage.
	peerStore := raft.NewJSONPeers(s.RaftDir, transport)

	// Check for any existing peers.
	peers, err := peerStore.Peers()
	if err != nil {
		return err
	}

	// Allow the node to entry single-mode, potentially electing itself, if
	// explicitly enabled and there is only 1 node in the cluster already.
	if enableSingle && len(peers) <= 1 {
		s.logger.Println("enabling single-node mode")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, logStore, snapshots, peerStore, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	go s.monitorLeadership()
	return nil
}


// monitorLeadership is used to monitor if we acquire or lose our role
// as the leader in the Raft cluster. There is some work the leader is
// expected to do, so we must react to changes
func (s *Store) monitorLeadership() {
	leaderCh := s.raft.LeaderCh()
	var stopCh chan struct{}
	for {
		select {
		case isLeader := <-leaderCh:
			if isLeader {
				stopCh = make(chan struct{})
				go s.leaderLoop(stopCh)
				s.logger.Printf("[INFO] hraftd: cluster leadership acquired")
			} else if stopCh != nil {
				close(stopCh)
				stopCh = nil
				s.logger.Printf("[INFO] hraftd: cluster leadership lost")
			}
		case <-s.shutdownCh:
			return
		}
	}
}


// leaderLoop runs as long as we are the leader to run various
// maintenance activities
func (s *Store) leaderLoop(stopCh chan struct{}) {
	// Ensure we revoke leadership on stepdown
	defer s.revokeLeadership()

	// Reconcile channel is only used once initial reconcile
	// has succeeded
	var reconcileCh chan *member.ClusterMember
	establishedLeader := false

RECONCILE:
	// Setup a reconciliation timer
	reconcileCh = nil
	interval := time.After(s.reconcileInterval)

	// Apply a raft barrier to ensure our FSM is caught up
	barrier := s.raft.Barrier(0)
	if err := barrier.Error(); err != nil {
		s.logger.Printf("[ERR] hraftd: failed to wait for barrier: %v", err)
		goto WAIT
	}

	// Check if we need to handle initial leadership actions
	if !establishedLeader {
		if err := s.establishLeadership(); err != nil {
			s.logger.Printf("[ERR] hraftd: failed to establish leadership: %v",
				err)
			goto WAIT
		}
		establishedLeader = true
	}

	// Reconcile any missing data
	if err := s.reconcile(); err != nil {
		s.logger.Printf("[ERR] hraftd: failed to reconcile: %v", err)
		goto WAIT
	}

	// Initial reconcile worked, now we can process the channel
	// updates
	reconcileCh = s.reconcileCh

WAIT:
	// Periodically reconcile as long as we are the leader,
	// or when Serf events arrive
	for {
		select {
		case <-stopCh:
			return
		case <-s.shutdownCh:
			return
		case <-interval:
			goto RECONCILE
		case member := <-reconcileCh:
			s.reconcileMember(member)
		}
	}
}

func (s *Store) reconcile() error {
	members := s.serf.Cluster()
	for _, member := range members {
		if err := s.reconcileMember(member); err != nil {
			return err
		}
	}
	return nil
}


// revokeLeadership is invoked once we step down as leader.
// This is used to cleanup any state that may be specific to the leader.
func (s *Store) revokeLeadership() error {
	return nil
}

func (s *Store) establishLeadership() error {
	return nil
}


func (s *Store) reconcileMember(memb *member.ClusterMember) error {
	// don't reconcile ourself
	if memb.ID == s.id {
		return nil
	}
	s.logger.Println("member: %v", memb)
	var err error
	switch memb.Status {
	case member.StatusAlive:
		err = s.addRaftPeer(memb)
	case member.StatusLeft, member.StatusReap:
		err = s.removeRaftPeer(memb)
	}
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) addRaftPeer(memb *member.ClusterMember) error {
	s.logger.Printf("addRaftPeer: %v", memb)
	addr := &net.TCPAddr{IP: net.ParseIP(memb.IP), Port: memb.RaftPort}
	future := s.raft.AddPeer(addr.String())
	if err := future.Error(); err != nil && err != raft.ErrKnownPeer {
		s.logger.Printf("failed to add raft peer: %v", err)
		return err
	} else if err == nil {
		s.logger.Printf("added raft peer: %v", addr)
	}
	return nil
}

func (s *Store) removeRaftPeer(memb *member.ClusterMember) error {
	addr := &net.TCPAddr{IP: net.ParseIP(memb.IP), Port: memb.RaftPort}
	future := s.raft.RemovePeer(addr.String())
	if err := future.Error(); err != nil && err != raft.ErrUnknownPeer {
		s.logger.Printf("failed to remove raft peer: %v", err)
		return err
	} else if err == nil {
		s.logger.Printf("removed raft peer: %v", addr)
	}
	return nil
}


// Get returns the value for the given key.
func (s *Store) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[key], nil
}

// Set sets the value for the given key.
func (s *Store) Set(key, value string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:    "set",
		Key:   key,
		Value: value,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

// Delete deletes the given key.
func (s *Store) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:  "delete",
		Key: key,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

// Join joins a node, located at addr, to this store. The node must be ready to
// respond to Raft communications at that address.
func (s *Store) Join(addr string) error {
	s.logger.Printf("received join request for remote node as %s", addr)

	f := s.raft.AddPeer(addr)
	if f.Error() != nil {
		return f.Error()
	}
	s.logger.Printf("node at %s joined successfully", addr)
	return nil
}

type fsm Store

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case "set":
		return f.applySet(c.Key, c.Value)
	case "delete":
		return f.applyDelete(c.Key)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map.
	o := make(map[string]string)
	for k, v := range f.m {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	o := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.m = o
	return nil
}

func (f *fsm) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, key)
	return nil
}

type fsmSnapshot struct {
	store map[string]string
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *fsmSnapshot) Release() {}
