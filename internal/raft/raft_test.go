package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// memTransport соединяет узлы напрямую в памяти; узлы из down недоступны.
type memTransport struct {
	mu    sync.Mutex
	nodes map[string]*Node
	down  map[string]bool
}

func newMemTransport() *memTransport {
	return &memTransport{nodes: map[string]*Node{}, down: map[string]bool{}}
}

func (m *memTransport) target(addr string) (*Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down[addr] || m.nodes[addr] == nil {
		return nil, errors.New("unreachable")
	}
	return m.nodes[addr], nil
}

func (m *memTransport) RequestVote(_ context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error) {
	n, err := m.target(addr)
	if err != nil {
		return RequestVoteResponse{}, err
	}
	return n.HandleRequestVote(req), nil
}

func (m *memTransport) AppendEntries(_ context.Context, addr string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	n, err := m.target(addr)
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	return n.HandleAppendEntries(req), nil
}

func (m *memTransport) setDown(addr string, down bool) {
	m.mu.Lock()
	m.down[addr] = down
	m.mu.Unlock()
}

func newTestNode(t *testing.T, id, size int, tr Transport) *Node {
	t.Helper()
	cfg := DefaultConfig()
	cfg.ID = id
	cfg.Peers = map[int]string{}
	for i := 1; i <= size; i++ {
		cfg.Peers[i] = fmt.Sprintf("n%d", i)
	}
	cfg.Seed = 42
	cfg.Transport = tr
	cfg.Logger = slog.New(slog.DiscardHandler)
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func startCluster(t *testing.T, size int) (*memTransport, []*Node) {
	t.Helper()
	tr := newMemTransport()
	nodes := make([]*Node, 0, size)
	for i := 1; i <= size; i++ {
		n := newTestNode(t, i, size, tr)
		tr.nodes[n.cfg.Peers[i]] = n
		nodes = append(nodes, n)
	}
	for _, n := range nodes {
		n.Start(context.Background())
		t.Cleanup(n.Stop)
	}
	return tr, nodes
}

// waitSingleLeader ждет, пока среди живых узлов будет ровно один лидер, которого признают остальные.
func waitSingleLeader(t *testing.T, nodes []*Node, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*Node
		for _, n := range nodes {
			if ok, _ := n.IsLeader(); ok {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			l := leaders[0]
			agreed := true
			for _, n := range nodes {
				if id, _ := n.Leader(); id != l.ID() {
					agreed = false
				}
			}
			if agreed {
				return l
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no single leader within %v", timeout)
	return nil
}

func TestNewValidatesConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ID = 1
	cfg.Peers = map[int]string{2: "n2"}
	cfg.Transport = newMemTransport()
	if _, err := New(cfg); err == nil {
		t.Fatal("expected error when peers lack own id")
	}
	cfg.Peers = map[int]string{1: "n1"}
	cfg.HeartbeatInterval = 200 * time.Millisecond
	if _, err := New(cfg); err == nil {
		t.Fatal("expected error when heartbeat >= election timeout")
	}
}

func TestVoteOncePerTerm(t *testing.T) {
	n := newTestNode(t, 1, 3, newMemTransport())
	n.ctx, n.cancel = context.WithCancel(context.Background())
	defer n.cancel()

	if r := n.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: 2}); !r.VoteGranted {
		t.Fatal("first vote in term 1 must be granted")
	}
	if r := n.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: 2}); !r.VoteGranted {
		t.Fatal("repeated request from the same candidate must be granted")
	}
	if r := n.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: 3}); r.VoteGranted {
		t.Fatal("second candidate in the same term must be denied")
	}
	if r := n.HandleRequestVote(RequestVoteRequest{Term: 2, CandidateID: 3}); !r.VoteGranted || r.Term != 2 {
		t.Fatalf("higher term must reset vote, got %+v", r)
	}
	if r := n.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: 2}); r.VoteGranted || r.Term != 2 {
		t.Fatalf("stale term must be denied, got %+v", r)
	}
}

func TestAppendEntriesTermRules(t *testing.T) {
	n := newTestNode(t, 1, 3, newMemTransport())
	n.ctx, n.cancel = context.WithCancel(context.Background())
	defer n.cancel()
	n.currentTerm = 5
	n.role = Leader

	if r := n.HandleAppendEntries(AppendEntriesRequest{Term: 4, LeaderID: 2}); r.Success || r.Term != 5 {
		t.Fatalf("stale heartbeat must be rejected with current term, got %+v", r)
	}
	if ok, _ := n.IsLeader(); !ok {
		t.Fatal("stale heartbeat must not demote leader")
	}
	if r := n.HandleAppendEntries(AppendEntriesRequest{Term: 6, LeaderID: 2, LeaderAddr: "n2"}); !r.Success {
		t.Fatal("heartbeat with higher term must be accepted")
	}
	st := n.Status()
	if st.Role != Follower || st.Term != 6 || st.LeaderID != 2 || st.VotedFor != nil {
		t.Fatalf("unexpected status after higher-term heartbeat: %+v", st)
	}
	tr := n.Transitions()
	last := tr[len(tr)-1]
	if last.From != Leader || last.To != Follower || last.Term != 6 || last.Reason == "" {
		t.Fatalf("transition not logged correctly: %+v", last)
	}
}

func TestCandidateStepsDownOnHigherTermVoteResponse(t *testing.T) {
	n := newTestNode(t, 1, 3, newMemTransport())
	n.ctx, n.cancel = context.WithCancel(context.Background())
	defer n.cancel()
	n.mu.Lock()
	n.startElectionLocked(0)
	n.mu.Unlock()
	if st := n.Status(); st.Role != Candidate || st.Term != 1 {
		t.Fatalf("expected candidate in term 1, got %+v", st)
	}
	n.handleVoteResponse(1, 2, RequestVoteResponse{Term: 3, VoteGranted: false})
	if st := n.Status(); st.Role != Follower || st.Term != 3 {
		t.Fatalf("expected follower in term 3, got %+v", st)
	}
}

func TestElectionAndReelection(t *testing.T) {
	tr, nodes := startCluster(t, 3)
	start := time.Now()
	leader := waitSingleLeader(t, nodes, time.Second)
	firstTerm := leader.Term()
	t.Logf("leader %d elected in term %d after %v", leader.ID(), firstTerm, time.Since(start))

	tr.setDown(leader.cfg.Peers[leader.ID()], true)
	leader.Stop()
	killed := time.Now()

	var alive []*Node
	for _, n := range nodes {
		if n != leader {
			alive = append(alive, n)
		}
	}
	next := waitSingleLeader(t, alive, time.Second)
	t.Logf("new leader %d in term %d after %v", next.ID(), next.Term(), time.Since(killed))
	if next.Term() <= firstTerm {
		t.Fatalf("new leader term %d must exceed %d", next.Term(), firstTerm)
	}
}

func TestNoLeaderWithoutQuorum(t *testing.T) {
	tr, nodes := startCluster(t, 3)
	leader := waitSingleLeader(t, nodes, time.Second)
	var survivor *Node
	for _, n := range nodes {
		if n != leader && survivor == nil {
			survivor = n
			continue
		}
		tr.setDown(n.cfg.Peers[n.ID()], true)
		n.Stop()
	}
	termBefore := survivor.Term()
	time.Sleep(800 * time.Millisecond)
	st := survivor.Status()
	if st.Role == Leader {
		t.Fatal("single surviving node of 3 must not become leader")
	}
	if st.Term <= termBefore {
		t.Fatalf("survivor must keep starting elections, term %d -> %d", termBefore, st.Term)
	}
	t.Logf("survivor %d stays %s, term %d -> %d", survivor.ID(), st.Role, termBefore, st.Term)
}

func TestLeaderStepsDownWithoutQuorum(t *testing.T) {
	tr, nodes := startCluster(t, 3)
	leader := waitSingleLeader(t, nodes, time.Second)
	for _, n := range nodes {
		if n != leader {
			tr.setDown(n.cfg.Peers[n.ID()], true)
			n.Stop()
		}
	}
	lost := time.Now()
	deadline := lost.Add(time.Second)
	for {
		if ok, _ := leader.IsLeader(); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("leader without quorum must step down")
		}
		time.Sleep(5 * time.Millisecond)
	}
	trs := leader.Transitions()
	last := trs[len(trs)-1]
	if last.From != Leader || last.To != Follower || !strings.Contains(last.Reason, "lost quorum") {
		t.Fatalf("unexpected transition %+v", last)
	}
	if id, _ := leader.Leader(); id != 0 {
		t.Fatalf("stepped-down node must forget leader, got %d", id)
	}
	t.Logf("leader %d stepped down after %v: %s", leader.ID(), time.Since(lost), last.Reason)
}

func TestElectionTimeoutIsRandomInRange(t *testing.T) {
	n := newTestNode(t, 1, 3, newMemTransport())
	seen := map[time.Duration]bool{}
	for range 200 {
		n.resetElectionTimerLocked()
		d := n.electionTimeout
		if d < n.cfg.ElectionTimeoutMin || d >= n.cfg.ElectionTimeoutMax {
			t.Fatalf("timeout %v out of range", d)
		}
		seen[d] = true
	}
	if len(seen) < 50 {
		t.Fatalf("timeouts are not random enough: %d distinct values", len(seen))
	}
}
