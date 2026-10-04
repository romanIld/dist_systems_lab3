package queue

import (
	"context"
	"errors"
	"testing"
)

// fakeConsensus - управляемое состояние выборов для тестов.
type fakeConsensus struct {
	leader bool
	term   uint64
}

func (f *fakeConsensus) Term() uint64             { return f.term }
func (f *fakeConsensus) IsLeader() (bool, uint64) { return f.leader, f.term }
func (f *fakeConsensus) Leader() (int, string)    { return 0, "" }

func newTestService(t *testing.T, self int, cons Consensus) *Service {
	t.Helper()
	s, err := New(Config{
		NodeID: self,
		Peers:  map[int]string{1: "http://n1", 2: "http://n2", 3: "http://n3"},
	}, cons)
	if err != nil {
		t.Fatal(err)
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	t.Cleanup(s.cancel)
	return s
}

func TestRoundRobinSkipsDeadNodes(t *testing.T) {
	s := newTestService(t, 2, &fakeConsensus{})
	s.alive = map[int]bool{1: true, 3: true}
	var got []int
	for range 6 {
		id, _ := s.nextAliveLocked()
		got = append(got, id)
	}
	if want := []int{1, 2, 3, 1, 2, 3}; !equalInts(got, want) {
		t.Fatalf("round-robin %v, want %v", got, want)
	}
	s.alive[3] = false
	got = got[:0]
	for range 4 {
		id, _ := s.nextAliveLocked()
		got = append(got, id)
	}
	if want := []int{1, 2, 1, 2}; !equalInts(got, want) {
		t.Fatalf("round-robin without node 3: %v, want %v", got, want)
	}
}

func TestSubmitRequiresLeaderAndSync(t *testing.T) {
	cons := &fakeConsensus{term: 1}
	s := newTestService(t, 1, cons)
	if _, err := s.Submit(context.Background(), "x", ""); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower: got %v, want ErrNotLeader", err)
	}
	cons.leader = true
	if _, err := s.Submit(context.Background(), "x", ""); !errors.Is(err, ErrNotReady) {
		t.Fatalf("leader before sync: got %v, want ErrNotReady", err)
	}
	s.readyTerm, s.nextID = 1, 1
	task, err := s.Submit(context.Background(), "x", "high")
	if err != nil || task.ID != 1 || task.AssignedNode != 1 || task.Status != Pending {
		t.Fatalf("submit to self: %+v %v", task, err)
	}
	if _, ok := s.reg.get(1); !ok {
		t.Fatal("accepted task must be recorded in registry")
	}
}

func TestAcceptRejectsStaleTerm(t *testing.T) {
	s := newTestService(t, 2, &fakeConsensus{term: 5})
	if err := s.Accept(InternalTaskRequest{Term: 4, TaskID: 1}); !errors.Is(err, ErrStaleTerm) {
		t.Fatalf("got %v, want ErrStaleTerm", err)
	}
	if err := s.Accept(InternalTaskRequest{Term: 5, TaskID: 1, Data: "d"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Accept(InternalTaskRequest{Term: 5, TaskID: 1, Data: "d"}); err != nil {
		t.Fatal("repeated delivery must be idempotent")
	}
	own := s.OwnTasks()
	if len(own) != 1 || own[0].AssignedNode != 2 || own[0].Status != Pending {
		t.Fatalf("unexpected own tasks %+v", own)
	}
}

func TestApplyStatusReport(t *testing.T) {
	cons := &fakeConsensus{leader: true, term: 2}
	s := newTestService(t, 1, cons)
	s.reg.put(Task{ID: 1, AssignedNode: 2, Status: Pending})
	s.reg.put(Task{ID: 2, AssignedNode: 3, Status: Pending})

	res, _ := s.ApplyStatusReport(StatusReport{NodeID: 2, Updates: []StatusUpdate{{TaskID: 1, Status: Processing}}})
	if res.Applied != 0 || len(res.Unknown) != 1 {
		t.Fatalf("before sync updates must be returned for retry: %+v", res)
	}
	s.readyTerm = 2
	res, _ = s.ApplyStatusReport(StatusReport{NodeID: 2, Updates: []StatusUpdate{
		{TaskID: 1, Status: Completed},
		{TaskID: 2, Status: Completed}, // задача числится за узлом 3
		{TaskID: 9, Status: Completed}, // неизвестна лидеру
	}})
	if res.Applied != 1 || len(res.Unknown) != 1 || res.Unknown[0] != 9 {
		t.Fatalf("unexpected result %+v", res)
	}
	res, _ = s.ApplyStatusReport(StatusReport{NodeID: 2, Updates: []StatusUpdate{{TaskID: 1, Status: Processing}}})
	if t1, _ := s.reg.get(1); res.Applied != 0 || t1.Status != Completed {
		t.Fatalf("status must not go back: %+v", t1)
	}
	if t2, _ := s.reg.get(2); t2.Status != Pending {
		t.Fatalf("report from non-owner must be ignored: %+v", t2)
	}
	cons.leader = false
	if _, err := s.ApplyStatusReport(StatusReport{}); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("got %v, want ErrNotLeader", err)
	}
}

func TestReconcileOwnTasks(t *testing.T) {
	s := newTestService(t, 2, &fakeConsensus{})
	s.acceptTask(Task{ID: 1})
	s.acceptTask(Task{ID: 2})
	s.reconcileOwnLocked([]Task{
		{ID: 1, AssignedNode: 3, Status: Pending},   // переназначена на другой узел
		{ID: 2, AssignedNode: 2, Status: Pending},   // остается
		{ID: 3, AssignedNode: 2, Status: Completed}, // была у узла до перезапуска
		{ID: 4, AssignedNode: 2, Status: Processing},
	})
	own := s.OwnTasks()
	if len(own) != 3 || own[0].ID != 2 || own[1].ID != 3 || own[2].ID != 4 {
		t.Fatalf("unexpected own tasks %+v", own)
	}
	if own[1].Status != Completed || own[2].Status != Pending {
		t.Fatalf("adopted unfinished task must restart from PENDING: %+v", own)
	}
}

func TestMergeSnapshots(t *testing.T) {
	old := RegistryDelta{Epoch: 1, Version: 3, Tasks: []Task{
		{ID: 1, AssignedNode: 1, Status: Pending},
		{ID: 2, AssignedNode: 2, Status: Pending},
	}}
	fresh := RegistryDelta{Epoch: 1, Version: 5, Tasks: []Task{
		{ID: 1, AssignedNode: 1, Status: Completed},
		{ID: 2, AssignedNode: 2, Status: Pending},
		{ID: 3, AssignedNode: 3, Status: Pending},
	}}
	m := mergeSnapshots([]peerSnapshot{
		{NodeID: 2, Registry: old, Tasks: []Task{{ID: 2, Status: Processing}}},
		{NodeID: 3, Registry: fresh, Tasks: []Task{{ID: 3, Status: Pending}, {ID: 4, Status: Pending}}},
		{NodeID: 1, Err: errors.New("down")},
	})
	if len(m.Tasks) != 4 {
		t.Fatalf("want 4 tasks, got %+v", m.Tasks)
	}
	if m.Tasks[1].Status != Completed {
		t.Fatalf("freshest replica must win for task 1: %+v", m.Tasks[1])
	}
	if m.Tasks[2].Status != Processing || !m.Held[2] {
		t.Fatalf("holder status must win for task 2: %+v", m.Tasks[2])
	}
	if m.Tasks[4].AssignedNode != 3 || !m.Held[4] {
		t.Fatalf("task known only to holder must be recovered: %+v", m.Tasks[4])
	}
	if m.Held[1] || m.Responded[1] || !m.Responded[2] || !m.Responded[3] {
		t.Fatalf("unexpected held/responded: %+v %+v", m.Held, m.Responded)
	}
}

func TestMergeSkipsUnchangedReplica(t *testing.T) {
	own := RegistryDelta{Epoch: 2, Version: 4, Tasks: []Task{{ID: 1, AssignedNode: 2, Status: Pending}}}
	m := mergeSnapshots([]peerSnapshot{
		{NodeID: 1, Registry: own},
		{NodeID: 2, Registry: RegistryDelta{Epoch: 2, Version: 3, Unchanged: true}, Tasks: []Task{{ID: 1, Status: Processing}}},
	})
	if len(m.Tasks) != 1 || m.Tasks[1].Status != Processing || !m.Held[1] || !m.Responded[2] {
		t.Fatalf("held tasks of a node with unchanged replica must still be merged: %+v", m)
	}
}

func TestRegistryUnchanged(t *testing.T) {
	s := newTestService(t, 1, &fakeConsensus{})
	s.reg = newRegistry(2)
	s.reg.put(Task{ID: 1})
	s.reg.put(Task{ID: 2})
	if d := s.Registry(0, &RegistryDelta{Epoch: 2, Version: 2}); !d.Unchanged || len(d.Tasks) != 0 {
		t.Fatalf("replica not newer than known must not be sent: %+v", d)
	}
	if d := s.Registry(0, &RegistryDelta{Epoch: 2, Version: 1}); d.Unchanged || len(d.Tasks) != 2 {
		t.Fatalf("newer replica must be sent in full: %+v", d)
	}
	if d := s.Registry(0, &RegistryDelta{Epoch: 1, Version: 9}); d.Unchanged || len(d.Tasks) != 2 {
		t.Fatalf("replica of another epoch must be sent: %+v", d)
	}
	if d := s.Registry(1, nil); d.Full || len(d.Tasks) != 1 || d.Tasks[0].ID != 2 {
		t.Fatalf("delta since version 1 must contain only task 2: %+v", d)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
