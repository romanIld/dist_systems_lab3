package cluster

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"lab3/internal/queue"
	"lab3/internal/raft"
)

const testSeed = 20261004

// syncBuffer - потокобезопасный буфер для логов узлов.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// harness - кластер теста и журнал шагов с отметками времени от начала теста.
type harness struct {
	*Cluster
	t     *testing.T
	start time.Time
}

func newHarness(t *testing.T, size int, workTime time.Duration) *harness {
	t.Helper()
	logs := &syncBuffer{}
	start := time.Now()
	c, err := Start(Options{
		Size:     size,
		WorkTime: workTime,
		Seed:     testSeed,
		Logger:   slog.New(slog.NewTextHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{Cluster: c, t: t, start: start}
	h.step("cluster of %d nodes started: %v", size, c.peers)
	t.Cleanup(func() {
		c.Stop()
		if t.Failed() || os.Getenv("LAB3_NODE_LOGS") != "" {
			t.Logf("node logs:\n%s", logs.String())
		}
	})
	return h
}

func (h *harness) step(format string, args ...any) {
	h.t.Helper()
	h.t.Logf("[%7.3fs] %s", time.Since(h.start).Seconds(), fmt.Sprintf(format, args...))
}

func (h *harness) leader(timeout time.Duration) (int, time.Duration) {
	h.t.Helper()
	id, took, err := h.WaitLeader(timeout)
	if err != nil {
		h.t.Fatal(err)
	}
	return id, took
}

func (h *harness) term(id int) uint64 {
	h.t.Helper()
	st, err := h.Status(id)
	if err != nil {
		h.t.Fatal(err)
	}
	return st.Term
}

func (h *harness) submitN(n int, prefix string) []int64 {
	h.t.Helper()
	ids := make([]int64, 0, n)
	var assigned []int
	for i := range n {
		r, err := h.SubmitToLeader(fmt.Sprintf("%s %d", prefix, i+1))
		if err != nil {
			h.t.Fatal(err)
		}
		ids = append(ids, r.TaskID)
		assigned = append(assigned, r.AssignedNode)
	}
	h.step("submitted %d tasks %v -> nodes %v", n, ids, assigned)
	return ids
}

func (h *harness) registry(id int) map[int64]queue.Task {
	h.t.Helper()
	d, err := h.Registry(id)
	if err != nil {
		h.t.Fatal(err)
	}
	m := make(map[int64]queue.Task, len(d.Tasks))
	for _, t := range d.Tasks {
		m[t.ID] = t
	}
	return m
}

func (h *harness) local(id int) map[int64]queue.Task {
	h.t.Helper()
	nt, err := h.Tasks(id, id)
	if err != nil {
		h.t.Fatal(err)
	}
	if nt.Source != "local" {
		h.t.Fatalf("node %d answered its own tasks from %q", id, nt.Source)
	}
	m := make(map[int64]queue.Task, len(nt.Tasks))
	for _, t := range nt.Tasks {
		m[t.ID] = t
	}
	return m
}

// checkNoLoss проверяет, что у всех живых узлов одинаковый реестр из want задач,
// а каждая незавершенная задача лежит в памяти живого узла, за которым числится.
func (h *harness) checkNoLoss(want int) {
	h.t.Helper()
	ref, err := h.WaitConverged(2 * time.Second)
	if err != nil {
		h.t.Fatal(err)
	}
	if len(ref.Tasks) != want {
		h.t.Fatalf("registry has %d tasks, want %d", len(ref.Tasks), want)
	}
	for i, task := range ref.Tasks {
		if task.ID != int64(i+1) {
			h.t.Fatalf("task ids are not 1..%d: position %d has id %d", want, i, task.ID)
		}
	}
	alive := h.Alive()
	locals := map[int]map[int64]queue.Task{}
	for _, id := range alive {
		locals[id] = h.local(id)
		reg := h.registry(id)
		for _, task := range ref.Tasks {
			if reg[task.ID] != task {
				h.t.Fatalf("node %d registry differs for task %d: %+v vs %+v", id, task.ID, reg[task.ID], task)
			}
		}
	}
	perNode := map[int]int{}
	for _, task := range ref.Tasks {
		perNode[task.AssignedNode]++
		if task.Status == queue.Completed {
			continue
		}
		if !slices.Contains(alive, task.AssignedNode) {
			h.t.Fatalf("unfinished task %d is assigned to dead node %d", task.ID, task.AssignedNode)
		}
		if _, ok := locals[task.AssignedNode][task.ID]; !ok {
			h.t.Fatalf("task %d is not held by its node %d", task.ID, task.AssignedNode)
		}
	}
	h.step("all %d tasks known to nodes %v, registry epoch %d version %d, per node %v",
		want, alive, ref.Epoch, ref.Version, perNode)
}

// Сценарий 1 (4.1): после старта 3 узлов за < 1 с появляется ровно один лидер.
func TestScenario1LeaderElection(t *testing.T) {
	h := newHarness(t, 3, time.Hour)
	leader, _ := h.leader(time.Second)
	took := time.Since(h.start)
	term := h.term(leader)
	h.step("node %d became leader in term %d, %v after cluster start", leader, term, took.Round(time.Millisecond))
	if took >= time.Second {
		t.Fatalf("leader elected after %v, want < 1s", took)
	}

	for _, id := range h.Alive() {
		st, err := h.Status(id)
		if err != nil {
			t.Fatal(err)
		}
		wantRole := raft.Follower
		if id == leader {
			wantRole = raft.Leader
		}
		if st.Role != wantRole || st.LeaderID != leader || st.Term != term {
			t.Fatalf("node %d: role %s leader %d term %d", id, st.Role, st.LeaderID, st.Term)
		}
	}
	trs, err := h.Transitions(leader)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, tr := range trs {
		h.step("node %d transition %s -> %s term %d: %s", leader, tr.From, tr.To, tr.Term, tr.Reason)
		if tr.From == raft.Candidate && tr.To == raft.Leader && tr.Term == term && tr.Reason != "" {
			found = true
		}
	}
	if !found {
		t.Fatal("transition CANDIDATE -> LEADER with reason and term is missing in the log")
	}
}

// Сценарий 2: 10 задач распределяются по round-robin 1,2,3,1,... (4/3/3).
func TestScenario2TaskDistribution(t *testing.T) {
	h := newHarness(t, 3, time.Hour)
	h.leader(time.Second)
	for i := range 10 {
		r, err := h.SubmitToLeader(fmt.Sprintf("task %d", i+1))
		if err != nil {
			t.Fatal(err)
		}
		wantNode := i%3 + 1
		if r.TaskID != int64(i+1) || r.AssignedNode != wantNode {
			t.Fatalf("task %d: got id %d node %d, want id %d node %d", i+1, r.TaskID, r.AssignedNode, i+1, wantNode)
		}
	}
	want := map[int]int{1: 4, 2: 3, 3: 3}
	for id, n := range want {
		local := h.local(id)
		if len(local) != n {
			t.Fatalf("node %d holds %d tasks, want %d", id, len(local), n)
		}
		for _, task := range local {
			if task.AssignedNode != id {
				t.Fatalf("node %d holds task %d assigned to %d", id, task.ID, task.AssignedNode)
			}
		}
	}
	h.step("distribution by node: %v", want)
	h.checkNoLoss(10)
}

// 4.3: запрос к фолловеру приводит к ошибке с адресом лидера.
func TestOnlyLeaderAcceptsTasks(t *testing.T) {
	h := newHarness(t, 3, time.Hour)
	leader, _ := h.leader(time.Second)
	for _, id := range h.Alive() {
		if id == leader {
			continue
		}
		res, err := h.Submit(id, "to follower")
		if err != nil {
			t.Fatal(err)
		}
		h.step("POST /task to follower %d -> %d %q leader=%q address=%q", id, res.Code, res.Err.Message, res.Err.Leader, res.Err.LeaderAddress)
		if res.Code != http.StatusTemporaryRedirect || res.Err.Status != "error" ||
			res.Err.Leader != fmt.Sprintf("node%d", leader) || res.Err.LeaderAddress != h.Addr(leader) {
			t.Fatalf("follower %d must reject with leader address, got %+v", id, res)
		}
	}
	if n := len(h.registry(leader)); n != 0 {
		t.Fatalf("followers must not create tasks, registry has %d", n)
	}
	res, err := h.Submit(leader, "to leader")
	if err != nil || res.Code != http.StatusCreated || res.OK.Status != "success" {
		t.Fatalf("leader must accept task: %+v %v", res, err)
	}
	h.step("POST /task to leader %d -> %d task %d on node %d", leader, res.Code, res.OK.TaskID, res.OK.AssignedNode)
}

// Сценарий 3 (4.2): лидер падает, за < 1 с появляется новый с большим термом.
func TestScenario3LeaderFailure(t *testing.T) {
	h := newHarness(t, 3, time.Hour)
	old, _ := h.leader(time.Second)
	oldTerm := h.term(old)
	h.step("leader %d term %d", old, oldTerm)

	h.Kill(old)
	killed := time.Now()
	h.step("leader %d killed", old)
	next, took := h.leader(time.Second)
	newTerm := h.term(next)
	h.step("new leader %d term %d ready after %v", next, newTerm, took.Round(time.Millisecond))
	if next == old || newTerm <= oldTerm {
		t.Fatalf("new leader %d term %d, old %d term %d", next, newTerm, old, oldTerm)
	}

	var detected, elected time.Time
	for _, id := range h.Alive() {
		trs, err := h.Transitions(id)
		if err != nil {
			t.Fatal(err)
		}
		for _, tr := range trs {
			if tr.Time.Before(killed) {
				continue
			}
			h.step("node %d: %s -> %s term %d at +%v: %s", id, tr.From, tr.To, tr.Term, tr.Time.Sub(killed).Round(time.Millisecond), tr.Reason)
			if tr.To == raft.Candidate && (detected.IsZero() || tr.Time.Before(detected)) {
				detected = tr.Time
			}
			if tr.To == raft.Leader {
				elected = tr.Time
			}
		}
	}
	detect, elect := detected.Sub(killed), elected.Sub(killed)
	h.step("failure detected after %v, new leader elected after %v", detect.Round(time.Millisecond), elect.Round(time.Millisecond))
	if detected.IsZero() || detect > 350*time.Millisecond {
		t.Fatalf("detection took %v, want <= election timeout (300 ms)", detect)
	}
	if elected.IsZero() || elect > 500*time.Millisecond {
		t.Fatalf("election took %v, want <= 500 ms", elect)
	}
}

// Сценарии 4-5 (4.4, 4.5): задачи не теряются при смене лидера, новые распределяются
// по живым узлам, все узлы приходят к одинаковому состоянию очереди.
func TestScenario4And5TasksSurviveLeaderChange(t *testing.T) {
	h := newHarness(t, 3, time.Hour)
	old, _ := h.leader(time.Second)
	h.submitN(10, "before")
	h.checkNoLoss(10)

	h.Kill(old)
	h.step("leader %d killed", old)
	next, took := h.leader(time.Second)
	h.step("new leader %d ready after %v", next, took.Round(time.Millisecond))

	alive := h.Alive()
	var prev int
	for i := range 5 {
		r, err := h.SubmitToLeader(fmt.Sprintf("after %d", i+1))
		if err != nil {
			t.Fatal(err)
		}
		if r.TaskID != int64(11+i) {
			t.Fatalf("task id %d, want %d (ids continue after leader change)", r.TaskID, 11+i)
		}
		if !slices.Contains(alive, r.AssignedNode) || r.AssignedNode == prev {
			t.Fatalf("task %d assigned to %d: must go round-robin over alive %v", r.TaskID, r.AssignedNode, alive)
		}
		prev = r.AssignedNode
	}
	h.step("5 tasks after leader change distributed over %v", alive)
	h.checkNoLoss(15)
}

// Сценарий 6: падение фолловера - его незавершенные задачи переназначаются живым,
// лидер не меняется, новые задачи идут только живым узлам.
func TestScenario6FollowerFailure(t *testing.T) {
	h := newHarness(t, 3, time.Hour)
	leader, _ := h.leader(time.Second)
	term := h.term(leader)
	h.submitN(6, "task")
	victim := 1
	if victim == leader {
		victim = 2
	}
	victimTasks := h.local(victim)
	h.Kill(victim)
	killed := time.Now()
	h.step("follower %d killed, it held %d tasks", victim, len(victimTasks))

	deadline := time.Now().Add(2 * time.Second)
	for {
		reg := h.registry(leader)
		moved := 0
		for id := range victimTasks {
			if reg[id].AssignedNode != victim {
				moved++
			}
		}
		if moved == len(victimTasks) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d tasks of dead node reassigned", moved, len(victimTasks))
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.step("tasks of node %d reassigned after %v", victim, time.Since(killed).Round(time.Millisecond))

	for _, id := range h.submitN(4, "after") {
		if h.registry(leader)[id].AssignedNode == victim {
			t.Fatalf("task %d assigned to dead node %d", id, victim)
		}
	}
	if got, _ := h.leader(time.Second); got != leader || h.term(leader) != term {
		t.Fatalf("follower failure must not change leader: %d term %d", got, h.term(got))
	}
	h.checkNoLoss(10)
}

// Сценарий 7: перезапущенный фолловер (пустая память) возвращается в кластер,
// не сбивая лидера, восстанавливает свои задачи из реестра и снова получает новые.
func TestScenario7FollowerRecovery(t *testing.T) {
	h := newHarness(t, 3, 0)
	leader, _ := h.leader(time.Second)
	term := h.term(leader)
	h.submitN(6, "task")
	deadline := time.Now().Add(2 * time.Second)
	for {
		done := 0
		for _, task := range h.registry(leader) {
			if task.Status == queue.Completed {
				done++
			}
		}
		if done == 6 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of 6 tasks completed", done)
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.step("all 6 tasks completed")

	victim := 1
	if victim == leader {
		victim = 2
	}
	h.Kill(victim)
	h.step("follower %d killed", victim)
	time.Sleep(500 * time.Millisecond)
	h.submitN(2, "while down")

	if err := h.Restart(victim); err != nil {
		t.Fatal(err)
	}
	h.step("follower %d restarted with empty memory", victim)
	h.checkNoLoss(8)

	st, err := h.Status(victim)
	if err != nil {
		t.Fatal(err)
	}
	if st.Role != raft.Follower || st.LeaderID != leader || st.Term != term {
		t.Fatalf("restarted node must follow leader %d term %d, got %+v", leader, term, st.Status)
	}
	local := h.local(victim)
	for id, task := range h.registry(leader) {
		_, held := local[id]
		if (task.AssignedNode == victim) != held {
			t.Fatalf("task %d assigned to %d, held by restarted node: %v", id, task.AssignedNode, held)
		}
	}
	h.step("restarted node %d recovered %d own tasks from registry", victim, len(local))

	deadline = time.Now().Add(time.Second)
	for {
		ls, err := h.Status(leader)
		if err != nil {
			t.Fatal(err)
		}
		if slices.Contains(ls.AliveNodes, victim) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leader does not see node %d alive: %v", victim, ls.AliveNodes)
		}
		time.Sleep(10 * time.Millisecond)
	}
	got := false
	for _, id := range h.submitN(3, "after recovery") {
		if h.registry(leader)[id].AssignedNode == victim {
			got = true
		}
	}
	if !got {
		t.Fatalf("restarted node %d must be back in round-robin", victim)
	}
	h.checkNoLoss(11)
}

// Потеря кворума: при падении 2 из 3 узлов лидер отступает и задачи не принимаются;
// после возвращения одного узла снова выбирается лидер, задачи не потеряны.
func TestQuorumLossAndRecovery(t *testing.T) {
	h := newHarness(t, 3, time.Hour)
	leader, _ := h.leader(time.Second)
	h.submitN(6, "task")
	var down []int
	for _, id := range h.Alive() {
		if id != leader {
			down = append(down, id)
			h.Kill(id)
		}
	}
	lost := time.Now()
	h.step("followers %v killed, leader %d left alone", down, leader)

	deadline := lost.Add(time.Second)
	for {
		st, err := h.Status(leader)
		if err != nil {
			t.Fatal(err)
		}
		if st.Role != raft.Leader {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("leader without quorum must step down")
		}
		time.Sleep(5 * time.Millisecond)
	}
	trs, err := h.Transitions(leader)
	if err != nil {
		t.Fatal(err)
	}
	h.step("node %d stepped down after %v: %s", leader, time.Since(lost).Round(time.Millisecond), trs[len(trs)-1].Reason)
	res, err := h.Submit(leader, "no quorum")
	if err != nil {
		t.Fatal(err)
	}
	h.step("POST /task without quorum -> %d %q", res.Code, res.Err.Message)
	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("without quorum tasks must be rejected with 503, got %d", res.Code)
	}

	if err := h.Restart(down[0]); err != nil {
		t.Fatal(err)
	}
	h.step("node %d restarted", down[0])
	next, took := h.leader(2 * time.Second)
	h.step("leader %d elected %v after quorum restored", next, took.Round(time.Millisecond))
	h.checkNoLoss(6)
	h.submitN(2, "after quorum restored")
	h.checkNoLoss(8)
}
