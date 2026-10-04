package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"lab3/internal/api"
	"lab3/internal/httpx"
	"lab3/internal/queue"
	"lab3/internal/raft"
)

// timeline - события сценария с временем от запуска кластера.
type timeline struct {
	t0     time.Time
	events []event
}

type event struct {
	at   time.Time
	text string
}

func (tl *timeline) add(at time.Time, format string, args ...any) {
	tl.events = append(tl.events, event{at: at, text: fmt.Sprintf(format, args...)})
	fmt.Printf("  [%7.3fs] %s\n", at.Sub(tl.t0).Seconds(), tl.events[len(tl.events)-1].text)
}

// addTransitions добавляет переходы CANDIDATE/LEADER узлов, случившиеся после since.
func (tl *timeline) addTransitions(c *procCluster, since time.Time) []raft.Transition {
	var all []raft.Transition
	for _, id := range c.alive() {
		for _, tr := range c.transitions(id) {
			if tr.Time.After(since) && tr.To != raft.Follower {
				tl.add(tr.Time, "Node %d: %s -> %s (терм %d): %s", id, tr.From, tr.To, tr.Term, tr.Reason)
				all = append(all, tr)
			}
		}
	}
	return all
}

func (tl *timeline) rows() [][]string {
	sort.SliceStable(tl.events, func(i, j int) bool { return tl.events[i].at.Before(tl.events[j].at) })
	rows := make([][]string, len(tl.events))
	for i, e := range tl.events {
		rows[i] = []string{fmt.Sprintf("%.3f с", e.at.Sub(tl.t0).Seconds()), e.text}
	}
	return rows
}

// lastStart - момент, когда последний из узлов начал работу (запись "node started").
func lastStart(c *procCluster) time.Time {
	var t time.Time
	for _, id := range c.alive() {
		for _, tr := range c.transitions(id) {
			if tr.Reason == "node started" && tr.Time.After(t) {
				t = tr.Time
			}
		}
	}
	return t
}

func firstTransition(trs []raft.Transition, to raft.Role) time.Time {
	var t time.Time
	for _, tr := range trs {
		if tr.To == to && (t.IsZero() || tr.Time.Before(t)) {
			t = tr.Time
		}
	}
	return t
}

func verdict(ok bool) string {
	if ok {
		return "OK"
	}
	return "FAIL"
}

func submitN(c *procCluster, n int, prefix string) ([]int64, []int, error) {
	var ids []int64
	var nodes []int
	for i := range n {
		r, _, err := c.submit(fmt.Sprintf("%s %d", prefix, i+1))
		if err != nil {
			return nil, nil, err
		}
		ids = append(ids, r.TaskID)
		nodes = append(nodes, r.AssignedNode)
	}
	return ids, nodes, nil
}

// checkNoLoss проверяет полноту и согласованность очереди после сходимости реестров.
func checkNoLoss(c *procCluster, ref queue.RegistryDelta, want int) error {
	if len(ref.Tasks) != want {
		return fmt.Errorf("registry has %d tasks, want %d", len(ref.Tasks), want)
	}
	for i, t := range ref.Tasks {
		if t.ID != int64(i+1) {
			return fmt.Errorf("task ids are not 1..%d", want)
		}
	}
	alive := c.alive()
	held := map[int64]int{}
	for _, id := range alive {
		nt, err := c.localTasks(id)
		if err != nil {
			return err
		}
		for _, t := range nt.Tasks {
			held[t.ID] = id
		}
		reg, err := c.registry(id)
		if err != nil {
			return err
		}
		if !slices.Equal(reg.Tasks, ref.Tasks) {
			return fmt.Errorf("node %d registry differs from leader", id)
		}
	}
	for _, t := range ref.Tasks {
		if t.Status == queue.Completed {
			continue
		}
		if !slices.Contains(alive, t.AssignedNode) || held[t.ID] != t.AssignedNode {
			return fmt.Errorf("unfinished task %d of node %d is not held by a live node", t.ID, t.AssignedNode)
		}
	}
	return nil
}

func countByNode(ref queue.RegistryDelta) string {
	counts := map[int]int{}
	for _, t := range ref.Tasks {
		counts[t.AssignedNode]++
	}
	keys := make([]int, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("N%d:%d", k, counts[k])
	}
	return strings.Join(parts, ", ")
}

func countByStatus(ref queue.RegistryDelta) string {
	counts := map[queue.Status]int{}
	for _, t := range ref.Tasks {
		counts[t.Status]++
	}
	return fmt.Sprintf("PENDING %d, PROCESSING %d, COMPLETED %d",
		counts[queue.Pending], counts[queue.Processing], counts[queue.Completed])
}

// mainScenario - сценарии 1-7 подряд на одном кластере с таймлайном событий.
func mainScenario(o options, r *report) error {
	c := newProcCluster(o.bin, o.logDir, "main", 3, o.basePort, o.workTime)
	defer c.stopAll()
	tl := &timeline{t0: time.Now()}
	var rows [][]string
	row := func(n, name, action, expect, got string, ok bool) {
		rows = append(rows, []string{n, name, action, expect, got, verdict(ok)})
	}

	// 1. Выбор лидера.
	if err := c.startAll(); err != nil {
		return err
	}
	tl.add(tl.t0, "Запуск кластера (3 узла, work-time %s)", o.workTime)
	leader, readyAt, err := c.waitLeader(3 * time.Second)
	if err != nil {
		return err
	}
	trs := tl.addTransitions(c, tl.t0)
	tl.add(readyAt, "Node %d замечен лидером всеми узлами, синхронизация завершена", leader)
	st, _ := c.status(leader)
	started := lastStart(c)
	tl.add(started, "Все 3 процесса узлов запущены")
	elected := firstTransition(trs, raft.Leader).Sub(started)
	row("1", "Выбор лидера", "Запустить 3 узла", "Один узел становится лидером за < 1 с",
		fmt.Sprintf("Node %d - лидер (терм %d) через %s после старта узлов (запуск процессов ОС: %s)",
			leader, st.Term, msf(elected), msf(started.Sub(tl.t0))),
		elected < time.Second)

	// 2. Распределение задач.
	ids, nodes, err := submitN(c, 10, "task")
	if err != nil {
		return err
	}
	ref, err := c.waitConverged(3 * time.Second)
	if err != nil {
		return err
	}
	rr := true
	for i, n := range nodes {
		rr = rr && n == i%3+1 && ids[i] == int64(i+1)
	}
	dist := countByNode(ref)
	tl.add(time.Now(), "Добавлено 10 задач, распределение: %s", dist)
	row("2", "Распределение задач", "Добавить 10 задач", "Round-robin 1,2,3,1,...: N1:4, N2:3, N3:3",
		fmt.Sprintf("Задачи 1-10 -> узлы %v; %s", nodes, dist), rr && dist == "N1:4, N2:3, N3:3")

	time.Sleep(time.Second)
	before, err := c.registry(leader)
	if err != nil {
		return err
	}
	tl.add(time.Now(), "Состояние перед падением лидера: %s", countByStatus(before))

	// 3. Падение лидера.
	killed := time.Now()
	c.kill(leader)
	tl.add(killed, "Node %d (лидер) остановлен - эмуляция падения процесса", leader)
	newLeader, readyAt, err := c.waitLeader(3 * time.Second)
	if err != nil {
		return err
	}
	trs = tl.addTransitions(c, killed)
	tl.add(readyAt, "Node %d готов принимать задачи: задачи собраны со всех узлов", newLeader)
	after, err := c.registry(newLeader)
	if err != nil {
		return err
	}
	moved := 0
	afterByID := map[int64]queue.Task{}
	for _, t := range after.Tasks {
		afterByID[t.ID] = t
	}
	for _, t := range before.Tasks {
		if t.AssignedNode == leader && t.Status != queue.Completed && afterByID[t.ID].AssignedNode != leader {
			moved++
		}
	}
	tl.add(readyAt, "Незавершенные задачи упавшего лидера переназначены: %d", moved)
	st, _ = c.status(newLeader)
	detect := firstTransition(trs, raft.Candidate).Sub(killed)
	elect := firstTransition(trs, raft.Leader).Sub(killed)
	ready := readyAt.Sub(killed)
	tl.add(readyAt, "Система восстановлена за %s", msf(ready))
	row("3", "Падение лидера", "Остановить процесс лидера", "Через < 1 с появляется новый лидер",
		fmt.Sprintf("Node %d - лидер (терм %d): обнаружение %s, выборы %s, готов %s; переназначено %d задач",
			newLeader, st.Term, msf(detect), msf(elect), msf(ready), moved),
		ready < time.Second && detect <= 300*time.Millisecond+20*time.Millisecond && elect <= 500*time.Millisecond)

	// 4. Работа после падения.
	ids, nodes, err = submitN(c, 5, "after leader change")
	if err != nil {
		return err
	}
	ok := ids[0] == 11 && ids[4] == 15
	for _, n := range nodes {
		ok = ok && slices.Contains(c.alive(), n)
	}
	tl.add(time.Now(), "Добавлено 5 задач после смены лидера: %v -> узлы %v", ids, nodes)
	row("4", "Работа после падения", "Добавить 5 задач новому лидеру", "Задачи принимаются и распределяются по живым узлам",
		fmt.Sprintf("Задачи %d-%d -> узлы %v", ids[0], ids[4], nodes), ok)

	// 5. Сохранность задач.
	ref, err = c.waitConverged(3 * time.Second)
	if err != nil {
		return err
	}
	lossErr := checkNoLoss(c, ref, 15)
	tl.add(time.Now(), "Проверка всех узлов: %d задач, реестры совпадают (epoch %d, version %d)", len(ref.Tasks), ref.Epoch, ref.Version)
	row("5", "Сохранность задач", "Сравнить задачи на всех узлах", "Задачи не потеряны, все учтены, состояние согласовано",
		fmt.Sprintf("15/15 задач на узлах %v, реестры совпадают; %s; %s", c.alive(), countByNode(ref), errText(lossErr)),
		lossErr == nil)

	// Бывший лидер возвращается фолловером, чтобы в сценарии 6 кластер снова был из 3 узлов.
	oldBack := time.Now()
	if err := c.start(leader); err != nil {
		return err
	}
	tl.add(oldBack, "Node %d (бывший лидер) перезапущен с пустой памятью", leader)
	if _, err := c.waitConverged(3 * time.Second); err != nil {
		return err
	}
	if err := waitAlive(c, newLeader, leader, 2*time.Second); err != nil {
		return err
	}
	ost, _ := c.status(leader)
	tl.add(time.Now(), "Node %d вернулся: %s лидера %d (терм %d), реестр получен, лидер включил его в round-robin",
		leader, ost.Role, ost.LeaderID, ost.Term)

	// 6. Отказ фолловера: останавливаем тот, который держит задачи (не вернувшийся бывший лидер).
	follower := 0
	for _, id := range c.alive() {
		if id != newLeader && id != leader {
			follower = id
		}
	}
	var victim []int64
	for _, t := range ref.Tasks {
		if t.AssignedNode == follower && t.Status != queue.Completed {
			victim = append(victim, t.ID)
		}
	}
	fKilled := time.Now()
	c.kill(follower)
	tl.add(fKilled, "Node %d (фолловер) остановлен, незавершенных задач на нем: %d", follower, len(victim))
	deadAt, err := waitDead(c, newLeader, follower, 3*time.Second)
	if err != nil {
		return err
	}
	tl.add(deadAt, "Лидер признал Node %d упавшим (нет ответа на heartbeat дольше 300 мс)", follower)
	reassignedAt, err := waitReassigned(c, newLeader, follower, victim, 3*time.Second)
	if err != nil {
		return err
	}
	tl.add(reassignedAt, "Незавершенные задачи Node %d переназначены живым узлам: %d", follower, len(victim))
	ids, nodes, err = submitN(c, 3, "follower down")
	if err != nil {
		return err
	}
	sameLeader, _, err := c.waitLeader(time.Second)
	if err != nil {
		return err
	}
	tl.add(time.Now(), "Добавлено 3 задачи: %v -> узлы %v", ids, nodes)
	row("6", "Отказ фолловера", "Вернуть бывшего лидера, остановить фолловера, добавить 3 задачи",
		"Лидер не меняется, задачи фолловера переназначаются, новые идут только живым",
		fmt.Sprintf("Лидер Node %d без изменений; %d задач переназначены через %s; новые задачи -> %v",
			sameLeader, len(victim), msf(reassignedAt.Sub(fKilled)), nodes),
		sameLeader == newLeader && !slices.Contains(nodes, follower))

	// 7. Восстановление фолловера.
	restarted := time.Now()
	if err := c.start(follower); err != nil {
		return err
	}
	tl.add(restarted, "Node %d перезапущен с пустой памятью", follower)
	ref, err = c.waitConverged(3 * time.Second)
	if err != nil {
		return err
	}
	convergedAt := time.Now()
	fst, _ := c.status(follower)
	own, _ := c.localTasks(follower)
	tl.add(convergedAt, "Node %d: %s лидера %d (терм %d), реестр получен, своих задач восстановлено %d",
		follower, fst.Role, fst.LeaderID, fst.Term, own.Count)
	if err := waitAlive(c, newLeader, follower, 2*time.Second); err != nil {
		return err
	}
	ids, nodes, err = submitN(c, 3, "follower back")
	if err != nil {
		return err
	}
	tl.add(time.Now(), "Добавлено 3 задачи: %v -> узлы %v", ids, nodes)
	ref, err = c.waitConverged(3 * time.Second)
	if err != nil {
		return err
	}
	lossErr = checkNoLoss(c, ref, 21)
	tl.add(time.Now(), "Итог: %d задач, %s; %s", len(ref.Tasks), countByNode(ref), countByStatus(ref))
	row("7", "Восстановление фолловера", "Перезапустить фолловера, добавить 3 задачи",
		"Узел возвращается фолловером, получает реестр и снова участвует в round-robin",
		fmt.Sprintf("Node %d - %s (терм %d), реестр получен за %s, своих задач %d; новые задачи -> %v; всего %d задач: %s",
			follower, fst.Role, fst.Term, msf(convergedAt.Sub(restarted)), own.Count, nodes, len(ref.Tasks), errText(lossErr)),
		fst.Role == raft.Follower && fst.LeaderID == newLeader && slices.Contains(nodes, follower) && lossErr == nil)

	r.h2("Сценарии 1-7 на одном кластере")
	r.table([]string{"N", "Сценарий", "Действия", "Ожидаемый результат", "Фактический результат", "Итог"}, rows)
	r.h2("Таймлайн основного прогона")
	r.table([]string{"Время", "Событие"}, tl.rows())
	return nil
}

func errText(err error) string {
	if err != nil {
		return "ошибка: " + err.Error()
	}
	return "потерь нет"
}

func waitReassigned(c *procCluster, leader, dead int, ids []int64, timeout time.Duration) (time.Time, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		reg, err := c.registry(leader)
		if err != nil {
			return time.Time{}, err
		}
		done := true
		for _, t := range reg.Tasks {
			if slices.Contains(ids, t.ID) && t.AssignedNode == dead {
				done = false
			}
		}
		if done {
			return time.Now(), nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return time.Time{}, fmt.Errorf("tasks of node %d not reassigned within %v", dead, timeout)
}

// waitDead ждет, пока лидер исключит узел из списка живых.
func waitDead(c *procCluster, leader, id int, timeout time.Duration) (time.Time, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, err := c.status(leader)
		if err == nil && !slices.Contains(st.AliveNodes, id) {
			return time.Now(), nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return time.Time{}, fmt.Errorf("leader %d still sees node %d alive", leader, id)
}

func waitAlive(c *procCluster, leader, id int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, err := c.status(leader)
		if err == nil && slices.Contains(st.AliveNodes, id) {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fmt.Errorf("leader %d does not see node %d", leader, id)
}

// coldStart - время выбора лидера при одновременном запуске узлов.
func coldStart(o options, r *report) error {
	var rows [][]string
	for i := range o.runs {
		c := newProcCluster(o.bin, o.logDir, "cold", 3, o.basePort, time.Hour)
		t0 := time.Now()
		if err := c.startAll(); err != nil {
			return err
		}
		leader, readyAt, err := c.waitLeader(3 * time.Second)
		if err != nil {
			c.stopAll()
			return err
		}
		started := lastStart(c)
		var trs []raft.Transition
		for _, id := range c.alive() {
			trs = append(trs, c.transitions(id)...)
		}
		c.stopAll()
		electAt := firstTransition(trs, raft.Leader)
		rounds := 0
		for _, tr := range trs {
			if tr.To == raft.Candidate {
				rounds++
			}
		}
		elect, ready := electAt.Sub(t0), readyAt.Sub(t0)
		r.cold = append(r.cold, electAt.Sub(started))
		r.coldLaunch = append(r.coldLaunch, elect)
		r.coldReady = append(r.coldReady, ready)
		rows = append(rows, []string{fmt.Sprint(i + 1), fmt.Sprintf("Node %d", leader), msf(started.Sub(t0)),
			msf(electAt.Sub(started)), msf(elect), msf(ready), fmt.Sprint(rounds)})
	}
	r.h2(fmt.Sprintf("Холодный старт (%d запусков)", o.runs))
	r.p("Время от запуска процессов. \"Старт узлов\" - когда последний процесс начал работу; " +
		"\"выборы\" - от этого момента до перехода в LEADER; \"кандидатов\" - число переходов в CANDIDATE " +
		"(больше 1 - были конкурирующие кандидаты).")
	r.table([]string{"Запуск", "Лидер", "Старт узлов", "Выборы", "LEADER от запуска", "Готов принимать задачи", "Кандидатов"}, rows)
	r.p("Выборы от старта узлов: " + summarize(r.cold).String() + ". LEADER от запуска процессов: " +
		summarize(r.coldLaunch).String() + ". Готов: " + summarize(r.coldReady).String() + ".")
	return nil
}

// leaderFailover - время обнаружения падения лидера и выборов нового.
func leaderFailover(o options, r *report) error {
	var rows [][]string
	var detects, elects []time.Duration
	for i := range o.runs {
		c := newProcCluster(o.bin, o.logDir, "failover", 3, o.basePort, time.Hour)
		if err := c.startAll(); err != nil {
			return err
		}
		leader, _, err := c.waitLeader(3 * time.Second)
		if err != nil {
			c.stopAll()
			return err
		}
		time.Sleep(300 * time.Millisecond)
		killed := time.Now()
		c.kill(leader)
		next, readyAt, err := c.waitLeader(3 * time.Second)
		if err != nil {
			c.stopAll()
			return err
		}
		var trs []raft.Transition
		for _, id := range c.alive() {
			for _, tr := range c.transitions(id) {
				if tr.Time.After(killed) {
					trs = append(trs, tr)
				}
			}
		}
		c.stopAll()
		rounds := 0
		for _, tr := range trs {
			if tr.To == raft.Candidate {
				rounds++
			}
		}
		detect := firstTransition(trs, raft.Candidate).Sub(killed)
		elect := firstTransition(trs, raft.Leader).Sub(killed)
		ready := readyAt.Sub(killed)
		detects, elects = append(detects, detect), append(elects, elect)
		r.failover = append(r.failover, elect)
		r.failoverRdy = append(r.failoverRdy, ready)
		rows = append(rows, []string{fmt.Sprint(i + 1), fmt.Sprintf("Node %d -> Node %d", leader, next),
			msf(detect), msf(elect), msf(ready), fmt.Sprint(rounds)})
	}
	r.h2(fmt.Sprintf("Падение лидера (%d запусков)", o.runs))
	r.p("Время от остановки процесса лидера: обнаружение - первый переход фолловера в CANDIDATE, " +
		"выборы - переход нового лидера в LEADER, готов - лидер синхронизировал задачи и признан всеми узлами.")
	r.table([]string{"Запуск", "Смена лидера", "Обнаружение", "Выборы", "Готов", "Кандидатов"}, rows)
	r.p("Обнаружение: " + summarize(detects).String() + ". Выборы: " + summarize(elects).String() +
		". Готов: " + summarize(r.failoverRdy).String() + ".")
	return nil
}

// postTask - один POST /task без повторов (для нагрузочных замеров).
func postTask(c *http.Client, addr, data string) (api.TaskResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp api.TaskResponse
	err := httpx.PostJSON(ctx, c, addr+"/task", api.TaskRequest{Data: data}, &resp)
	return resp, err
}

// concurrentSubmit отправляет n задач лидеру из workers параллельных клиентов.
func concurrentSubmit(c *procCluster, n, workers int, prefix string) (time.Duration, error) {
	addr := c.leader
	var next atomic.Int64
	var firstErr error
	var mu sync.Mutex
	var wg sync.WaitGroup
	start := time.Now()
	for range workers {
		wg.Go(func() {
			for {
				i := next.Add(1)
				if i > int64(n) {
					return
				}
				if _, err := postTask(c.http, addr, fmt.Sprintf("%s %d", prefix, i)); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					return
				}
			}
		})
	}
	wg.Wait()
	return time.Since(start), firstErr
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}

// performance - задержка добавления, пропускная способность, большой объем задач.
func performance(o options, r *report) error {
	c := newProcCluster(o.bin, o.logDir, "perf", 3, o.basePort, 0)
	defer c.stopAll()
	if err := c.startAll(); err != nil {
		return err
	}
	if _, _, err := c.waitLeader(3 * time.Second); err != nil {
		return err
	}

	start := time.Now()
	for i := range o.seqN {
		t := time.Now()
		if _, err := postTask(c.http, c.leader, fmt.Sprintf("seq %d", i)); err != nil {
			return err
		}
		r.latency = append(r.latency, time.Since(t))
	}
	seqTotal := time.Since(start)
	r.seqRate = float64(o.seqN) / seqTotal.Seconds()
	sorted := slices.Clone(r.latency)
	slices.Sort(sorted)

	concTotal, err := concurrentSubmit(c, o.concN, o.conc, "conc")
	if err != nil {
		return err
	}
	r.concRate = float64(o.concN) / concTotal.Seconds()

	r.h2("Производительность")
	r.p("Отдельный кластер из 3 процессов, work-time 0 (задачи выполняются сразу, " +
		"каждая дает два отчета о статусе лидеру). Клиент на той же машине.")
	r.table([]string{"Замер", "Значение"}, [][]string{
		{fmt.Sprintf("Задержка POST /task, %d последовательных запросов", o.seqN),
			fmt.Sprintf("p50 %s, p95 %s, p99 %s, макс %s", usf(percentile(sorted, 0.5)), usf(percentile(sorted, 0.95)),
				usf(percentile(sorted, 0.99)), usf(sorted[len(sorted)-1]))},
		{"Пропускная способность, 1 клиент", fmt.Sprintf("%.0f задач/с", r.seqRate)},
		{fmt.Sprintf("Пропускная способность, %d клиентов, %d задач", o.conc, o.concN), fmt.Sprintf("%.0f задач/с", r.concRate)},
	})

	// Большой объем: доводим число задач до bigN.
	filled := o.seqN + o.concN
	if rest := o.bigN - filled; rest > 0 {
		t, err := concurrentSubmit(c, rest, o.conc, "big")
		if err != nil {
			return err
		}
		r.bigFill = t
	}
	r.bigN = max(o.bigN, filled)
	convStart := time.Now()
	ref, err := c.waitConverged(60 * time.Second)
	if err != nil {
		return err
	}
	conv := time.Since(convStart)
	if len(ref.Tasks) != r.bigN {
		return fmt.Errorf("registry has %d tasks, want %d", len(ref.Tasks), r.bigN)
	}
	leader, _, err := c.waitLeader(time.Second)
	if err != nil {
		return err
	}
	var heapRows []string
	for _, id := range c.alive() {
		st, err := c.status(id)
		if err != nil {
			return err
		}
		r.bigHeap = append(r.bigHeap, st.HeapMB)
		heapRows = append(heapRows, fmt.Sprintf("Node %d: %.1f МБ", id, st.HeapMB))
	}

	follower := 1
	if follower == leader {
		follower = 2
	}
	c.kill(follower)
	restarted := time.Now()
	if err := c.start(follower); err != nil {
		return err
	}
	if err := waitKnown(c, follower, r.bigN, 30*time.Second); err != nil {
		return err
	}
	r.bigPull = time.Since(restarted)

	killed := time.Now()
	c.kill(leader)
	if _, _, err := c.waitLeader(10 * time.Second); err != nil {
		return err
	}
	r.bigLF = time.Since(killed)
	ref, err = c.waitConverged(30 * time.Second)
	if err != nil {
		return err
	}

	r.h2(fmt.Sprintf("Большой объем: %d задач", r.bigN))
	r.table([]string{"Замер", "Значение"}, [][]string{
		{"Задач в системе", fmt.Sprint(len(ref.Tasks))},
		{"Распределение", countByNode(ref)},
		{"Сходимость реестров на всех узлах после загрузки", msf(conv)},
		{"Память (heap) узлов", strings.Join(heapRows, "; ")},
		{"Перезапуск фолловера: получение полного реестра", msf(r.bigPull)},
		{"Падение лидера: новый лидер готов (сбор задач со всех узлов)", msf(r.bigLF)},
	})
	return nil
}

func waitKnown(c *procCluster, id, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, err := c.status(id)
		if err == nil && st.KnownTasks == want && st.Role == raft.Follower {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return errors.New("restarted node did not receive the registry")
}

func usf(d time.Duration) string { return fmt.Sprintf("%.2f мс", float64(d.Microseconds())/1000) }

// summary - обязательная таблица характеристик из методички.
func (r *report) summary() {
	sorted := slices.Clone(r.latency)
	slices.Sort(sorted)
	heap := 0.0
	for _, h := range r.bigHeap {
		heap = max(heap, h)
	}
	cold, fo := summarize(r.cold), summarize(r.failoverRdy)
	r.h2("Характеристики системы")
	r.table([]string{"Параметр", "Значение"}, [][]string{
		{"Количество узлов", "3 (поддерживается 3-5 и больше, задается -peers)"},
		{"Время выбора лидера (холодный старт)", cold.String() + " от старта узлов; с запуском процессов ОС: " +
			summarize(r.coldLaunch).String()},
		{"Время восстановления после падения", fo.String() + "; выборы: " + summarize(r.failover).String()},
		{"Heartbeat интервал", "50 мс"},
		{"Election timeout", "случайный, 150-300 мс"},
		{"Максимальное число задач в системе", fmt.Sprintf("проверено %d (heap до %.0f МБ на узел); ограничено памятью", r.bigN, heap)},
		{"Время добавления задачи", fmt.Sprintf("p50 %s, p99 %s", usf(percentile(sorted, 0.5)), usf(percentile(sorted, 0.99)))},
		{"Пропускная способность (задач/сек)", fmt.Sprintf("%.0f (1 клиент), %.0f (параллельные клиенты)", r.seqRate, r.concRate)},
	})
}
