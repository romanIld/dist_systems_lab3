package queue

import (
	"context"
	"slices"
	"time"
)

// Submit принимает задачу от клиента и назначает ее узлу по round-robin. Только для лидера.
func (s *Service) Submit(ctx context.Context, data, priority string) (Task, error) {
	isLeader, term := s.cons.IsLeader()
	if !isLeader {
		return Task{}, ErrNotLeader
	}
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()
	s.mu.Lock()
	if s.readyTerm != term {
		s.mu.Unlock()
		return Task{}, ErrNotReady
	}
	id := s.nextID
	s.nextID++
	s.mu.Unlock()
	t := Task{ID: id, Data: data, Priority: priority, Timestamp: time.Now().UTC(), Status: Pending}
	return s.deliverAndRecord(ctx, term, t)
}

// deliverAndRecord отправляет задачу следующему живому узлу; при отказе пробует следующий.
// Задача попадает в реестр только после подтверждения узлом. Вызывать под dispatchMu.
func (s *Service) deliverAndRecord(ctx context.Context, term uint64, t Task) (Task, error) {
	for range len(s.order) {
		s.mu.Lock()
		node, ok := s.nextAliveLocked()
		s.mu.Unlock()
		if !ok {
			break
		}
		t.AssignedNode = node
		if err := s.deliver(ctx, term, t); err != nil {
			s.log.Warn("task delivery failed", "task", t.ID, "target", node, "err", err)
			continue
		}
		s.mu.Lock()
		s.reg.put(t)
		s.mu.Unlock()
		return t, nil
	}
	return t, ErrNoNodes
}

func (s *Service) deliver(ctx context.Context, term uint64, t Task) error {
	if t.AssignedNode == s.cfg.NodeID {
		s.acceptTask(t)
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, s.cfg.RPCTimeout)
	defer cancel()
	return s.peer.sendTask(ctx, s.cfg.Peers[t.AssignedNode], InternalTaskRequest{
		Term:         term,
		LeaderID:     s.cfg.NodeID,
		TaskID:       t.ID,
		Data:         t.Data,
		Priority:     t.Priority,
		Timestamp:    t.Timestamp,
		Status:       t.Status,
		AssignedNode: t.AssignedNode,
	})
}

// nextAliveLocked - следующий узел по кругу среди живых (лидер всегда жив).
func (s *Service) nextAliveLocked() (int, bool) {
	for k := range len(s.order) {
		idx := (s.rrNext + k) % len(s.order)
		id := s.order[idx]
		if id == s.cfg.NodeID || s.alive[id] {
			s.rrNext = idx + 1
			return id, true
		}
	}
	return 0, false
}

// ApplyStatusReport обновляет статусы в реестре лидера. Пока синхронизация не
// завершена, все обновления возвращаются как неизвестные, чтобы узел их повторил.
func (s *Service) ApplyStatusReport(rep StatusReport) (StatusReportResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	isLeader, term := s.cons.IsLeader()
	if !isLeader {
		return StatusReportResult{}, ErrNotLeader
	}
	res := StatusReportResult{Unknown: []int64{}}
	for _, u := range rep.Updates {
		t, ok := s.reg.get(u.TaskID)
		if !ok || s.readyTerm != term {
			res.Unknown = append(res.Unknown, u.TaskID)
			continue
		}
		if t.AssignedNode != rep.NodeID || u.Status.rank() <= t.Status.rank() {
			continue // задача переназначена или статус устарел
		}
		t.Status = u.Status
		s.reg.put(t)
		res.Applied++
	}
	return res, nil
}

// OnPeerResult отмечает узел живым, если он ответил на heartbeat.
func (s *Service) OnPeerResult(peer int, ok bool) {
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastOK[peer] = time.Now()
	if !s.alive[peer] {
		s.alive[peer] = true
		s.log.Info("node is reachable, included in round-robin", "peer", peer)
	}
}

// livenessLoop на лидере находит узлы без ответа дольше DeadAfter и переназначает их задачи.
func (s *Service) livenessLoop() {
	t := time.NewTicker(s.cfg.DeadAfter / 6)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}
		isLeader, term := s.cons.IsLeader()
		if !isLeader {
			continue
		}
		s.mu.Lock()
		var dead []int
		if s.readyTerm == term {
			for _, id := range s.order {
				if id != s.cfg.NodeID && s.alive[id] && time.Since(s.lastOK[id]) > s.cfg.DeadAfter {
					s.alive[id] = false
					dead = append(dead, id)
				}
			}
		}
		s.mu.Unlock()
		if len(dead) == 0 {
			continue
		}
		s.log.Warn("nodes considered dead", "nodes", dead, "silence_over", s.cfg.DeadAfter)
		s.dispatchMu.Lock()
		s.reassignLocked(term, func(t Task) bool { return slices.Contains(dead, t.AssignedNode) })
		s.dispatchMu.Unlock()
	}
}

// reassignLocked раздает живым узлам незавершенные задачи, подходящие под match.
// Вызывать под dispatchMu. Возвращает число переназначенных задач.
func (s *Service) reassignLocked(term uint64, match func(Task) bool) int {
	s.mu.Lock()
	var orphans []Task
	for _, t := range s.reg.all() {
		if t.Status != Completed && match(t) {
			orphans = append(orphans, t)
		}
	}
	s.mu.Unlock()
	moved := 0
	for _, t := range orphans {
		from := t.AssignedNode
		t.Status = Pending
		nt, err := s.deliverAndRecord(s.ctx, term, t)
		if err != nil {
			s.log.Error("task reassignment failed", "task", t.ID, "err", err)
			break
		}
		s.log.Info("task reassigned", "task", t.ID, "from", from, "to", nt.AssignedNode)
		moved++
	}
	return moved
}
