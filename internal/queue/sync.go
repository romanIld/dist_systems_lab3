package queue

import (
	"context"
	"slices"
	"sort"
	"sync"
	"time"
)

// peerSnapshot - что новый лидер получил от узла при синхронизации.
type peerSnapshot struct {
	NodeID   int
	Tasks    []Task        // GET /internal/tasks: задачи, которые узел реально держит
	Registry RegistryDelta // GET /internal/registry: реплика реестра узла
	Err      error
}

// mergeResult - итог слияния состояния кластера.
type mergeResult struct {
	Tasks     map[int64]Task
	Held      map[int64]bool // задача есть в памяти назначенного узла
	Responded map[int]bool
}

// mergeSnapshots собирает полный список задач. База - самая свежая реплика реестра,
// к ней добавляются задачи из остальных реплик, а затем поверх - задачи, которые
// узлы реально держат (владелец и статус берутся у держателя).
func mergeSnapshots(snaps []peerSnapshot) mergeResult {
	res := mergeResult{Tasks: map[int64]Task{}, Held: map[int64]bool{}, Responded: map[int]bool{}}
	var ok []peerSnapshot
	for _, p := range snaps {
		if p.Err == nil {
			ok = append(ok, p)
			res.Responded[p.NodeID] = true
		}
	}
	ok = slices.DeleteFunc(ok, func(p peerSnapshot) bool { return p.Registry.Unchanged })
	sort.SliceStable(ok, func(i, j int) bool {
		a, b := ok[i].Registry, ok[j].Registry
		if a.Epoch != b.Epoch {
			return a.Epoch > b.Epoch
		}
		return a.Version > b.Version
	})
	for _, p := range ok {
		for _, t := range p.Registry.Tasks {
			if _, seen := res.Tasks[t.ID]; !seen {
				res.Tasks[t.ID] = t
			}
		}
	}

	held := map[int64]Task{}
	for _, p := range snaps {
		if p.Err != nil {
			continue
		}
		for _, t := range p.Tasks {
			t.AssignedNode = p.NodeID
			if prev, dup := held[t.ID]; dup {
				// Задачу держат два узла: верим тому, за кем она числится в реестре.
				if res.Tasks[t.ID].AssignedNode != t.AssignedNode {
					t = prev
				}
			}
			held[t.ID] = t
		}
	}
	for id, t := range held {
		res.Tasks[id] = t
		res.Held[id] = true
	}
	return res
}

// OnBecomeLeader синхронизирует состояние очереди при переходе в LEADER: опрашивает
// все узлы, восстанавливает полный реестр, переназначает задачи недоступных узлов.
func (s *Service) OnBecomeLeader(term uint64) {
	start := time.Now()
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()

	s.mu.Lock()
	snaps := []peerSnapshot{{
		NodeID:   s.cfg.NodeID,
		Tasks:    s.ownLocked(),
		Registry: RegistryDelta{Epoch: s.reg.epoch, Version: s.reg.version, Tasks: s.reg.all()},
	}}
	known := snaps[0].Registry
	s.mu.Unlock()
	snaps = append(snaps, s.collect(&known)...)

	if isLeader, cur := s.cons.IsLeader(); !isLeader || cur != term {
		return
	}
	m := mergeSnapshots(snaps)

	s.mu.Lock()
	ids := make([]int64, 0, len(m.Tasks))
	for id := range m.Tasks {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	s.reg = newRegistry(term)
	for _, id := range ids {
		s.reg.put(m.Tasks[id])
	}
	s.nextID = 1
	s.rrNext = 0
	if n := len(ids); n > 0 {
		last := m.Tasks[ids[n-1]]
		s.nextID = last.ID + 1
		s.rrNext = (indexOf(s.order, last.AssignedNode) + 1) % len(s.order)
	}
	now := time.Now()
	var responded []int
	for _, id := range s.order {
		if id == s.cfg.NodeID {
			continue
		}
		s.alive[id] = m.Responded[id]
		if m.Responded[id] {
			s.lastOK[id] = now
			responded = append(responded, id)
		}
	}
	s.mu.Unlock()

	moved := s.reassignLocked(term, func(t Task) bool {
		return !m.Responded[t.AssignedNode] || !m.Held[t.ID]
	})

	s.mu.Lock()
	if isLeader, cur := s.cons.IsLeader(); isLeader && cur == term {
		s.readyTerm = term
	}
	total, nextID := len(s.reg.tasks), s.nextID
	s.mu.Unlock()
	s.log.Info("leader sync completed", "term", term, "tasks", total, "responded", responded,
		"reassigned", moved, "next_id", nextID, "took", time.Since(start))
}

// collect параллельно запрашивает у остальных узлов их задачи и реплики реестра;
// реплики, которые не новее known, не передаются.
func (s *Service) collect(known *RegistryDelta) []peerSnapshot {
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		out []peerSnapshot
	)
	for _, id := range s.order {
		if id == s.cfg.NodeID {
			continue
		}
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(s.ctx, s.cfg.SyncTimeout)
			defer cancel()
			addr := s.cfg.Peers[id]
			p := peerSnapshot{NodeID: id}
			var nt NodeTasks
			if nt, p.Err = s.peer.nodeTasks(ctx, addr); p.Err == nil {
				p.Tasks = nt.Tasks
				p.Registry, p.Err = s.peer.registry(ctx, addr, 0, known)
			}
			if p.Err != nil {
				s.log.Warn("sync: node unavailable", "peer", id, "err", p.Err)
			}
			mu.Lock()
			out = append(out, p)
			mu.Unlock()
		})
	}
	wg.Wait()
	return out
}

func indexOf(xs []int, v int) int {
	for i, x := range xs {
		if x == v {
			return i
		}
	}
	return -1
}
