package raft

import (
	"context"
	"fmt"
	"time"
)

// leaderLoop рассылает heartbeat каждые HeartbeatInterval, пока узел - лидер этого терма.
func (n *Node) leaderLoop(term uint64) {
	t := time.NewTicker(n.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		if !n.checkQuorum(term) {
			return
		}
		n.broadcastHeartbeat(term)
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
		}
	}
}

// checkQuorum проверяет, что узел все еще лидер терма и большинство узлов отвечало на
// heartbeat за последние ElectionTimeoutMax; иначе лидер отступает (защита от двух лидеров
// при разделении сети).
func (n *Node) checkQuorum(term uint64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader || n.currentTerm != term {
		return false
	}
	acked := 1
	for _, t := range n.peerAck {
		if time.Since(t) <= n.cfg.ElectionTimeoutMax {
			acked++
		}
	}
	if acked >= n.quorum() {
		n.lastHeartbeat = time.Now()
		return true
	}
	n.setRoleLocked(Follower, fmt.Sprintf("lost quorum: %d of %d nodes answered heartbeats within %.0f ms",
		acked, len(n.cfg.Peers), ms(n.cfg.ElectionTimeoutMax)))
	n.leaderID, n.leaderAddr = 0, ""
	n.resetElectionTimerLocked()
	return false
}

func (n *Node) broadcastHeartbeat(term uint64) {
	req := AppendEntriesRequest{
		Term:       term,
		LeaderID:   n.cfg.ID,
		LeaderAddr: n.cfg.Peers[n.cfg.ID],
		Entries:    []string{},
		SentAt:     time.Now().UTC(),
	}
	if h := n.hooks.HeartbeatInfo; h != nil {
		req.RegistryEpoch, req.RegistryVersion = h()
	}
	for _, peer := range n.peers {
		n.goTracked(func() { n.sendHeartbeat(peer, req) })
	}
}

// sendHeartbeat отправляет пустой AppendEntries; медленный пир не задерживает остальных.
func (n *Node) sendHeartbeat(peer int, req AppendEntriesRequest) {
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
	defer cancel()
	resp, err := n.cfg.Transport.AppendEntries(ctx, n.cfg.Peers[peer], req)
	ok := err == nil && resp.Success
	if err == nil && resp.Term > req.Term {
		n.mu.Lock()
		n.becomeFollowerLocked(resp.Term, fmt.Sprintf("node %d has higher term %d in AppendEntries response", peer, resp.Term))
		n.mu.Unlock()
		return
	}
	if ok {
		n.mu.Lock()
		if n.role == Leader && n.currentTerm == req.Term {
			n.peerAck[peer] = time.Now()
		}
		n.mu.Unlock()
	}
	if h := n.hooks.OnPeerResult; h != nil {
		h(peer, ok)
	}
}
