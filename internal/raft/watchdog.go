package raft

import (
	"fmt"
	"time"
)

// watchdog - фоновая горутина: если heartbeat не приходил дольше election timeout, начинает выборы.
func (n *Node) watchdog() {
	t := time.NewTicker(n.cfg.WatchdogTick)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
		}
		n.mu.Lock()
		silence := time.Since(n.lastHeartbeat)
		if n.role != Leader && silence >= n.electionTimeout {
			n.startElectionLocked(silence)
		}
		n.mu.Unlock()
	}
}

func (n *Node) startElectionLocked(silence time.Duration) {
	var reason string
	if n.role == Candidate {
		reason = fmt.Sprintf("election timeout %.0f ms expired without quorum, new round", ms(n.electionTimeout))
	} else {
		reason = fmt.Sprintf("no heartbeat for %.0f ms (election timeout %.0f ms)", ms(silence), ms(n.electionTimeout))
	}
	n.currentTerm++
	n.votedFor = n.cfg.ID
	n.votesReceived = map[int]bool{n.cfg.ID: true}
	n.leaderID, n.leaderAddr = 0, ""
	n.setRoleLocked(Candidate, reason)
	n.resetElectionTimerLocked()
	if len(n.votesReceived) >= n.quorum() {
		n.becomeLeaderLocked("single-node cluster")
		return
	}
	term := n.currentTerm
	for _, peer := range n.peers {
		n.goTracked(func() { n.requestVote(term, peer) })
	}
}
