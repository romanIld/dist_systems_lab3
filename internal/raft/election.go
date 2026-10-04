package raft

import (
	"context"
	"fmt"
	"time"
)

// requestVote отправляет RequestVote одному пиру и учитывает ответ.
func (n *Node) requestVote(term uint64, peer int) {
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
	defer cancel()
	start := time.Now()
	resp, err := n.cfg.Transport.RequestVote(ctx, n.cfg.Peers[peer], RequestVoteRequest{Term: term, CandidateID: n.cfg.ID})
	if err != nil {
		n.log.Debug("request vote failed", "peer", peer, "term", term, "took", time.Since(start), "err", err)
		return
	}
	n.log.Debug("request vote answered", "peer", peer, "term", term, "granted", resp.VoteGranted, "took", time.Since(start))
	n.handleVoteResponse(term, peer, resp)
}

func (n *Node) handleVoteResponse(term uint64, peer int, resp RequestVoteResponse) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.currentTerm {
		n.becomeFollowerLocked(resp.Term, fmt.Sprintf("node %d has higher term %d in RequestVote response", peer, resp.Term))
		return
	}
	if n.role != Candidate || n.currentTerm != term || !resp.VoteGranted {
		return
	}
	n.votesReceived[peer] = true
	n.log.Info("vote received", "from", peer, "term", term, "votes", len(n.votesReceived), "quorum", n.quorum())
	if len(n.votesReceived) >= n.quorum() {
		n.becomeLeaderLocked(fmt.Sprintf("received %d of %d votes (quorum %d)", len(n.votesReceived), len(n.cfg.Peers), n.quorum()))
	}
}
