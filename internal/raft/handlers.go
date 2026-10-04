package raft

import (
	"fmt"
	"net/http"

	"lab3/internal/httpx"
)

// Пути RPC.
const (
	PathRequestVote   = "/raft/request-vote"
	PathAppendEntries = "/raft/append-entries"
)

// HandleRequestVote обрабатывает запрос голоса. Проверки актуальности лога нет (упрощение методички).
func (n *Node) HandleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.currentTerm {
		n.log.Debug("vote denied: stale term", "candidate", req.CandidateID, "term", req.Term, "current", n.currentTerm)
		return RequestVoteResponse{Term: n.currentTerm, VoteGranted: false, VoterID: n.cfg.ID}
	}
	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term, fmt.Sprintf("RequestVote from node %d with higher term %d", req.CandidateID, req.Term))
	}
	granted := n.votedFor == 0 || n.votedFor == req.CandidateID
	if granted {
		n.votedFor = req.CandidateID
		n.resetElectionTimerLocked()
	}
	n.log.Info("vote", "candidate", req.CandidateID, "term", n.currentTerm, "granted", granted, "voted_for", n.votedFor)
	return RequestVoteResponse{Term: n.currentTerm, VoteGranted: granted, VoterID: n.cfg.ID}
}

// HandleAppendEntries обрабатывает heartbeat лидера и обновляет lastHeartbeat.
func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	if req.Term < n.currentTerm {
		resp := AppendEntriesResponse{Term: n.currentTerm, Success: false, NodeID: n.cfg.ID}
		n.mu.Unlock()
		return resp
	}
	if req.Term > n.currentTerm || n.role != Follower {
		n.becomeFollowerLocked(req.Term, fmt.Sprintf("heartbeat from leader %d with term %d", req.LeaderID, req.Term))
	}
	if n.leaderID != req.LeaderID {
		n.log.Info("leader discovered", "leader", req.LeaderID, "term", req.Term)
	}
	n.leaderID, n.leaderAddr = req.LeaderID, req.LeaderAddr
	n.resetElectionTimerLocked()
	resp := AppendEntriesResponse{Term: n.currentTerm, Success: true, NodeID: n.cfg.ID}
	n.mu.Unlock()
	if h := n.hooks.OnHeartbeat; h != nil {
		h(req)
	}
	return resp
}

// Register подключает RPC-обработчики к mux.
func (n *Node) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST "+PathRequestVote, func(w http.ResponseWriter, r *http.Request) {
		var req RequestVoteRequest
		if err := httpx.ReadJSON(r, &req); err != nil {
			httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		httpx.WriteJSON(w, http.StatusOK, n.HandleRequestVote(req))
	})
	mux.HandleFunc("POST "+PathAppendEntries, func(w http.ResponseWriter, r *http.Request) {
		var req AppendEntriesRequest
		if err := httpx.ReadJSON(r, &req); err != nil {
			httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		httpx.WriteJSON(w, http.StatusOK, n.HandleAppendEntries(req))
	})
}
