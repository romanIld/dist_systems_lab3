package raft

import "time"

// RequestVoteRequest - запрос голоса от кандидата.
type RequestVoteRequest struct {
	Term        uint64 `json:"term"`
	CandidateID int    `json:"candidate_id"`
}

// RequestVoteResponse - ответ на запрос голоса.
type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
	VoterID     int    `json:"voter_id"`
}

// AppendEntriesRequest - heartbeat лидера. Entries всегда пуст (лог не реплицируется);
// RegistryEpoch/RegistryVersion сообщают фолловерам версию реестра задач лидера.
type AppendEntriesRequest struct {
	Term            uint64    `json:"term"`
	LeaderID        int       `json:"leader_id"`
	LeaderAddr      string    `json:"leader_addr"`
	Entries         []string  `json:"entries"`
	RegistryEpoch   uint64    `json:"registry_epoch"`
	RegistryVersion uint64    `json:"registry_version"`
	SentAt          time.Time `json:"sent_at"`
}

// AppendEntriesResponse - ответ на heartbeat.
type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
	NodeID  int    `json:"node_id"`
}
