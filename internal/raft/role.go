// Package raft реализует упрощенный Raft: только выбор лидера и heartbeat.
package raft

import "fmt"

// Role - состояние узла.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	default:
		return fmt.Sprintf("Role(%d)", int(r))
	}
}

// MarshalText сериализует роль в JSON как строку.
func (r Role) MarshalText() ([]byte, error) { return []byte(r.String()), nil }

// UnmarshalText разбирает роль из строки.
func (r *Role) UnmarshalText(b []byte) error {
	switch string(b) {
	case "FOLLOWER":
		*r = Follower
	case "CANDIDATE":
		*r = Candidate
	case "LEADER":
		*r = Leader
	default:
		return fmt.Errorf("unknown role %q", b)
	}
	return nil
}
