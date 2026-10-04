package raft

import (
	"sync"
	"time"
)

// Transition - запись журнала переходов между состояниями.
type Transition struct {
	Time   time.Time `json:"time"`
	From   Role      `json:"from"`
	To     Role      `json:"to"`
	Term   uint64    `json:"term"`
	Reason string    `json:"reason"`
}

type transitionLog struct {
	mu    sync.Mutex
	items []Transition
}

func (l *transitionLog) add(t Transition) {
	l.mu.Lock()
	l.items = append(l.items, t)
	l.mu.Unlock()
}

func (l *transitionLog) list() []Transition {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Transition(nil), l.items...)
}
