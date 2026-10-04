// Package queue реализует распределенную очередь задач поверх выбора лидера.
package queue

import "time"

// Status - состояние выполнения задачи.
type Status string

const (
	Pending    Status = "PENDING"
	Processing Status = "PROCESSING"
	Completed  Status = "COMPLETED"
)

// rank задает порядок статусов: статус в реестре не откатывается назад.
func (s Status) rank() int {
	switch s {
	case Processing:
		return 1
	case Completed:
		return 2
	default:
		return 0
	}
}

// Task - задача очереди.
type Task struct {
	ID           int64     `json:"id"`
	Data         string    `json:"data"`
	Priority     string    `json:"priority,omitempty"`
	Timestamp    time.Time `json:"timestamp"`
	Status       Status    `json:"status"`
	AssignedNode int       `json:"assigned_node"`
}
