package queue

import "time"

// InternalTaskRequest - POST /internal/task: лидер передает задачу узлу.
type InternalTaskRequest struct {
	Term         uint64    `json:"term"`
	LeaderID     int       `json:"leader_id"`
	TaskID       int64     `json:"task_id"`
	Data         string    `json:"data"`
	Priority     string    `json:"priority,omitempty"`
	Timestamp    time.Time `json:"timestamp"`
	Status       Status    `json:"status"`
	AssignedNode int       `json:"assigned_node"`
}

func (r InternalTaskRequest) task() Task {
	return Task{ID: r.TaskID, Data: r.Data, Priority: r.Priority, Timestamp: r.Timestamp,
		Status: r.Status, AssignedNode: r.AssignedNode}
}

// InternalTaskResponse - подтверждение получения задачи.
type InternalTaskResponse struct {
	Status string `json:"status"`
	NodeID int    `json:"node_id"`
}

// NodeTasks - ответ GET /internal/tasks и GET /tasks/{node_id}.
type NodeTasks struct {
	NodeID int    `json:"node_id"`
	Tasks  []Task `json:"tasks"`
	Count  int    `json:"count"`
	Source string `json:"source,omitempty"`
}

// RegistryDelta - ответ GET /internal/registry?since=N.
// Unchanged: реплика не новее той, что указал запрашивающий (задачи не передаются).
type RegistryDelta struct {
	Epoch     uint64 `json:"epoch"`
	Version   uint64 `json:"version"`
	Full      bool   `json:"full"`
	Unchanged bool   `json:"unchanged,omitempty"`
	Tasks     []Task `json:"tasks"`
}

// StatusUpdate - смена статуса задачи на узле-владельце.
type StatusUpdate struct {
	TaskID int64  `json:"task_id"`
	Status Status `json:"status"`
}

// StatusReport - POST /internal/task-status: узел сообщает лидеру статусы своих задач.
type StatusReport struct {
	NodeID  int            `json:"node_id"`
	Updates []StatusUpdate `json:"updates"`
}

// StatusReportResult - сколько обновлений применено и какие задачи лидеру пока неизвестны.
type StatusReportResult struct {
	Applied int     `json:"applied"`
	Unknown []int64 `json:"unknown"`
}

// Info - состояние очереди узла для GET /status.
type Info struct {
	Ready           bool   `json:"queue_ready"`
	OwnTasks        int    `json:"own_tasks"`
	KnownTasks      int    `json:"known_tasks"`
	RegistryEpoch   uint64 `json:"registry_epoch"`
	RegistryVersion uint64 `json:"registry_version"`
	AliveNodes      []int  `json:"alive_nodes,omitempty"`
}
