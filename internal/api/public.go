// Package api - HTTP-интерфейс узла: публичные и внутренние эндпоинты.
package api

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"strconv"
	"strings"

	"lab3/internal/httpx"
	"lab3/internal/queue"
	"lab3/internal/raft"
)

// Handler обслуживает HTTP-запросы одного узла.
type Handler struct {
	raft  *raft.Node
	queue *queue.Service
	peers map[int]string
	log   *slog.Logger
}

// New создает обработчик и регистрирует все маршруты, включая RPC Raft.
func New(r *raft.Node, q *queue.Service, peers map[int]string, log *slog.Logger) http.Handler {
	h := &Handler{raft: r, queue: q, peers: peers, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /task", h.postTask)
	mux.HandleFunc("GET /tasks/{node_id}", h.getTasks)
	mux.HandleFunc("GET /status", h.getStatus)
	mux.HandleFunc("POST "+queue.PathInternalTask, h.postInternalTask)
	mux.HandleFunc("GET "+queue.PathInternalTasks, h.getInternalTasks)
	mux.HandleFunc("GET "+queue.PathRegistry, h.getRegistry)
	mux.HandleFunc("POST "+queue.PathTaskStatus, h.postTaskStatus)
	mux.HandleFunc("GET /internal/transitions", h.getTransitions)
	r.Register(mux)
	return mux
}

// TaskRequest - тело POST /task.
type TaskRequest struct {
	Data     string `json:"data"`
	Priority string `json:"priority,omitempty"`
}

// TaskResponse - успешный ответ POST /task.
type TaskResponse struct {
	Status       string `json:"status"`
	TaskID       int64  `json:"task_id"`
	AssignedNode int    `json:"assigned_node"`
	Message      string `json:"message"`
}

// ErrorResponse - ответ с ошибкой; для не-лидера содержит адрес лидера.
type ErrorResponse struct {
	Status        string `json:"status"`
	Message       string `json:"message"`
	Leader        string `json:"leader,omitempty"`
	LeaderAddress string `json:"leader_address,omitempty"`
}

// StatusResponse - GET /status: роль, терм и состояние очереди узла.
type StatusResponse struct {
	raft.Status
	queue.Info
	Address string  `json:"address"`
	HeapMB  float64 `json:"heap_mb"`
}

func (h *Handler) postTask(w http.ResponseWriter, r *http.Request) {
	var req TaskRequest
	if err := httpx.ReadJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if strings.TrimSpace(req.Data) == "" {
		writeError(w, http.StatusBadRequest, "field 'data' is required")
		return
	}
	t, err := h.queue.Submit(r.Context(), req.Data, req.Priority)
	switch {
	case errors.Is(err, queue.ErrNotLeader):
		h.notLeader(w)
	case errors.Is(err, queue.ErrNotReady), errors.Is(err, queue.ErrNoNodes):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case err != nil:
		writeError(w, http.StatusInternalServerError, err.Error())
	default:
		h.log.Info("task accepted", "task", t.ID, "assigned_node", t.AssignedNode)
		httpx.WriteJSON(w, http.StatusCreated, TaskResponse{
			Status:       "success",
			TaskID:       t.ID,
			AssignedNode: t.AssignedNode,
			Message:      fmt.Sprintf("Task assigned to node %d", t.AssignedNode),
		})
	}
}

// notLeader отвечает 307 с адресом лидера или 503, если лидер еще не известен.
func (h *Handler) notLeader(w http.ResponseWriter) {
	id, addr := h.raft.Leader()
	if id == 0 {
		writeError(w, http.StatusServiceUnavailable, "This node is not the leader; leader is unknown, election in progress")
		return
	}
	w.Header().Set("Location", addr+"/task")
	httpx.WriteJSON(w, http.StatusTemporaryRedirect, ErrorResponse{
		Status:        "error",
		Message:       "This node is not the leader",
		Leader:        fmt.Sprintf("node%d", id),
		LeaderAddress: addr,
	})
}

func (h *Handler) getTasks(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("node_id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "node_id must be an integer")
		return
	}
	if _, ok := h.peers[id]; !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("node %d is not in the cluster", id))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, h.queue.TasksOf(id))
}

func (h *Handler) getStatus(w http.ResponseWriter, _ *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	httpx.WriteJSON(w, http.StatusOK, StatusResponse{
		Status:  h.raft.Status(),
		Info:    h.queue.Info(),
		Address: h.peers[h.raft.ID()],
		HeapMB:  float64(ms.HeapAlloc) / (1 << 20),
	})
}

func writeError(w http.ResponseWriter, code int, msg string) {
	httpx.WriteJSON(w, code, ErrorResponse{Status: "error", Message: msg})
}
