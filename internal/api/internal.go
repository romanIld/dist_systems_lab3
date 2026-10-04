package api

import (
	"errors"
	"net/http"
	"strconv"

	"lab3/internal/httpx"
	"lab3/internal/queue"
)

func (h *Handler) postInternalTask(w http.ResponseWriter, r *http.Request) {
	var req queue.InternalTaskRequest
	if err := httpx.ReadJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if err := h.queue.Accept(req); err != nil {
		httpx.WriteJSON(w, http.StatusConflict, queue.InternalTaskResponse{Status: "rejected: " + err.Error(), NodeID: h.raft.ID()})
		return
	}
	httpx.WriteJSON(w, http.StatusOK, queue.InternalTaskResponse{Status: "accepted", NodeID: h.raft.ID()})
}

func (h *Handler) getInternalTasks(w http.ResponseWriter, _ *http.Request) {
	ts := h.queue.OwnTasks()
	httpx.WriteJSON(w, http.StatusOK, queue.NodeTasks{NodeID: h.raft.ID(), Tasks: ts, Count: len(ts)})
}

func (h *Handler) getRegistry(w http.ResponseWriter, r *http.Request) {
	var since uint64
	if v := r.URL.Query().Get("since"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "since must be a non-negative integer")
			return
		}
		since = n
	}
	var known *queue.RegistryDelta
	if e, v := r.URL.Query().Get("epoch"), r.URL.Query().Get("version"); e != "" && v != "" {
		epoch, err1 := strconv.ParseUint(e, 10, 64)
		version, err2 := strconv.ParseUint(v, 10, 64)
		if err1 != nil || err2 != nil {
			writeError(w, http.StatusBadRequest, "epoch and version must be non-negative integers")
			return
		}
		known = &queue.RegistryDelta{Epoch: epoch, Version: version}
	}
	httpx.WriteJSON(w, http.StatusOK, h.queue.Registry(since, known))
}

func (h *Handler) postTaskStatus(w http.ResponseWriter, r *http.Request) {
	var rep queue.StatusReport
	if err := httpx.ReadJSON(r, &rep); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	res, err := h.queue.ApplyStatusReport(rep)
	if errors.Is(err, queue.ErrNotLeader) {
		h.notLeader(w)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, res)
}

func (h *Handler) getTransitions(w http.ResponseWriter, _ *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, h.raft.Transitions())
}
