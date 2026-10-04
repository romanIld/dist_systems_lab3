package queue

import (
	"context"
	"fmt"
	"net/http"

	"lab3/internal/httpx"
)

// Пути внутреннего API.
const (
	PathInternalTask  = "/internal/task"
	PathInternalTasks = "/internal/tasks"
	PathRegistry      = "/internal/registry"
	PathTaskStatus    = "/internal/task-status"
)

// peerClient - вызовы внутреннего API других узлов.
type peerClient struct {
	http *http.Client
}

func (c peerClient) sendTask(ctx context.Context, addr string, req InternalTaskRequest) error {
	var resp InternalTaskResponse
	if err := httpx.PostJSON(ctx, c.http, addr+PathInternalTask, req, &resp); err != nil {
		return err
	}
	if resp.Status != "accepted" {
		return fmt.Errorf("task %d not accepted: %s", req.TaskID, resp.Status)
	}
	return nil
}

func (c peerClient) nodeTasks(ctx context.Context, addr string) (NodeTasks, error) {
	var resp NodeTasks
	err := httpx.GetJSON(ctx, c.http, addr+PathInternalTasks, &resp)
	return resp, err
}

// registry запрашивает реестр узла; known - своя реплика, которую не нужно передавать повторно.
func (c peerClient) registry(ctx context.Context, addr string, since uint64, known *RegistryDelta) (RegistryDelta, error) {
	url := fmt.Sprintf("%s%s?since=%d", addr, PathRegistry, since)
	if known != nil {
		url += fmt.Sprintf("&epoch=%d&version=%d", known.Epoch, known.Version)
	}
	var resp RegistryDelta
	err := httpx.GetJSON(ctx, c.http, url, &resp)
	return resp, err
}

func (c peerClient) reportStatus(ctx context.Context, addr string, rep StatusReport) (StatusReportResult, error) {
	var resp StatusReportResult
	err := httpx.PostJSON(ctx, c.http, addr+PathTaskStatus, rep, &resp)
	return resp, err
}
