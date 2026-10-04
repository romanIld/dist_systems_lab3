package raft

import (
	"context"
	"net/http"

	"lab3/internal/httpx"
)

// Transport доставляет RPC до других узлов.
type Transport interface {
	RequestVote(ctx context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(ctx context.Context, addr string, req AppendEntriesRequest) (AppendEntriesResponse, error)
}

// HTTPTransport - RPC как POST с JSON-телом.
type HTTPTransport struct {
	Client *http.Client
}

// NewHTTPTransport создает транспорт с общим пулом соединений.
func NewHTTPTransport() *HTTPTransport {
	return &HTTPTransport{Client: httpx.NewClient()}
}

func (t *HTTPTransport) RequestVote(ctx context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error) {
	var resp RequestVoteResponse
	err := httpx.PostJSON(ctx, t.Client, addr+PathRequestVote, req, &resp)
	return resp, err
}

func (t *HTTPTransport) AppendEntries(ctx context.Context, addr string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	var resp AppendEntriesResponse
	err := httpx.PostJSON(ctx, t.Client, addr+PathAppendEntries, req, &resp)
	return resp, err
}
