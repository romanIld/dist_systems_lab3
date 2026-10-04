// Package node собирает узел кластера: выборы, очередь задач и HTTP-сервер.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"lab3/internal/api"
	"lab3/internal/httpx"
	"lab3/internal/queue"
	"lab3/internal/raft"
)

// Config - параметры узла.
type Config struct {
	ID     int
	Peers  map[int]string // адреса всех узлов, включая свой
	Listen string         // адрес прослушивания; по умолчанию порт из своего адреса
	Raft   raft.Config    // таймеры; ID, Peers, Transport, Logger заполняются автоматически
	Queue  queue.Config   // параметры очереди; NodeID, Peers, Logger заполняются автоматически
	Logger *slog.Logger
}

// DefaultConfig возвращает таймеры из методички.
func DefaultConfig() Config {
	return Config{Raft: raft.DefaultConfig(), Queue: queue.DefaultConfig()}
}

// Node - запущенный узел.
type Node struct {
	cfg    Config
	Raft   *raft.Node
	Queue  *queue.Service
	srv    *http.Server
	ln     net.Listener
	cancel context.CancelFunc
	done   chan struct{}
}

// New создает узел, не запуская его.
func New(cfg Config) (*Node, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	self, ok := cfg.Peers[cfg.ID]
	if !ok {
		return nil, fmt.Errorf("node %d is not listed in peers", cfg.ID)
	}
	if cfg.Listen == "" {
		u, err := url.Parse(self)
		if err != nil || u.Port() == "" {
			return nil, fmt.Errorf("cannot derive listen address from %q", self)
		}
		cfg.Listen = ":" + u.Port()
	}
	client := httpx.NewClient()

	rc := cfg.Raft
	rc.ID, rc.Peers, rc.Logger = cfg.ID, cfg.Peers, cfg.Logger
	rc.Transport = &raft.HTTPTransport{Client: client}
	r, err := raft.New(rc)
	if err != nil {
		return nil, err
	}
	qc := cfg.Queue
	qc.NodeID, qc.Peers, qc.Logger, qc.Client = cfg.ID, cfg.Peers, cfg.Logger, client
	q, err := queue.New(qc, r)
	if err != nil {
		return nil, err
	}
	r.SetHooks(raft.Hooks{
		OnBecomeLeader: q.OnBecomeLeader,
		OnHeartbeat: func(hb raft.AppendEntriesRequest) {
			q.OnHeartbeat(hb.LeaderAddr, hb.RegistryEpoch, hb.RegistryVersion)
		},
		OnPeerResult:  q.OnPeerResult,
		HeartbeatInfo: q.HeartbeatInfo,
	})
	n := &Node{cfg: cfg, Raft: r, Queue: q}
	n.srv = &http.Server{
		Handler:           api.New(r, q, cfg.Peers, cfg.Logger.With("node", cfg.ID)),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return n, nil
}

// Start открывает порт и запускает фоновые процессы.
func (n *Node) Start() error {
	ln, err := net.Listen("tcp", n.cfg.Listen)
	if err != nil {
		return err
	}
	n.ln = ln
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	n.done = make(chan struct{})
	go func() {
		defer close(n.done)
		if err := n.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			n.cfg.Logger.Error("http server failed", "node", n.cfg.ID, "err", err)
		}
	}()
	n.Queue.Start(ctx)
	n.Raft.Start(ctx)
	return nil
}

// Stop имитирует падение: закрывает сервер и соединения, останавливает горутины.
func (n *Node) Stop() {
	_ = n.srv.Close()
	if n.cancel != nil {
		n.cancel()
	}
	n.Raft.Stop()
	n.Queue.Stop()
	if n.done != nil {
		<-n.done
	}
}

// Shutdown корректно завершает узел, дожидаясь текущих запросов.
func (n *Node) Shutdown(ctx context.Context) error {
	err := n.srv.Shutdown(ctx)
	n.Stop()
	return err
}

// ParsePeers разбирает строку вида "1=http://127.0.0.1:8081,2=http://127.0.0.1:8082".
func ParsePeers(s string) (map[int]string, error) {
	peers := map[int]string{}
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idStr, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("peer %q: expected id=url", part)
		}
		id, err := strconv.Atoi(strings.TrimSpace(idStr))
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("peer %q: id must be a positive integer", part)
		}
		addr = strings.TrimRight(strings.TrimSpace(addr), "/")
		if u, err := url.Parse(addr); err != nil || u.Scheme == "" || u.Host == "" {
			return nil, fmt.Errorf("peer %q: invalid url", part)
		}
		if _, dup := peers[id]; dup {
			return nil, fmt.Errorf("peer id %d is duplicated", id)
		}
		peers[id] = addr
	}
	if len(peers) == 0 {
		return nil, errors.New("peer list is empty")
	}
	return peers, nil
}

// FormatPeers - обратная к ParsePeers операция.
func FormatPeers(peers map[int]string) string {
	ids := make([]int, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%d=%s", id, peers[id])
	}
	return strings.Join(parts, ",")
}
