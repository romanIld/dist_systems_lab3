package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"lab3/internal/bg"
)

// Hooks - обратные вызовы для слоя приложения. Вызываются без удержания блокировки узла.
type Hooks struct {
	// OnBecomeLeader вызывается в отдельной горутине при переходе в LEADER.
	OnBecomeLeader func(term uint64)
	// OnHeartbeat вызывается фолловером после принятия heartbeat.
	OnHeartbeat func(req AppendEntriesRequest)
	// OnPeerResult сообщает лидеру, ответил ли пир на heartbeat.
	OnPeerResult func(peerID int, ok bool)
	// HeartbeatInfo возвращает версию реестра задач для heartbeat.
	HeartbeatInfo func() (epoch, version uint64)
}

// Config - параметры узла.
type Config struct {
	ID                 int
	Peers              map[int]string // адреса всех узлов кластера, включая свой
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	RPCTimeout         time.Duration
	WatchdogTick       time.Duration
	Seed               uint64
	Transport          Transport
	Logger             *slog.Logger
}

// DefaultConfig возвращает значения из методички: heartbeat 50 мс, election timeout 150-300 мс.
func DefaultConfig() Config {
	return Config{
		HeartbeatInterval:  50 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		RPCTimeout:         50 * time.Millisecond,
		WatchdogTick:       5 * time.Millisecond,
	}
}

// Status - снимок состояния узла.
type Status struct {
	NodeID            int     `json:"node_id"`
	Role              Role    `json:"role"`
	Term              uint64  `json:"term"`
	VotedFor          *int    `json:"voted_for"`
	VotesReceived     []int   `json:"votes_received"`
	LeaderID          int     `json:"leader_id"`
	LeaderAddr        string  `json:"leader_address"`
	ClusterSize       int     `json:"cluster_size"`
	LastHeartbeatAgo  float64 `json:"last_heartbeat_ago_ms"`
	ElectionTimeoutMs float64 `json:"election_timeout_ms"`
}

// Node - участник выборов.
type Node struct {
	cfg   Config
	log   *slog.Logger
	hooks Hooks
	peers []int // ID остальных узлов

	mu              sync.Mutex
	role            Role
	currentTerm     uint64
	votedFor        int // 0 - не голосовал
	votesReceived   map[int]bool
	leaderID        int
	leaderAddr      string
	lastHeartbeat   time.Time // фолловер: последний принятый heartbeat; лидер: последний отправленный
	electionTimeout time.Duration
	peerAck         map[int]time.Time // лидер: последний успешный ответ на heartbeat
	rng             *rand.Rand

	transitions transitionLog

	ctx    context.Context
	cancel context.CancelFunc
	bg     bg.Group
}

// New проверяет конфигурацию и создает узел в состоянии FOLLOWER.
func New(cfg Config) (*Node, error) {
	if cfg.ID <= 0 {
		return nil, errors.New("raft: node id must be positive")
	}
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, fmt.Errorf("raft: peers must contain own id %d", cfg.ID)
	}
	if cfg.ElectionTimeoutMin <= 0 || cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		return nil, errors.New("raft: invalid election timeout range")
	}
	if cfg.HeartbeatInterval <= 0 || cfg.HeartbeatInterval >= cfg.ElectionTimeoutMin {
		return nil, errors.New("raft: heartbeat interval must be less than election timeout")
	}
	if cfg.Transport == nil {
		return nil, errors.New("raft: transport is required")
	}
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = cfg.HeartbeatInterval
	}
	if cfg.WatchdogTick <= 0 {
		cfg.WatchdogTick = 5 * time.Millisecond
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	n := &Node{
		cfg: cfg,
		log: cfg.Logger.With("node", cfg.ID),
		rng: rand.New(rand.NewPCG(cfg.Seed, uint64(cfg.ID))),
	}
	for id := range cfg.Peers {
		if id != cfg.ID {
			n.peers = append(n.peers, id)
		}
	}
	sort.Ints(n.peers)
	return n, nil
}

// SetHooks задает обратные вызовы; вызывать до Start.
func (n *Node) SetHooks(h Hooks) { n.hooks = h }

// ID возвращает идентификатор узла.
func (n *Node) ID() int { return n.cfg.ID }

// Start запускает watchdog. Узел стартует FOLLOWER с термом 0.
func (n *Node) Start(ctx context.Context) {
	n.ctx, n.cancel = context.WithCancel(ctx)
	n.mu.Lock()
	n.role = Follower
	n.lastHeartbeat = time.Now()
	n.resetElectionTimerLocked()
	n.transitions.add(Transition{Time: time.Now(), From: Follower, To: Follower, Term: 0, Reason: "node started"})
	n.mu.Unlock()
	n.log.Info("started", "role", Follower, "term", 0, "election_timeout", n.electionTimeout)
	n.goTracked(n.watchdog)
}

// Stop останавливает все фоновые горутины узла.
func (n *Node) Stop() {
	if n.cancel != nil {
		n.cancel()
	}
	n.bg.Stop()
}

func (n *Node) goTracked(f func()) { n.bg.Go(f) }

// Term возвращает текущий терм.
func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// IsLeader сообщает, лидер ли узел, и его терм.
func (n *Node) IsLeader() (bool, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader, n.currentTerm
}

// Leader возвращает известного лидера (0, "" если неизвестен).
func (n *Node) Leader() (int, string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID, n.leaderAddr
}

// Status возвращает снимок состояния.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := Status{
		NodeID:            n.cfg.ID,
		Role:              n.role,
		Term:              n.currentTerm,
		LeaderID:          n.leaderID,
		LeaderAddr:        n.leaderAddr,
		ClusterSize:       len(n.cfg.Peers),
		LastHeartbeatAgo:  ms(time.Since(n.lastHeartbeat)),
		ElectionTimeoutMs: ms(n.electionTimeout),
		VotesReceived:     []int{},
	}
	if n.votedFor != 0 {
		v := n.votedFor
		s.VotedFor = &v
	}
	for id := range n.votesReceived {
		s.VotesReceived = append(s.VotesReceived, id)
	}
	sort.Ints(s.VotesReceived)
	return s
}

// Transitions возвращает журнал переходов.
func (n *Node) Transitions() []Transition { return n.transitions.list() }

func (n *Node) quorum() int { return len(n.cfg.Peers)/2 + 1 }

// resetElectionTimerLocked сбрасывает таймер и выбирает новый случайный timeout.
func (n *Node) resetElectionTimerLocked() {
	lo, hi := n.cfg.ElectionTimeoutMin, n.cfg.ElectionTimeoutMax
	n.electionTimeout = lo + time.Duration(n.rng.Int64N(int64(hi-lo)))
	n.lastHeartbeat = time.Now()
}

func (n *Node) setRoleLocked(to Role, reason string) {
	from := n.role
	n.role = to
	n.transitions.add(Transition{Time: time.Now(), From: from, To: to, Term: n.currentTerm, Reason: reason})
	n.log.Info("transition", "from", from, "to", to, "term", n.currentTerm, "reason", reason)
}

// becomeFollowerLocked обновляет терм (если он больше) и переводит узел в FOLLOWER.
func (n *Node) becomeFollowerLocked(term uint64, reason string) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = 0
		n.leaderID, n.leaderAddr = 0, ""
	}
	n.votesReceived = nil
	if n.role != Follower {
		n.setRoleLocked(Follower, reason)
	}
}

func (n *Node) becomeLeaderLocked(reason string) {
	n.setRoleLocked(Leader, reason)
	n.leaderID = n.cfg.ID
	n.leaderAddr = n.cfg.Peers[n.cfg.ID]
	now := time.Now()
	n.peerAck = make(map[int]time.Time, len(n.peers))
	for _, p := range n.peers {
		n.peerAck[p] = now
	}
	term := n.currentTerm
	n.goTracked(func() { n.leaderLoop(term) })
	if h := n.hooks.OnBecomeLeader; h != nil {
		n.goTracked(func() { h(term) })
	}
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
