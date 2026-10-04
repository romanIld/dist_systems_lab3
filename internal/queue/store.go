package queue

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"lab3/internal/bg"
	"lab3/internal/httpx"
)

// Ошибки очереди.
var (
	ErrNotLeader = errors.New("this node is not the leader")
	ErrNotReady  = errors.New("leader is synchronizing tasks, retry later")
	ErrNoNodes   = errors.New("no live node accepted the task")
	ErrStaleTerm = errors.New("request from a leader with stale term")
)

// Consensus - то, что очереди нужно знать о выборах.
type Consensus interface {
	Term() uint64
	IsLeader() (bool, uint64)
	Leader() (id int, addr string)
}

// Config - параметры очереди.
type Config struct {
	NodeID         int
	Peers          map[int]string // адреса всех узлов, включая свой
	WorkTime       time.Duration  // время "выполнения" одной задачи воркером
	DeadAfter      time.Duration  // узел считается упавшим без ответа на heartbeat дольше этого
	RPCTimeout     time.Duration  // таймаут доставки задачи и отчета о статусе
	SyncTimeout    time.Duration  // таймаут массовых передач: опрос узлов новым лидером, загрузка реестра
	ReportInterval time.Duration  // период повторной отправки статусов
	Client         *http.Client
	Logger         *slog.Logger
}

// DefaultConfig возвращает параметры по умолчанию.
func DefaultConfig() Config {
	return Config{
		WorkTime:       2 * time.Second,
		DeadAfter:      300 * time.Millisecond,
		RPCTimeout:     300 * time.Millisecond,
		SyncTimeout:    time.Second,
		ReportInterval: 50 * time.Millisecond,
	}
}

// Service - очередь задач одного узла.
type Service struct {
	cfg   Config
	log   *slog.Logger
	cons  Consensus
	peer  peerClient
	order []int // ID всех узлов по возрастанию, порядок round-robin

	mu      sync.Mutex
	own     map[int64]*Task // задачи, назначенные этому узлу
	jobs    []int64         // очередь воркера в порядке поступления
	dirty   map[int64]bool  // статусы, еще не подтвержденные лидером
	reg     *registry
	pulling bool

	// состояние лидера
	readyTerm uint64 // терм, в котором завершена синхронизация
	nextID    int64
	rrNext    int
	alive     map[int]bool
	lastOK    map[int]time.Time

	dispatchMu sync.Mutex // последовательная раздача задач лидером

	wake  chan struct{}
	flush chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	bg     bg.Group
}

// New создает очередь узла.
func New(cfg Config, cons Consensus) (*Service, error) {
	if _, ok := cfg.Peers[cfg.NodeID]; !ok {
		return nil, errors.New("queue: peers must contain own id")
	}
	def := DefaultConfig()
	if cfg.DeadAfter <= 0 {
		cfg.DeadAfter = def.DeadAfter
	}
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = def.RPCTimeout
	}
	if cfg.SyncTimeout <= 0 {
		cfg.SyncTimeout = def.SyncTimeout
	}
	if cfg.ReportInterval <= 0 {
		cfg.ReportInterval = def.ReportInterval
	}
	if cfg.Client == nil {
		cfg.Client = httpx.NewClient()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	s := &Service{
		cfg:    cfg,
		log:    cfg.Logger.With("node", cfg.NodeID),
		cons:   cons,
		peer:   peerClient{http: cfg.Client},
		own:    map[int64]*Task{},
		dirty:  map[int64]bool{},
		reg:    newRegistry(0),
		alive:  map[int]bool{},
		lastOK: map[int]time.Time{},
		wake:   make(chan struct{}, 1),
		flush:  make(chan struct{}, 1),
	}
	for id := range cfg.Peers {
		s.order = append(s.order, id)
	}
	sort.Ints(s.order)
	return s, nil
}

// Start запускает воркер, отправку статусов и контроль живости узлов.
func (s *Service) Start(ctx context.Context) {
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.goTracked(s.worker)
	s.goTracked(s.reporter)
	s.goTracked(s.livenessLoop)
}

// Stop останавливает фоновые горутины.
func (s *Service) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.bg.Stop()
}

func (s *Service) goTracked(f func()) { s.bg.Go(f) }

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// Accept принимает задачу от лидера (POST /internal/task).
func (s *Service) Accept(req InternalTaskRequest) error {
	if term := s.cons.Term(); req.Term < term {
		return ErrStaleTerm
	}
	s.acceptTask(req.task())
	return nil
}

func (s *Service) acceptTask(t Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.own[t.ID]; ok {
		return
	}
	t.AssignedNode = s.cfg.NodeID
	t.Status = Pending
	s.own[t.ID] = &t
	s.jobs = append(s.jobs, t.ID)
	signal(s.wake)
}

// OwnTasks возвращает задачи этого узла по возрастанию ID.
func (s *Service) OwnTasks() []Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ownLocked()
}

func (s *Service) ownLocked() []Task {
	out := make([]Task, 0, len(s.own))
	for _, t := range s.own {
		out = append(out, *t)
	}
	sortTasks(out)
	return out
}

// TasksOf возвращает задачи узла: свои - из памяти, чужие - из реплики реестра.
func (s *Service) TasksOf(nodeID int) NodeTasks {
	if nodeID == s.cfg.NodeID {
		ts := s.OwnTasks()
		return NodeTasks{NodeID: nodeID, Tasks: ts, Count: len(ts), Source: "local"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ts := make([]Task, 0)
	for _, t := range s.reg.all() {
		if t.AssignedNode == nodeID {
			ts = append(ts, t)
		}
	}
	return NodeTasks{NodeID: nodeID, Tasks: ts, Count: len(ts), Source: "registry"}
}

// Registry возвращает изменения реестра после версии since (since=0 - весь реестр).
// Если known задан и реплика той же эпохи не новее его, задачи не передаются.
func (s *Service) Registry(since uint64, known *RegistryDelta) RegistryDelta {
	s.mu.Lock()
	defer s.mu.Unlock()
	if known != nil && known.Epoch == s.reg.epoch && s.reg.version <= known.Version {
		return RegistryDelta{Epoch: s.reg.epoch, Version: s.reg.version, Full: true, Unchanged: true, Tasks: []Task{}}
	}
	full := since == 0 || since > s.reg.version
	if full {
		since = 0
	}
	return RegistryDelta{Epoch: s.reg.epoch, Version: s.reg.version, Full: full, Tasks: s.reg.since(since)}
}

// HeartbeatInfo - версия реестра для heartbeat лидера.
func (s *Service) HeartbeatInfo() (epoch, version uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reg.epoch, s.reg.version
}

// Info - состояние очереди для GET /status.
func (s *Service) Info() Info {
	s.mu.Lock()
	defer s.mu.Unlock()
	isLeader, term := s.cons.IsLeader()
	info := Info{
		Ready:           isLeader && s.readyTerm == term,
		OwnTasks:        len(s.own),
		KnownTasks:      len(s.reg.tasks),
		RegistryEpoch:   s.reg.epoch,
		RegistryVersion: s.reg.version,
	}
	if isLeader {
		for _, id := range s.order {
			if id == s.cfg.NodeID || s.alive[id] {
				info.AliveNodes = append(info.AliveNodes, id)
			}
		}
	}
	return info
}

// worker выполняет свои задачи по очереди: PENDING -> PROCESSING -> COMPLETED.
func (s *Service) worker() {
	for {
		id, ok := s.nextJob()
		if !ok {
			return
		}
		s.setOwnStatus(id, Processing)
		if s.cfg.WorkTime > 0 {
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(s.cfg.WorkTime):
			}
		}
		s.setOwnStatus(id, Completed)
	}
}

func (s *Service) nextJob() (int64, bool) {
	for {
		s.mu.Lock()
		for len(s.jobs) > 0 {
			id := s.jobs[0]
			s.jobs = s.jobs[1:]
			if t, ok := s.own[id]; ok && t.Status == Pending {
				s.mu.Unlock()
				return id, true
			}
		}
		s.mu.Unlock()
		select {
		case <-s.ctx.Done():
			return 0, false
		case <-s.wake:
		}
	}
}

func (s *Service) setOwnStatus(id int64, st Status) {
	s.mu.Lock()
	t, ok := s.own[id]
	if !ok || t.Status.rank() >= st.rank() {
		s.mu.Unlock()
		return
	}
	t.Status = st
	s.dirty[id] = true
	s.mu.Unlock()
	signal(s.flush)
}

// reporter отправляет лидеру накопленные смены статусов и повторяет неудачные.
func (s *Service) reporter() {
	t := time.NewTicker(s.cfg.ReportInterval)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		case <-s.flush:
		}
		s.flushReports()
	}
}

func (s *Service) flushReports() {
	s.mu.Lock()
	rep := StatusReport{NodeID: s.cfg.NodeID}
	for id := range s.dirty {
		if t, ok := s.own[id]; ok {
			rep.Updates = append(rep.Updates, StatusUpdate{TaskID: id, Status: t.Status})
		}
	}
	clear(s.dirty)
	s.mu.Unlock()
	if len(rep.Updates) == 0 {
		return
	}

	var res StatusReportResult
	var err error
	if isLeader, _ := s.cons.IsLeader(); isLeader {
		res, err = s.ApplyStatusReport(rep)
	} else if _, addr := s.cons.Leader(); addr == "" {
		err = errors.New("leader unknown")
	} else {
		ctx, cancel := context.WithTimeout(s.ctx, s.cfg.RPCTimeout)
		res, err = s.peer.reportStatus(ctx, addr, rep)
		cancel()
	}

	retry := res.Unknown
	if err != nil {
		retry = make([]int64, 0, len(rep.Updates))
		for _, u := range rep.Updates {
			retry = append(retry, u.TaskID)
		}
	}
	if len(retry) > 0 {
		s.mu.Lock()
		for _, id := range retry {
			if _, ok := s.own[id]; ok {
				s.dirty[id] = true
			}
		}
		s.mu.Unlock()
	}
}

// OnHeartbeat вызывается фолловером: если реестр лидера новее, догружает изменения.
func (s *Service) OnHeartbeat(leaderAddr string, epoch, version uint64) {
	s.mu.Lock()
	upToDate := epoch == s.reg.epoch && version <= s.reg.version
	if s.pulling || upToDate || leaderAddr == "" {
		s.mu.Unlock()
		return
	}
	since := s.reg.version
	if epoch != s.reg.epoch {
		since = 0
	}
	s.pulling = true
	s.mu.Unlock()
	s.goTracked(func() { s.pull(leaderAddr, since) })
}

func (s *Service) pull(addr string, since uint64) {
	defer func() {
		s.mu.Lock()
		s.pulling = false
		s.mu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(s.ctx, s.cfg.SyncTimeout)
	defer cancel()
	d, err := s.peer.registry(ctx, addr, since, nil)
	if err != nil {
		s.log.Debug("registry pull failed", "leader", addr, "err", err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if isLeader, _ := s.cons.IsLeader(); isLeader {
		return
	}
	if d.Full {
		s.reg = newRegistry(d.Epoch)
	} else if d.Epoch != s.reg.epoch {
		return
	}
	for _, t := range d.Tasks {
		s.reg.put(t)
	}
	s.reg.epoch, s.reg.version = d.Epoch, d.Version
	s.reconcileOwnLocked(d.Tasks)
}

// reconcileOwnLocked сверяет свои задачи с реестром лидера: забирает задачи, которые
// числятся за узлом (после перезапуска), и отдает переназначенные на другие узлы.
func (s *Service) reconcileOwnLocked(tasks []Task) {
	adopted, dropped := 0, 0
	for _, t := range tasks {
		_, held := s.own[t.ID]
		switch {
		case t.AssignedNode == s.cfg.NodeID && !held:
			c := t
			if c.Status != Completed {
				c.Status = Pending
				s.jobs = append(s.jobs, c.ID)
			}
			s.own[c.ID] = &c
			adopted++
		case t.AssignedNode != s.cfg.NodeID && held:
			delete(s.own, t.ID)
			delete(s.dirty, t.ID)
			dropped++
		}
	}
	if adopted > 0 {
		signal(s.wake)
	}
	if adopted+dropped > 0 {
		s.log.Info("own tasks reconciled with leader registry", "adopted", adopted, "dropped", dropped)
	}
}
