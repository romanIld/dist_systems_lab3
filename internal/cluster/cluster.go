// Package cluster поднимает кластер узлов в одном процессе на реальных сокетах 127.0.0.1.
// Используется в интеграционных тестах: падение узла - остановка его сервера и горутин.
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"lab3/internal/api"
	"lab3/internal/httpx"
	"lab3/internal/node"
	"lab3/internal/queue"
	"lab3/internal/raft"
)

// Options - параметры тестового кластера.
type Options struct {
	Size     int
	WorkTime time.Duration
	Seed     uint64
	Logger   *slog.Logger
}

// Cluster - набор узлов в одном процессе.
type Cluster struct {
	opts   Options
	peers  map[int]string
	listen map[int]string
	client *http.Client

	mu    sync.Mutex
	nodes map[int]*node.Node
}

// Start выделяет свободные порты и запускает все узлы.
func Start(opts Options) (*Cluster, error) {
	if opts.Size < 1 {
		return nil, errors.New("cluster size must be positive")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	ports, err := freePorts(opts.Size)
	if err != nil {
		return nil, err
	}
	c := &Cluster{
		opts:   opts,
		peers:  map[int]string{},
		listen: map[int]string{},
		client: httpx.NewClient(),
		nodes:  map[int]*node.Node{},
	}
	for i, p := range ports {
		id := i + 1
		c.listen[id] = fmt.Sprintf("127.0.0.1:%d", p)
		c.peers[id] = "http://" + c.listen[id]
	}
	for id := 1; id <= opts.Size; id++ {
		if err := c.startNode(id); err != nil {
			c.Stop()
			return nil, err
		}
	}
	return c, nil
}

func freePorts(n int) ([]int, error) {
	var lns []net.Listener
	defer func() {
		for _, ln := range lns {
			ln.Close()
		}
	}()
	ports := make([]int, 0, n)
	for range n {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		lns = append(lns, ln)
		ports = append(ports, ln.Addr().(*net.TCPAddr).Port)
	}
	return ports, nil
}

func (c *Cluster) startNode(id int) error {
	cfg := node.DefaultConfig()
	cfg.ID = id
	cfg.Peers = c.peers
	cfg.Listen = c.listen[id]
	cfg.Raft.Seed = c.opts.Seed
	cfg.Queue.WorkTime = c.opts.WorkTime
	cfg.Logger = c.opts.Logger
	n, err := node.New(cfg)
	if err != nil {
		return err
	}
	if err := n.Start(); err != nil {
		return fmt.Errorf("start node %d: %w", id, err)
	}
	c.mu.Lock()
	c.nodes[id] = n
	c.mu.Unlock()
	return nil
}

// Kill останавливает узел (имитация падения процесса).
func (c *Cluster) Kill(id int) {
	c.mu.Lock()
	n := c.nodes[id]
	delete(c.nodes, id)
	c.mu.Unlock()
	if n != nil {
		n.Stop()
	}
}

// Restart запускает ранее остановленный узел с пустой памятью на том же адресе.
func (c *Cluster) Restart(id int) error { return c.startNode(id) }

// Stop останавливает все узлы.
func (c *Cluster) Stop() {
	for _, id := range c.Alive() {
		c.Kill(id)
	}
}

// Alive возвращает ID работающих узлов по возрастанию.
func (c *Cluster) Alive() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := make([]int, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

// Addr возвращает базовый URL узла.
func (c *Cluster) Addr(id int) string { return c.peers[id] }

// Transitions возвращает журнал переходов узла.
func (c *Cluster) Transitions(id int) ([]raft.Transition, error) {
	var out []raft.Transition
	err := c.get(id, "/internal/transitions", &out)
	return out, err
}

// Status запрашивает GET /status у узла.
func (c *Cluster) Status(id int) (api.StatusResponse, error) {
	var st api.StatusResponse
	err := c.get(id, "/status", &st)
	return st, err
}

// Tasks запрашивает GET /tasks/{of} у узла id.
func (c *Cluster) Tasks(id, of int) (queue.NodeTasks, error) {
	var nt queue.NodeTasks
	err := c.get(id, fmt.Sprintf("/tasks/%d", of), &nt)
	return nt, err
}

// Registry возвращает полную реплику реестра узла.
func (c *Cluster) Registry(id int) (queue.RegistryDelta, error) {
	var d queue.RegistryDelta
	err := c.get(id, queue.PathRegistry+"?since=0", &d)
	return d, err
}

func (c *Cluster) get(id int, path string, out any) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return httpx.GetJSON(ctx, c.client, c.peers[id]+path, out)
}

// SubmitResult - ответ узла на POST /task (редирект не выполняется).
type SubmitResult struct {
	Code     int
	Location string
	OK       api.TaskResponse
	Err      api.ErrorResponse
}

// Submit отправляет POST /task конкретному узлу.
func (c *Cluster) Submit(id int, data string) (SubmitResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var res SubmitResult
	err := httpx.PostJSON(ctx, c.client, c.peers[id]+"/task", api.TaskRequest{Data: data}, &res.OK)
	var se *httpx.StatusError
	switch {
	case err == nil:
		res.Code = http.StatusCreated
	case errors.As(err, &se):
		res.Code = se.Code
		res.Err = parseError(se.Body)
		if res.Code == http.StatusTemporaryRedirect {
			res.Location = res.Err.LeaderAddress
		}
		err = nil
	}
	return res, err
}

// SubmitToLeader находит лидера и отправляет ему задачу.
func (c *Cluster) SubmitToLeader(data string) (api.TaskResponse, error) {
	leader, _, err := c.WaitLeader(2 * time.Second)
	if err != nil {
		return api.TaskResponse{}, err
	}
	res, err := c.Submit(leader, data)
	if err != nil {
		return api.TaskResponse{}, err
	}
	if res.Code != http.StatusCreated {
		return api.TaskResponse{}, fmt.Errorf("leader %d answered %d: %s", leader, res.Code, res.Err.Message)
	}
	return res.OK, nil
}

// WaitLeader ждет, пока среди живых узлов будет ровно один лидер с завершенной
// синхронизацией, а остальные признают его в том же терме. Возвращает лидера и время ожидания.
func (c *Cluster) WaitLeader(timeout time.Duration) (int, time.Duration, error) {
	start := time.Now()
	var last string
	for time.Since(start) < timeout {
		leader, desc := c.checkLeader()
		if leader != 0 {
			return leader, time.Since(start), nil
		}
		last = desc
		time.Sleep(5 * time.Millisecond)
	}
	return 0, time.Since(start), fmt.Errorf("no agreed leader within %v: %s", timeout, last)
}

func (c *Cluster) checkLeader() (int, string) {
	alive := c.Alive()
	sts := make(map[int]api.StatusResponse, len(alive))
	for _, id := range alive {
		st, err := c.Status(id)
		if err != nil {
			return 0, fmt.Sprintf("node %d: %v", id, err)
		}
		sts[id] = st
	}
	return AgreedLeader(sts)
}

// AgreedLeader возвращает лидера, если среди статусов ровно один LEADER с завершенной
// синхронизацией и все узлы признают его в том же терме; иначе 0 и описание причины.
func AgreedLeader(sts map[int]api.StatusResponse) (int, string) {
	var leaders []int
	for id, st := range sts {
		if st.Role == raft.Leader {
			leaders = append(leaders, id)
		}
	}
	if len(leaders) != 1 {
		return 0, fmt.Sprintf("leaders=%v", leaders)
	}
	l := sts[leaders[0]]
	if !l.Ready {
		return 0, fmt.Sprintf("leader %d not synchronized yet", l.NodeID)
	}
	for id, st := range sts {
		if st.LeaderID != l.NodeID || st.Term != l.Term {
			return 0, fmt.Sprintf("node %d sees leader %d term %d", id, st.LeaderID, st.Term)
		}
	}
	return l.NodeID, ""
}

// WaitConverged ждет, пока реплики реестра на всех живых узлах совпадут с лидерской.
func (c *Cluster) WaitConverged(timeout time.Duration) (queue.RegistryDelta, error) {
	start := time.Now()
	var last string
	for time.Since(start) < timeout {
		leader, desc := c.checkLeader()
		if leader == 0 {
			last = desc
			time.Sleep(10 * time.Millisecond)
			continue
		}
		ref, err := c.Registry(leader)
		if err != nil {
			return ref, err
		}
		same := true
		for _, id := range c.Alive() {
			d, err := c.Registry(id)
			if err != nil {
				return ref, err
			}
			if d.Epoch != ref.Epoch || d.Version != ref.Version || len(d.Tasks) != len(ref.Tasks) {
				same = false
				last = fmt.Sprintf("node %d at epoch %d version %d, leader at %d/%d", id, d.Epoch, d.Version, ref.Epoch, ref.Version)
				break
			}
		}
		if same {
			return ref, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return queue.RegistryDelta{}, fmt.Errorf("registries did not converge within %v: %s", timeout, last)
}

func parseError(body string) api.ErrorResponse {
	var e api.ErrorResponse
	if json.Unmarshal([]byte(body), &e) != nil {
		e.Message = body
	}
	return e
}
