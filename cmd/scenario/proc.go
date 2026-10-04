package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"lab3/internal/api"
	"lab3/internal/cluster"
	"lab3/internal/httpx"
	"lab3/internal/node"
	"lab3/internal/queue"
	"lab3/internal/raft"
)

// proc - процесс узла.
type proc struct {
	cmd  *exec.Cmd
	done chan struct{}
}

// procCluster - кластер из отдельных процессов raftnode на 127.0.0.1.
type procCluster struct {
	bin      string
	logDir   string
	tag      string
	workTime time.Duration
	peers    map[int]string
	procs    map[int]*proc
	http     *http.Client
	leader   string // адрес последнего известного лидера
	seed     uint64
	logLevel string
}

func newProcCluster(bin, logDir, tag string, size, basePort int, workTime time.Duration) *procCluster {
	peers := map[int]string{}
	for i := 1; i <= size; i++ {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", basePort+i-1)
	}
	return &procCluster{
		bin: bin, logDir: logDir, tag: tag, workTime: workTime,
		peers: peers, procs: map[int]*proc{}, http: httpx.NewClient(),
		seed: uint64(time.Now().UnixNano()), logLevel: nodeLogLevel,
	}
}

func (c *procCluster) start(id int) error {
	f, err := os.OpenFile(filepath.Join(c.logDir, fmt.Sprintf("%s-node%d.log", c.tag, id)),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	c.seed++
	cmd := exec.Command(c.bin, "-id", strconv.Itoa(id), "-peers", node.FormatPeers(c.peers),
		"-work-time", c.workTime.String(), "-seed", strconv.FormatUint(c.seed, 10), "-log-level", c.logLevel)
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Start(); err != nil {
		f.Close()
		return err
	}
	p := &proc{cmd: cmd, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		f.Close()
		close(p.done)
	}()
	c.procs[id] = p
	return nil
}

func (c *procCluster) startAll() error {
	for id := 1; id <= len(c.peers); id++ {
		if err := c.start(id); err != nil {
			return err
		}
	}
	return nil
}

// kill завершает процесс узла без корректного выключения.
func (c *procCluster) kill(id int) {
	p := c.procs[id]
	if p == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	<-p.done
	delete(c.procs, id)
}

func (c *procCluster) stopAll() {
	for id := range c.procs {
		c.kill(id)
	}
}

func (c *procCluster) alive() []int {
	ids := make([]int, 0, len(c.procs))
	for id := range c.procs {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

func (c *procCluster) get(id int, path string, out any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return httpx.GetJSON(ctx, c.http, c.peers[id]+path, out)
}

func (c *procCluster) status(id int) (api.StatusResponse, error) {
	var st api.StatusResponse
	err := c.get(id, "/status", &st)
	return st, err
}

func (c *procCluster) transitions(id int) []raft.Transition {
	var out []raft.Transition
	_ = c.get(id, "/internal/transitions", &out)
	return out
}

func (c *procCluster) registry(id int) (queue.RegistryDelta, error) {
	var d queue.RegistryDelta
	err := c.get(id, queue.PathRegistry+"?since=0", &d)
	return d, err
}

func (c *procCluster) localTasks(id int) (queue.NodeTasks, error) {
	var nt queue.NodeTasks
	err := c.get(id, fmt.Sprintf("/tasks/%d", id), &nt)
	return nt, err
}

// waitLeader опрашивает узлы, пока не появится согласованный и готовый лидер.
// Возвращает лидера и момент, когда это было замечено.
func (c *procCluster) waitLeader(timeout time.Duration) (int, time.Time, error) {
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		sts := map[int]api.StatusResponse{}
		ok := true
		for _, id := range c.alive() {
			st, err := c.status(id)
			if err != nil {
				ok, last = false, err.Error()
				break
			}
			sts[id] = st
		}
		if ok {
			id, desc := cluster.AgreedLeader(sts)
			if id != 0 {
				c.leader = c.peers[id]
				return id, time.Now(), nil
			}
			last = desc
		}
		time.Sleep(2 * time.Millisecond)
	}
	return 0, time.Time{}, fmt.Errorf("no leader within %v: %s", timeout, last)
}

// submit отправляет задачу лидеру; при 307 переходит к новому адресу, при 503 повторяет.
func (c *procCluster) submit(data string) (api.TaskResponse, time.Duration, error) {
	var lastErr error
	for range 20 {
		if c.leader == "" {
			if _, _, err := c.waitLeader(2 * time.Second); err != nil {
				return api.TaskResponse{}, 0, err
			}
		}
		addr := c.leader
		var resp api.TaskResponse
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		start := time.Now()
		err := httpx.PostJSON(ctx, c.http, addr+"/task", api.TaskRequest{Data: data}, &resp)
		took := time.Since(start)
		cancel()
		if err == nil {
			return resp, took, nil
		}
		lastErr = err
		var se *httpx.StatusError
		if errors.As(err, &se) && se.Code == http.StatusServiceUnavailable {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		c.leader = "" // редирект или недоступность - заново ищем лидера
	}
	return api.TaskResponse{}, 0, lastErr
}

// waitConverged ждет совпадения реплик реестра всех живых узлов с лидерской.
func (c *procCluster) waitConverged(timeout time.Duration) (queue.RegistryDelta, error) {
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		leader, _, err := c.waitLeader(time.Until(deadline))
		if err != nil {
			return queue.RegistryDelta{}, err
		}
		ref, err := c.registry(leader)
		if err != nil {
			return ref, err
		}
		same := true
		for _, id := range c.alive() {
			st, err := c.status(id)
			if err != nil {
				return ref, err
			}
			if st.RegistryEpoch != ref.Epoch || st.RegistryVersion != ref.Version || st.KnownTasks != len(ref.Tasks) {
				same = false
				last = fmt.Sprintf("node %d at %d/%d, leader at %d/%d", id, st.RegistryEpoch, st.RegistryVersion, ref.Epoch, ref.Version)
				break
			}
		}
		if same {
			return ref, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return queue.RegistryDelta{}, fmt.Errorf("not converged within %v: %s", timeout, last)
}
