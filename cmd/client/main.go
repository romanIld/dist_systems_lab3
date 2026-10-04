// Команда client - консольный клиент кластера: добавить задачу, посмотреть статус и задачи.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"lab3/internal/api"
	"lab3/internal/httpx"
)

const usage = `использование: client [-addrs url,url,...] <команда>

команды:
  add [-priority p] <data>   добавить задачу (с переходом к лидеру по 307)
  status                     роль и терм каждого узла
  tasks <node_id>            задачи узла
`

func main() {
	addrs := flag.String("addrs", "http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083", "адреса узлов через запятую")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage); flag.PrintDefaults() }
	flag.Parse()
	c := &client{http: httpx.NewClient(), addrs: strings.Split(*addrs, ",")}

	var err error
	args := flag.Args()
	switch {
	case len(args) == 0:
		flag.Usage()
		os.Exit(2)
	case args[0] == "add":
		err = c.add(args[1:])
	case args[0] == "status":
		err = c.status()
	case args[0] == "tasks" && len(args) == 2:
		err = c.tasks(args[1])
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "client:", err)
		os.Exit(1)
	}
}

type client struct {
	http  *http.Client
	addrs []string
}

// add отправляет задачу первому доступному узлу и переходит к лидеру по редиректу.
func (c *client) add(args []string) error {
	fs := flag.NewFlagSet("add", flag.ExitOnError)
	priority := fs.String("priority", "", "приоритет задачи")
	_ = fs.Parse(args)
	if fs.NArg() == 0 {
		return errors.New("add: data is required")
	}
	req := api.TaskRequest{Data: strings.Join(fs.Args(), " "), Priority: *priority}

	targets := append([]string(nil), c.addrs...)
	var lastErr error
	for attempt := 0; attempt < 10 && len(targets) > 0; attempt++ {
		addr := targets[0]
		var resp api.TaskResponse
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := httpx.PostJSON(ctx, c.http, addr+"/task", req, &resp)
		cancel()
		if err == nil {
			return printJSON(resp)
		}
		lastErr = err
		var se *httpx.StatusError
		if errors.As(err, &se) && se.Code == http.StatusTemporaryRedirect {
			var e api.ErrorResponse
			if json.Unmarshal([]byte(se.Body), &e) == nil && e.LeaderAddress != "" {
				fmt.Fprintf(os.Stderr, "%s: not the leader, redirect to %s\n", addr, e.LeaderAddress)
				targets = append([]string{e.LeaderAddress}, targets[1:]...)
				continue
			}
		}
		if errors.As(err, &se) && se.Code == http.StatusServiceUnavailable {
			fmt.Fprintf(os.Stderr, "%s: %s, retrying\n", addr, se.Body)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		fmt.Fprintf(os.Stderr, "%s: %v\n", addr, err)
		targets = targets[1:]
	}
	return fmt.Errorf("task not added: %w", lastErr)
}

func (c *client) status() error {
	fmt.Printf("%-24s %-10s %-5s %-7s %-6s %-6s\n", "ADDRESS", "ROLE", "TERM", "LEADER", "OWN", "KNOWN")
	for _, addr := range c.addrs {
		var st api.StatusResponse
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := httpx.GetJSON(ctx, c.http, addr+"/status", &st)
		cancel()
		if err != nil {
			fmt.Printf("%-24s %s\n", addr, "unavailable")
			continue
		}
		fmt.Printf("%-24s %-10s %-5d %-7d %-6d %-6d\n", addr, st.Role, st.Term, st.LeaderID, st.OwnTasks, st.KnownTasks)
	}
	return nil
}

func (c *client) tasks(nodeID string) error {
	var lastErr error
	for _, addr := range c.addrs {
		var out json.RawMessage
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := httpx.GetJSON(ctx, c.http, addr+"/tasks/"+nodeID, &out)
		cancel()
		if err == nil {
			return printJSON(out)
		}
		lastErr = err
	}
	return lastErr
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}
