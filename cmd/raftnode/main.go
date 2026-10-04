// Команда raftnode запускает один узел кластера распределенной очереди задач.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"lab3/internal/node"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "raftnode:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := node.DefaultConfig()
	var (
		peers    = flag.String("peers", "1=http://127.0.0.1:8081,2=http://127.0.0.1:8082,3=http://127.0.0.1:8083", "все узлы кластера: id=url,...")
		seed     = flag.Uint64("seed", uint64(time.Now().UnixNano()), "seed генератора election timeout")
		logLevel = flag.String("log-level", "info", "уровень логов: debug, info, warn")
	)
	flag.IntVar(&cfg.ID, "id", 1, "ID этого узла")
	flag.StringVar(&cfg.Listen, "listen", "", "адрес прослушивания (по умолчанию порт из своего адреса в -peers)")
	flag.DurationVar(&cfg.Raft.HeartbeatInterval, "heartbeat", cfg.Raft.HeartbeatInterval, "интервал heartbeat")
	flag.DurationVar(&cfg.Raft.ElectionTimeoutMin, "election-min", cfg.Raft.ElectionTimeoutMin, "минимальный election timeout")
	flag.DurationVar(&cfg.Raft.ElectionTimeoutMax, "election-max", cfg.Raft.ElectionTimeoutMax, "максимальный election timeout")
	flag.DurationVar(&cfg.Queue.WorkTime, "work-time", cfg.Queue.WorkTime, "время выполнения одной задачи")
	flag.DurationVar(&cfg.Queue.DeadAfter, "dead-after", cfg.Queue.DeadAfter, "узел считается упавшим без heartbeat-ответа дольше этого")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return err
	}
	out := newAsyncWriter(os.Stdout, 4096)
	defer out.Close()
	cfg.Logger = slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{Level: level}))
	var err error
	if cfg.Peers, err = node.ParsePeers(*peers); err != nil {
		return err
	}
	cfg.Raft.Seed = *seed

	n, err := node.New(cfg)
	if err != nil {
		return err
	}
	if err := n.Start(); err != nil {
		return err
	}
	cfg.Logger.Info("node listening", "node", cfg.ID, "address", cfg.Peers[cfg.ID], "peers", node.FormatPeers(cfg.Peers))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	cfg.Logger.Info("shutting down", "node", cfg.ID)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return n.Shutdown(shutdownCtx)
}
