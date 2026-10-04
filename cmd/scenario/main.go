// Команда scenario запускает кластер отдельными процессами raftnode, прогоняет
// сценарии тестирования и замеры и пишет таблицы для отчета в results/.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"
)

type options struct {
	bin      string
	out      string
	logDir   string
	basePort int
	runs     int
	workTime time.Duration
	seqN     int
	concN    int
	conc     int
	bigN     int
	only     string
}

// nodeLogLevel - уровень логов запускаемых узлов.
var nodeLogLevel = "info"

func main() {
	var o options
	flag.StringVar(&o.bin, "bin", "", "путь к raftnode (по умолчанию собирается во временный каталог)")
	flag.StringVar(&o.out, "out", "results/scenario.md", "файл с таблицами результатов")
	flag.StringVar(&o.logDir, "logs", "results/logs", "каталог логов узлов")
	flag.IntVar(&o.basePort, "base-port", 18081, "порт первого узла")
	flag.IntVar(&o.runs, "runs", 10, "повторов для замеров времени выборов")
	flag.DurationVar(&o.workTime, "work-time", 2*time.Second, "время выполнения задачи в основном сценарии")
	flag.IntVar(&o.seqN, "seq", 1000, "задач в замере задержки (последовательно)")
	flag.IntVar(&o.concN, "conc-tasks", 5000, "задач в замере пропускной способности")
	flag.IntVar(&o.conc, "conc", 16, "параллельных клиентов")
	flag.IntVar(&o.bigN, "big", 50000, "всего задач в нагрузочном прогоне")
	flag.StringVar(&o.only, "only", "", "выполнить только указанные части через запятую: main,cold,failover,perf")
	flag.StringVar(&nodeLogLevel, "node-log-level", nodeLogLevel, "уровень логов узлов")
	flag.Parse()
	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "scenario:", err)
		os.Exit(1)
	}
}

func run(o options) error {
	if err := os.MkdirAll(o.logDir, 0o755); err != nil {
		return err
	}
	if o.bin == "" {
		dir, err := os.MkdirTemp("", "lab3-scenario")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		o.bin = filepath.Join(dir, "raftnode"+exeSuffix())
		build := exec.Command("go", "build", "-o", o.bin, "./cmd/raftnode")
		build.Stdout, build.Stderr = os.Stdout, os.Stderr
		if err := build.Run(); err != nil {
			return fmt.Errorf("build raftnode: %w", err)
		}
	}
	// Первый запуск нового исполняемого файла бывает медленным (проверка антивирусом),
	// поэтому прогреваем его до замеров.
	_ = exec.Command(o.bin, "-h").Run()

	r := &report{}
	r.h1("Результаты прогона cmd/scenario")
	r.p(fmt.Sprintf("Дата: %s. ОС: %s/%s, CPU: %d. Узлы - отдельные процессы на 127.0.0.1.",
		time.Now().Format("2006-01-02 15:04"), runtime.GOOS, runtime.GOARCH, runtime.NumCPU()))

	steps := []struct {
		name string
		fn   func(options, *report) error
	}{
		{"main", mainScenario},
		{"cold", coldStart},
		{"failover", leaderFailover},
		{"perf", performance},
	}
	for _, s := range steps {
		if o.only != "" && !slices.Contains(strings.Split(o.only, ","), s.name) {
			continue
		}
		fmt.Fprintf(os.Stderr, "== %s\n", s.name)
		if err := s.fn(o, r); err != nil {
			return fmt.Errorf("%s: %w", s.name, err)
		}
	}
	r.summary()

	if err := os.MkdirAll(filepath.Dir(o.out), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(o.out, []byte(r.String()), 0o644); err != nil {
		return err
	}
	fmt.Print(r.String())
	fmt.Fprintf(os.Stderr, "written %s\n", o.out)
	return nil
}

func exeSuffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

// report - markdown-документ с таблицами.
type report struct {
	b strings.Builder
	// сводные значения для таблицы характеристик
	cold, coldLaunch, failover []time.Duration
	coldReady, failoverRdy     []time.Duration
	latency                    []time.Duration
	seqRate, concRate          float64
	bigN                       int
	bigHeap                    []float64
	bigFill, bigPull, bigLF    time.Duration
}

func (r *report) h1(s string) { fmt.Fprintf(&r.b, "# %s\n\n", s) }
func (r *report) h2(s string) { fmt.Fprintf(&r.b, "## %s\n\n", s) }
func (r *report) p(s string)  { fmt.Fprintf(&r.b, "%s\n\n", s) }

func (r *report) table(head []string, rows [][]string) {
	fmt.Fprintf(&r.b, "| %s |\n", strings.Join(head, " | "))
	seps := make([]string, len(head))
	for i := range seps {
		seps[i] = "---"
	}
	fmt.Fprintf(&r.b, "| %s |\n", strings.Join(seps, " | "))
	for _, row := range rows {
		fmt.Fprintf(&r.b, "| %s |\n", strings.Join(row, " | "))
	}
	r.b.WriteString("\n")
}

func (r *report) String() string { return r.b.String() }

func msf(d time.Duration) string { return fmt.Sprintf("%.0f мс", float64(d.Microseconds())/1000) }

func secf(d time.Duration) string { return fmt.Sprintf("%.3f с", d.Seconds()) }

// stats - минимум, среднее, максимум и перцентиль.
type stats struct{ min, avg, max time.Duration }

func summarize(ds []time.Duration) stats {
	if len(ds) == 0 {
		return stats{}
	}
	s := stats{min: ds[0], max: ds[0]}
	var sum time.Duration
	for _, d := range ds {
		s.min, s.max = min(s.min, d), max(s.max, d)
		sum += d
	}
	s.avg = sum / time.Duration(len(ds))
	return s
}

func (s stats) String() string {
	return fmt.Sprintf("среднее %s (мин %s, макс %s)", msf(s.avg), msf(s.min), msf(s.max))
}
