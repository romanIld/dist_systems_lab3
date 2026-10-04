package main

import "io"

// asyncWriter пишет логи в фоновой горутине: медленный вывод (файл, консоль)
// не задерживает узел, который логирует под блокировкой состояния.
type asyncWriter struct {
	ch   chan []byte
	done chan struct{}
}

func newAsyncWriter(w io.Writer, buffer int) *asyncWriter {
	a := &asyncWriter{ch: make(chan []byte, buffer), done: make(chan struct{})}
	go func() {
		defer close(a.done)
		for b := range a.ch {
			_, _ = w.Write(b)
		}
	}()
	return a
}

// Write копирует запись в очередь; блокируется, только если очередь заполнена.
func (a *asyncWriter) Write(p []byte) (int, error) {
	a.ch <- append([]byte(nil), p...)
	return len(p), nil
}

// Close дописывает очередь и останавливает горутину.
func (a *asyncWriter) Close() {
	close(a.ch)
	<-a.done
}
