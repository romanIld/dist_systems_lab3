// Package bg - группа фоновых горутин с остановкой: после Stop новые горутины не запускаются.
package bg

import "sync"

// Group отслеживает фоновые горутины узла.
type Group struct {
	mu      sync.Mutex
	stopped bool
	wg      sync.WaitGroup
}

// Go запускает f, если группа еще не остановлена.
func (g *Group) Go(f func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return
	}
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		f()
	}()
}

// Stop запрещает новые горутины и ждет завершения запущенных.
func (g *Group) Stop() {
	g.mu.Lock()
	g.stopped = true
	g.mu.Unlock()
	g.wg.Wait()
}
