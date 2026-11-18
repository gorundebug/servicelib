package pulsarclient

import "sync"

// Gate prevents new requests during shutdown without spawning waiter goroutines.
// Its zero value is closed; Open starts a single lifecycle.
type Gate struct {
	mu     sync.Mutex
	open   bool
	active int
	done   chan struct{}
}

func (g *Gate) Open() { g.mu.Lock(); defer g.mu.Unlock(); g.open = true; g.done = make(chan struct{}) }
func (g *Gate) Enter() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.open {
		return false
	}
	g.active++
	return true
}

// Retain tracks child delivery work of an already admitted handler, including
// during drain. New top-level requests must use Enter instead.
func (g *Gate) Retain() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active == 0 {
		return false
	}
	g.active++
	return true
}
func (g *Gate) Leave() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active--
	if !g.open && g.active == 0 {
		close(g.done)
	}
}
func (g *Gate) Close() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.done == nil {
		g.done = make(chan struct{})
		close(g.done)
	}
	if g.open {
		g.open = false
		if g.active == 0 {
			close(g.done)
		}
	}
	return g.done
}
