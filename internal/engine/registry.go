package engine

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// Registry holds named Go step handlers. It is safe for concurrent use.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]HandlerFunc
}

// NewRegistry returns an empty handler registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]HandlerFunc)}
}

// Register associates a handler with a name. It panics if name is empty or
// already registered, mirroring the contract of database/sql drivers.
func (r *Registry) Register(name string, fn HandlerFunc) {
	if name == "" {
		panic("engine: Register with empty handler name")
	}
	if fn == nil {
		panic("engine: Register with nil handler for " + name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.handlers[name]; dup {
		panic("engine: handler already registered: " + name)
	}
	r.handlers[name] = fn
}

// lookup returns the handler for name.
func (r *Registry) lookup(name string) (HandlerFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.handlers[name]
	return fn, ok
}

// Names returns the registered handler names, sorted.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.handlers))
	for n := range r.handlers {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// call invokes the named handler, honoring context cancellation.
func (r *Registry) call(ctx context.Context, step Step) error {
	fn, ok := r.lookup(step.Handler)
	if !ok {
		return fmt.Errorf("no handler registered for %q", step.Handler)
	}
	return fn(ctx, step)
}
