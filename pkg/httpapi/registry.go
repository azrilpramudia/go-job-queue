package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	jobqueue "github.com/azrilpramudia/go-job-queue/pkg"
)

var (
	ErrInvalidTaskName = errors.New("task name cannot be empty")
	ErrNilTaskHandler  = errors.New("task handler cannot be nil")
	ErrDuplicateTask   = errors.New("task name already registered")
)

type TaskHandler func(context.Context, json.RawMessage) error

type Registry struct {
	mu       sync.RWMutex
	handlers map[string]TaskHandler
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]TaskHandler)}
}

func (r *Registry) Register(name string, handler TaskHandler) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return ErrInvalidTaskName
	}
	if handler == nil {
		return ErrNilTaskHandler
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[name]; exists {
		return fmt.Errorf("%w: %q", ErrDuplicateTask, name)
	}
	r.handlers[name] = handler
	return nil
}

func (r *Registry) Resolve(name string, payload []byte) (jobqueue.Task, bool) {
	handler, exists := r.lookup(name)
	if !exists {
		return nil, false
	}
	data := append(json.RawMessage(nil), payload...)
	return func(ctx context.Context) error {
		return handler(ctx, data)
	}, true
}

func (r *Registry) lookup(name string) (TaskHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	handler, exists := r.handlers[name]
	return handler, exists
}
