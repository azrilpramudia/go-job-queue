package job

import (
	"context"
	"sync"
	"time"
)

type Status string

const (
	StatusPending Status = "pending"
	StatusRunning Status = "running"
	StatusDone    Status = "done"
	StatusFailed  Status = "failed"
)

type Task func(ctx context.Context) error

type Job struct {
	ID         string
	Task       Task
	Timeout    time.Duration
	MaxRetries int
	BaseDelay  time.Duration

	mu       sync.RWMutex
	status   Status
	err      error
	attempts int
}

func NewJob(id string, task Task, timeout time.Duration) *Job {
	return &Job{
		ID:      id,
		Task:    task,
		Timeout: timeout,
		status:  StatusPending,
	}
}

func (j *Job) Status() Status {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.status
}

func (j *Job) Err() error {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.err
}

func (j *Job) MarkRunning() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.status = StatusRunning
}

func (j *Job) MarkDone() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.status = StatusDone
}

func (j *Job) MarkFailed(err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.status = StatusFailed
	j.err = err
}

func (j *Job) Attempts() int {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.attempts
}

func (j *Job) IncAttempts() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.attempts++
}
