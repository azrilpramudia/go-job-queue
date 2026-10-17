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
	TaskName   string
	Payload    []byte
	Timeout    time.Duration
	Priority   int
	MaxRetries int
	BaseDelay  time.Duration

	mu       sync.RWMutex
	status   Status
	err      error
	attempts int
	finished time.Time
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
	j.finished = time.Now()
}

func (j *Job) MarkFailed(err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.status = StatusFailed
	j.err = err
	j.finished = time.Now()
}

func (j *Job) Attempts() int {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.attempts
}

func (j *Job) Snapshot() (Status, error, int) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.status, j.err, j.attempts
}

func (j *Job) Restore(status Status, err error, attempts int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.status = status
	j.err = err
	j.attempts = attempts
	if status == StatusDone || status == StatusFailed {
		j.finished = time.Now()
	}
}

func (j *Job) FinishedAt() time.Time {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.finished
}

func (j *Job) ReleaseTaskData() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.Task = nil
	j.TaskName = ""
	j.Payload = nil
}

func (j *Job) IncAttempts() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.attempts++
}
