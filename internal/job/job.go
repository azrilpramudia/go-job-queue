package job

import (
	"context"
	"time"
)

type Status string

const (
	StatusPending Status = "pending"
	StatusRunning Status = "running"
	StatusDone 		Status = "done"
	StatusFailed 	Status = "failed"
)

type Task func(ctx context.Context) error

type Job struct {
	ID			string
	Task		Task
	Status 	Status
	Err 		error
	Timeout	time.Duration
}

func NewJob(id string, task Task, timeout time.Duration) *Job {
	return &Job{
		ID:	id,
		Task:	task,
		Status:	StatusPending,
		Timeout: timeout,
	}
}
