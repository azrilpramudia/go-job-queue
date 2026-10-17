package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/azrilpramudia/go-job-queue/pkg"
	"github.com/robfig/cron/v3"
)

type ScheduleID uint64

var nextScheduledJob atomic.Uint64

type Scheduler struct {
	client *pkg.Client
	cron   *cron.Cron

	mu      sync.Mutex
	started bool
	stopped bool
}

func New(client *pkg.Client) (*Scheduler, error) {
	if client == nil {
		return nil, errors.New("client cannot be nil")
	}
	return &Scheduler{client: client, cron: cron.New()}, nil
}

func (s *Scheduler) Schedule(expression string, spec pkg.JobSpec) (ScheduleID, error) {
	if spec.ID == "" {
		return 0, errors.New("job ID cannot be empty")
	}
	if spec.Task == nil {
		return 0, errors.New("job task cannot be nil")
	}
	if spec.Timeout <= 0 {
		return 0, errors.New("job timeout must be greater than zero")
	}
	if s.client.PersistenceEnabled() && spec.TaskName == "" {
		return 0, errors.New("persistent schedules require a registered task name")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return 0, errors.New("scheduler is stopped")
	}

	id, err := s.cron.AddFunc(expression, func() {
		runSpec := spec
		runSpec.ID = fmt.Sprintf("%s-%d", spec.ID, nextScheduledJob.Add(1))
		if err := s.client.Submit(runSpec); err != nil {
			slog.Error("submit scheduled job", "job_id", runSpec.ID, "error", err)
		}
	})
	if err != nil {
		return 0, fmt.Errorf("add cron schedule: %w", err)
	}
	return ScheduleID(id), nil
}

func (s *Scheduler) Remove(id ScheduleID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cron.Remove(cron.EntryID(id))
}

func (s *Scheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started || s.stopped {
		return
	}
	s.started = true
	s.cron.Start()
}

func (s *Scheduler) Stop(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context cannot be nil")
	}

	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	started := s.started
	done := s.cron.Stop()
	s.mu.Unlock()

	if !started {
		return nil
	}
	select {
	case <-done.Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
