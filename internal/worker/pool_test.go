package worker_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azrilpramudia/go-job-queue/internal/job"
	"github.com/azrilpramudia/go-job-queue/internal/queue"
	"github.com/azrilpramudia/go-job-queue/internal/worker"
)

func TestPool_JobResults(t *testing.T) {
	errBoom := errors.New("boom")

	tests := []struct {
		name       string
		task       job.Task
		timeout    time.Duration
		wantStatus job.Status
		wantErr    error
	}{
		{
			name:       "sukses",
			task:       func(ctx context.Context) error { return nil },
			timeout:    time.Second,
			wantStatus: job.StatusDone,
		},
		{
			name:       "gagal karena error",
			task:       func(ctx context.Context) error { return errBoom },
			timeout:    time.Second,
			wantStatus: job.StatusFailed,
			wantErr:    errBoom,
		},
		{
			name: "timeout",
			task: func(ctx context.Context) error {
				select {
				case <-time.After(2 * time.Second):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
			timeout:    50 * time.Millisecond,
			wantStatus: job.StatusFailed,
			wantErr:    context.DeadlineExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := queue.NewQueue(1)
			pool := worker.NewPool(1, q, context.Background())
			pool.Start()

			j := job.NewJob("test", tt.task, tt.timeout)
			q.Push(j)
			q.Close()
			pool.Wait()

			if j.Status() != tt.wantStatus {
				t.Errorf("status = %v, want %v", j.Status(), tt.wantStatus)
			}
			if !errors.Is(j.Err(), tt.wantErr) {
				t.Errorf("err = %v, want %v", j.Err(), tt.wantErr)
			}
		})
	}
}

func TestPool_RetryThenSuccess(t *testing.T) {
	var calls atomic.Int32

	q := queue.NewQueue(1)
	pool := worker.NewPool(1, q, context.Background())
	pool.Start()

	j := job.NewJob("flaky", func(ctx context.Context) error {
		if calls.Add(1) < 3 {
			return errors.New("gagal sementara")
		}
		return nil
	}, time.Second)
	j.MaxRetries = 3
	j.BaseDelay = time.Millisecond

	q.Push(j)
	q.Close()
	pool.Wait()

	if j.Status() != job.StatusDone {
		t.Errorf("status = %v, want done", j.Status())
	}
	if j.Attempts() != 3 {
		t.Errorf("attempts = %d, want 3", j.Attempts())
	}
}

func TestPool_PermanentFailureAfterRetries(t *testing.T) {
	q := queue.NewQueue(1)
	pool := worker.NewPool(1, q, context.Background())
	pool.Start()

	j := job.NewJob("always-fails", func(ctx context.Context) error {
		return errors.New("gagal permanen")
	}, time.Second)
	j.MaxRetries = 2
	j.BaseDelay = time.Millisecond

	q.Push(j)
	q.Close()
	pool.Wait()

	if j.Status() != job.StatusFailed {
		t.Errorf("status = %v, want failed", j.Status())
	}
	if j.Attempts() != 3 {
		t.Errorf("attempts = %d, want 3", j.Attempts())
	}
}

func TestPool_CanceledErrorIsNotRetried(t *testing.T) {
	q := queue.NewQueue(1)
	pool := worker.NewPool(1, q, context.Background())
	pool.Start()

	j := job.NewJob("canceled", func(ctx context.Context) error {
		return context.Canceled
	}, time.Second)
	j.MaxRetries = 5
	j.BaseDelay = time.Millisecond

	q.Push(j)
	q.Close()
	pool.Wait()

	if j.Status() != job.StatusFailed {
		t.Errorf("status = %v, want failed", j.Status())
	}
	if !errors.Is(j.Err(), context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", j.Err())
	}
	if j.Attempts() != 1 {
		t.Errorf("attempts = %d, want 1", j.Attempts())
	}
}

func TestPool_ProcessesAllJobs(t *testing.T) {
	const total = 50
	var count atomic.Int64

	q := queue.NewQueue(total)
	pool := worker.NewPool(5, q, context.Background())
	pool.Start()

	for i := 0; i < total; i++ {
		q.Push(job.NewJob(fmt.Sprint(i), func(ctx context.Context) error {
			count.Add(1)
			return nil
		}, time.Second))
	}
	q.Close()
	pool.Wait()

	if got := count.Load(); got != total {
		t.Errorf("diproses %d job, want %d", got, total)
	}
}

func TestPool_ContextCancelStopsJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	q := queue.NewQueue(1)
	pool := worker.NewPool(1, q, ctx)
	pool.Start()

	started := make(chan struct{})
	j := job.NewJob("long", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}, time.Minute)
	j.MaxRetries = 5
	j.BaseDelay = time.Millisecond

	q.Push(j)
	q.Close()

	<-started
	cancel()

	done := make(chan struct{})
	go func() { pool.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pool tidak berhenti setelah context di-cancel (kemungkinan deadlock)")
	}

	if !errors.Is(j.Err(), context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", j.Err())
	}
	if j.Attempts() != 1 {
		t.Errorf("attempts = %d, want 1", j.Attempts())
	}
}

func TestPool_ReadStatusWhileRunning(t *testing.T) {
	release := make(chan struct{})
	q := queue.NewQueue(1)
	pool := worker.NewPool(1, q, context.Background())
	pool.Start()

	j := job.NewJob("x", func(ctx context.Context) error {
		<-release
		return nil
	}, time.Second)
	q.Push(j)
	q.Close()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = j.Status()
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	close(release)
	pool.Wait()
	close(stop)
	wg.Wait()
}
