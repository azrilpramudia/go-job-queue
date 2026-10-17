package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	jobqueue "github.com/azrilpramudia/go-job-queue/pkg"
	"github.com/azrilpramudia/go-job-queue/scheduler"
)

func TestClient_SubmitAndReadStatus(t *testing.T) {
	client, err := jobqueue.NewClient(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	taskErr := errors.New("task failed")
	spec := jobqueue.JobSpec{
		ID:      "integration-job",
		Timeout: time.Second,
		Task: func(context.Context) error {
			return taskErr
		},
	}
	if err := client.Submit(spec); err != nil {
		t.Fatal(err)
	}
	if err := client.Submit(spec); err == nil {
		t.Fatal("Submit() with duplicate ID should return an error")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		status, ok := client.Status(spec.ID)
		if ok && status.Status == jobqueue.StatusFailed {
			if status.Attempts != 1 {
				t.Errorf("attempts = %d, want 1", status.Attempts)
			}
			if status.Error != taskErr.Error() {
				t.Errorf("error = %q, want %q", status.Error, taskErr)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("job did not reach failed status")
}

func TestClient_CloseRejectsNewSubmissions(t *testing.T) {
	client, err := jobqueue.NewClient(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	err = client.Submit(jobqueue.JobSpec{
		ID:      "after-close",
		Timeout: time.Second,
		Task: func(context.Context) error {
			return nil
		},
	})
	if err == nil {
		t.Fatal("Submit() after Close() should return an error")
	}
}

func TestScheduler_SubmitsRecurringJobs(t *testing.T) {
	client, err := jobqueue.NewClient(2, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	s, err := scheduler.New(client)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.Schedule("@every 20ms", jobqueue.JobSpec{
		ID:      "scheduled",
		Timeout: time.Second,
		Task: func(context.Context) error {
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	s.Start()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		done := 0
		for _, status := range client.ListStatuses() {
			if status.Status == jobqueue.StatusDone {
				done++
			}
		}
		if done >= 2 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := s.Stop(ctx); err != nil {
				t.Fatalf("Stop() error = %v", err)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("scheduler did not submit two completed jobs")
}
