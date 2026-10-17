package scheduler_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azrilpramudia/go-job-queue/pkg"
	"github.com/azrilpramudia/go-job-queue/scheduler"
)

func TestSchedule_RejectsInvalidCronExpression(t *testing.T) {
	client, err := pkg.NewClient(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	s, err := scheduler.New(client)
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Schedule("not a cron expression", pkg.JobSpec{
		ID:      "invalid",
		Timeout: time.Second,
		Task: func(context.Context) error {
			return nil
		},
	})
	if err == nil {
		t.Fatal("Schedule() with invalid expression should return an error")
	}
}

func TestScheduledPersistentJobIDsDoNotCollideAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	options := pkg.ClientOptions{
		WorkerCount:   1,
		QueueCapacity: 2,
		SQLitePath:    path,
		ResolveTask: func(name string, _ []byte) (pkg.Task, bool) {
			if name != "noop" {
				return nil, false
			}
			return func(context.Context) error { return nil }, true
		},
	}

	client, err := pkg.NewClientWithOptions(options)
	if err != nil {
		t.Fatal(err)
	}
	firstID := runScheduledOccurrence(t, client, "")
	if err := client.CloseWithContext(context.Background()); err != nil {
		t.Fatal(err)
	}

	client, err = pkg.NewClientWithOptions(options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	secondID := runScheduledOccurrence(t, client, firstID)

	if firstID == secondID {
		t.Fatalf("scheduled job ID after restart = %q, collides with previous occurrence", secondID)
	}
	for _, id := range []string{firstID, secondID} {
		status, ok := client.Status(id)
		if !ok || status.Status != pkg.StatusDone {
			t.Errorf("status for %q = %+v, exists %t; want persisted done status", id, status, ok)
		}
	}
}

func runScheduledOccurrence(t *testing.T, client *pkg.Client, previousID string) string {
	t.Helper()

	s, err := scheduler.New(client)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Schedule("@every 100ms", pkg.JobSpec{
		ID:       "periodic",
		TaskName: "noop",
		Payload:  []byte(`{}`),
		Timeout:  time.Second,
		Task:     func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline.C:
			_ = s.Stop(context.Background())
			t.Fatal("scheduled job did not finish")
		case <-ticker.C:
			for _, status := range client.ListStatuses() {
				if status.ID != previousID && strings.HasPrefix(status.ID, "periodic-") &&
					status.Status == pkg.StatusDone {
					if err := s.Stop(context.Background()); err != nil {
						t.Fatal(err)
					}
					return status.ID
				}
			}
		}
	}
}
