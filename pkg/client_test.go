package pkg_test

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azrilpramudia/go-job-queue/internal/store"
	jobqueue "github.com/azrilpramudia/go-job-queue/pkg"
)

func TestClient_PersistsAndRestoresJobStatus(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	var calls atomic.Int32
	options := jobqueue.ClientOptions{
		WorkerCount:   1,
		QueueCapacity: 2,
		SQLitePath:    path,
		ResolveTask: func(name string, payload []byte) (jobqueue.Task, bool) {
			if name != "count" || string(payload) != `{"n":1}` {
				return nil, false
			}
			return func(context.Context) error {
				calls.Add(1)
				return nil
			}, true
		},
	}

	client, err := jobqueue.NewClientWithOptions(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Submit(jobqueue.JobSpec{
		ID:       "persistent",
		TaskName: "count",
		Payload:  []byte(`{"n":1}`),
		Timeout:  time.Second,
		Task: func(context.Context) error {
			calls.Add(1)
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, client, "persistent", jobqueue.StatusDone)
	if err := client.CloseWithContext(context.Background()); err != nil {
		t.Fatal(err)
	}

	client, err = jobqueue.NewClientWithOptions(options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	status, ok := client.Status("persistent")
	if !ok || status.Status != jobqueue.StatusDone || status.Attempts != 1 {
		t.Fatalf("restored status = %+v, exists %t; want done with one attempt", status, ok)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("task calls = %d, want 1 (terminal job must not rerun)", got)
	}
}

func TestClient_RestartsRunningJobAsPending(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	database, err := store.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	err = database.Insert(context.Background(), store.Record{
		ID:         "interrupted",
		TaskName:   "recover",
		Payload:    []byte(`{}`),
		Timeout:    time.Second,
		MaxRetries: 2,
		Status:     "running",
		Attempts:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	client, err := jobqueue.NewClientWithOptions(jobqueue.ClientOptions{
		WorkerCount:   1,
		QueueCapacity: 1,
		SQLitePath:    path,
		ResolveTask: func(name string, _ []byte) (jobqueue.Task, bool) {
			if name != "recover" {
				return nil, false
			}
			return func(context.Context) error {
				calls.Add(1)
				return nil
			}, true
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	waitForStatus(t, client, "interrupted", jobqueue.StatusDone)
	status, _ := client.Status("interrupted")
	if status.Attempts != 2 {
		t.Errorf("attempts = %d, want 2 (one before and one after recovery)", status.Attempts)
	}
	if calls.Load() != 1 {
		t.Errorf("recovered task calls = %d, want 1", calls.Load())
	}
}

func TestClient_PersistentRetentionPrunesTerminalRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	options := jobqueue.ClientOptions{
		WorkerCount:     1,
		QueueCapacity:   2,
		SQLitePath:      path,
		MaxTerminalJobs: 1,
		ResolveTask: func(string, []byte) (jobqueue.Task, bool) {
			return func(context.Context) error { return nil }, true
		},
	}
	client, err := jobqueue.NewClientWithOptions(options)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"old", "new"} {
		if err := client.Submit(jobqueue.JobSpec{
			ID:       id,
			TaskName: "success",
			Payload:  []byte(`{}`),
			Timeout:  time.Second,
			Task:     func(context.Context) error { return nil },
		}); err != nil {
			t.Fatal(err)
		}
		waitForStatus(t, client, id, jobqueue.StatusDone)
	}
	if err := client.CloseWithContext(context.Background()); err != nil {
		t.Fatal(err)
	}

	client, err = jobqueue.NewClientWithOptions(options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, ok := client.Status("old"); ok {
		t.Error("oldest terminal job was not pruned from SQLite")
	}
	if _, ok := client.Status("new"); !ok {
		t.Error("newest terminal job should remain in SQLite")
	}
}

func TestClient_CloseContextBoundsWaitForUncooperativeTask(t *testing.T) {
	client, err := jobqueue.NewClient(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	if err := client.Submit(jobqueue.JobSpec{
		ID:      "uncooperative",
		Timeout: time.Second,
		Task: func(context.Context) error {
			close(started)
			<-release
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := client.CloseWithContext(ctx); err != context.DeadlineExceeded {
		t.Fatalf("CloseWithContext() error = %v, want deadline exceeded", err)
	}

	close(release)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := client.CloseWithContext(waitCtx); err != nil {
		t.Fatalf("CloseWithContext() after releasing task error = %v", err)
	}
}

func TestClient_CloseContextUnblocksSubmissionWaitingForCapacity(t *testing.T) {
	client, err := jobqueue.NewClient(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	if err := client.Submit(jobqueue.JobSpec{
		ID:      "running",
		Timeout: time.Second,
		Task: func(context.Context) error {
			close(started)
			<-release
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := client.Submit(jobqueue.JobSpec{
		ID:      "queued",
		Timeout: time.Second,
		Task:    func(ctx context.Context) error { return ctx.Err() },
	}); err != nil {
		t.Fatal(err)
	}

	submitDone := make(chan error, 1)
	go func() {
		submitDone <- client.Submit(jobqueue.JobSpec{
			ID:      "blocked-submit",
			Timeout: time.Second,
			Task:    func(context.Context) error { return nil },
		})
	}()
	select {
	case err := <-submitDone:
		t.Fatalf("submission unexpectedly completed while queue was full: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := client.CloseWithContext(ctx); err != context.DeadlineExceeded {
		t.Fatalf("CloseWithContext() error = %v, want deadline exceeded", err)
	}
	select {
	case err := <-submitDone:
		if err == nil {
			t.Fatal("blocked submission should be canceled during shutdown")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked submission did not unblock during shutdown")
	}
	if _, ok := client.Status("blocked-submit"); ok {
		t.Error("canceled submission remained in job status map")
	}

	close(release)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := client.CloseWithContext(waitCtx); err != nil {
		t.Fatalf("CloseWithContext() after releasing task error = %v", err)
	}
}

func TestClient_RetainsOnlyConfiguredNumberOfTerminalJobs(t *testing.T) {
	client, err := jobqueue.NewClientWithOptions(jobqueue.ClientOptions{
		WorkerCount:     1,
		QueueCapacity:   2,
		MaxTerminalJobs: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	for _, id := range []string{"old", "new"} {
		if err := client.Submit(jobqueue.JobSpec{
			ID:      id,
			Timeout: time.Second,
			Task:    func(context.Context) error { return nil },
		}); err != nil {
			t.Fatal(err)
		}
		waitForStatus(t, client, id, jobqueue.StatusDone)
	}

	if _, ok := client.Status("old"); ok {
		t.Error("oldest terminal job was not removed by retention")
	}
	if _, ok := client.Status("new"); !ok {
		t.Error("newest terminal job should remain available")
	}
}

func waitForStatus(t *testing.T, client *jobqueue.Client, id string, want jobqueue.Status) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		status, ok := client.Status(id)
		if ok && status.Status == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	status, _ := client.Status(id)
	t.Fatalf("job status = %v, want %v", status.Status, want)
}
