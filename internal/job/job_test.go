package job_test

import (
	"context"
	"testing"
	"time"

	"github.com/azrilpramudia/go-job-queue/internal/job"
)

func TestNewJob(t *testing.T) {
	expectedID := "job-123"
	expectedTimeout := 5 * time.Second
	dummyTask := func(ctx context.Context) error {
		return nil
	}

	j := job.NewJob(expectedID, dummyTask, expectedTimeout)

	if j == nil {
		t.Fatal("Expected job to not be nil")
	}
	if j.ID != expectedID {
		t.Errorf("Expected ID %s, got %s", expectedID, j.ID)
	}
	if j.Status != job.StatusPending {
		t.Errorf("Expected status %v, got %v", job.StatusPending, j.Status)
	}
	if j.Task == nil {
		t.Errorf("Expected Task to be assigned, got nil")
	}
}