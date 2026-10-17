package scheduler_test

import (
	"context"
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
