# Go Job Queue & Worker Pool

Jobs are scheduled by descending `Priority` (`int`): higher values run first.
Jobs with the same priority remain FIFO. Priority is read when a job is submitted
to the queue, so set it before calling `Push`:

```go
urgent := job.NewJob("urgent", task, time.Second)
urgent.Priority = 10
q.Push(urgent)
```

Priority only changes the order of jobs still waiting in the queue; it does not
preempt jobs already running.

## Public client

The `pkg` package provides a client for submitting jobs and reading their
statuses:

```go
import (
	"context"
	"log"
	"time"

	jobqueue "github.com/azrilpramudia/go-job-queue/pkg"
)

client, err := jobqueue.NewClient(3, 10)
if err != nil {
	log.Fatal(err)
}
defer client.Close()

err = client.Submit(jobqueue.JobSpec{
	ID:      "send-email",
	Timeout: 5 * time.Second,
	Task: func(ctx context.Context) error {
		return sendEmail(ctx)
	},
})
status, ok := client.Status("send-email")
```

`Close` stops accepting submissions, drains queued jobs, and waits for workers.
Job IDs must be unique for the lifetime of the client.

## Cron schedules

The `scheduler` package accepts standard five-field cron expressions and
descriptors such as `@every 10s`. It submits a new uniquely identified job for
each schedule occurrence. Cron expressions use the local timezone. Stop
schedules before closing the client:

```go
import "github.com/azrilpramudia/go-job-queue/scheduler"

s, err := scheduler.New(client)
if err != nil {
	log.Fatal(err)
}
_, err = s.Schedule("*/5 * * * *", jobqueue.JobSpec{
	ID:      "periodic-report",
	Timeout: time.Minute,
	Task:    generateReport,
})
if err != nil {
	log.Fatal(err)
}
s.Start()
```

Call `s.Stop(ctx)` during shutdown; it waits for already-running scheduler
callbacks to finish or returns when the context expires.
