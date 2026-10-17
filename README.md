# Go Job Queue & Worker Pool

A Go job queue with priority scheduling, worker pools, per-job timeouts,
exponential retry, cron scheduling, a public Go client, and an optional HTTP API.
The HTTP demo can persist jobs and statuses in SQLite.

## Requirements

- Go version specified in [`go.mod`](./go.mod) or newer.
- `curl` is only needed for the manual HTTP examples.

## Quick start

Run the demo worker pool:

```sh
go run ./cmd
```

Run the local HTTP API in a separate terminal:

```sh
go run ./cmd/httpapi
```

By default, the HTTP demo listens on `127.0.0.1:3000`, stores data in `jobs.db`
in the current working directory, and registers the sample task `noop`.

Run the project's validation commands:

```sh
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
```

The Makefile also provides `make run`, `make serve`, and `make test`.

## How scheduling works

- Jobs with higher integer `Priority` are selected before lower-priority jobs.
- Jobs with equal priority are selected FIFO.
- Priority applies only while jobs are waiting in the queue; it does not preempt
  work that a worker has already started.
- A queue capacity of zero creates an unbuffered queue. A positive capacity
  bounds the number of waiting jobs.
- Multiple workers run jobs concurrently, so log output and completion order can
  differ from submission order.

## Go client

The public API is in `github.com/azrilpramudia/go-job-queue/pkg`. A minimal
in-memory example:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	jobqueue "github.com/azrilpramudia/go-job-queue/pkg"
)

func main() {
	client, err := jobqueue.NewClient(2, 10)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	err = client.Submit(jobqueue.JobSpec{
		ID:       "hello-1",
		Timeout:  5 * time.Second,
		Priority: 10,
		Task: func(ctx context.Context) error {
			fmt.Println("job is running")
			return nil
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	for {
		status, ok := client.Status("hello-1")
		if !ok {
			log.Fatal("job status not found")
		}
		if status.Status == jobqueue.StatusDone || status.Status == jobqueue.StatusFailed {
			fmt.Printf("%+v\n", status)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}
```

`NewClient(workerCount, queueCapacity)` starts the workers immediately.
`Submit` returns after the job is accepted into the queue; it does not wait for
completion. Use `Status(id)` to read one job, `ListStatuses()` to read known
statuses, and `Metrics()` for in-process counters and active-job gauges.

Job IDs must be unique while their status is retained. A duplicate returns an
error matching `jobqueue.ErrDuplicateJobID`; invalid specifications match
`jobqueue.ErrInvalidJobSpec`. After a terminal status has expired or been pruned,
the ID may be reused.

### Retry, timeout, and cancellation

`Timeout` applies to each task attempt. `MaxRetries` counts retries after the
first attempt: `MaxRetries: 2` permits up to three attempts. `BaseDelay` is the
delay before the first retry; subsequent retry delays double exponentially and
are capped by `MaxBackoff` (30 seconds by default). A zero base delay retries
without waiting. Errors matching `context.Canceled` are not retried.

Tasks should observe their context and stop when it is canceled:

```go
Task: func(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Second):
		return nil
	}
},
```

The library cannot forcibly stop a task that ignores its context.

### Graceful shutdown

`Close()` stops accepting new submissions, waits for in-progress submissions,
drains accepted jobs, and waits for workers without a time limit. Use
`CloseWithContext` when the application needs a shutdown deadline:

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
if err := client.CloseWithContext(ctx); err != nil {
	log.Printf("job client shutdown: %v", err)
}
```

When the deadline expires, the client cancels task contexts and returns the
context error. Cooperative tasks can then exit. Go cannot kill a non-cooperative
goroutine; an application that must exit by a hard deadline should log the
failure and let the process supervisor terminate it.

## Cron scheduler

The `scheduler` package supports five-field cron expressions and descriptors
such as `@every 10s`. Five-field expressions use the process's local timezone.
Each occurrence submits a new job. `Schedule` returns a `ScheduleID` that can be
passed to `Remove`.

```go
s, err := scheduler.New(client)
if err != nil {
	log.Fatal(err)
}

scheduleID, err := s.Schedule("@every 1m", jobqueue.JobSpec{
	ID:      "periodic-report",
	Timeout: time.Minute,
	Task: func(ctx context.Context) error {
		return generateReport(ctx)
	},
})
if err != nil {
	log.Fatal(err)
}
_ = scheduleID
s.Start()
```

On shutdown, stop the scheduler before closing the client. `Stop(ctx)` waits for
in-flight scheduler callbacks until its context expires:

```go
stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := s.Stop(stopCtx); err != nil {
	log.Printf("scheduler shutdown: %v", err)
}
if err := client.CloseWithContext(stopCtx); err != nil {
	log.Printf("client shutdown: %v", err)
}
```

Cron definitions are application configuration; they are not stored in SQLite
and must be registered again at startup. For persistent scheduled jobs, set
`JobSpec.TaskName` and `JobSpec.Payload`, and register a resolver for that task.
The scheduler adds a cryptographically random, per-instance namespace and a
monotonic sequence to each occurrence ID, so re-registering a schedule after a
restart does not reuse the prior process's occurrence IDs. As with all
recovered jobs, task side effects can be repeated after a crash; design handlers
to be idempotent.

## HTTP API

The implementation is in `pkg/httpapi`; the runnable example is
`cmd/httpapi`. It accepts JSON and invokes only task handlers registered by the
application—it never accepts executable code from an HTTP request.

### Start the demo

```sh
go run ./cmd/httpapi
```

The demo binds to `127.0.0.1:3000` and registers one sample handler named
`noop`. Requests are limited to 1 MiB.

### Endpoints

| Method | Path         | Description                                                            |
| ------ | ------------ | ---------------------------------------------------------------------- |
| `GET`  | `/health`    | Public health check; `200` while the client is open, `503` after close |
| `POST` | `/jobs`      | Submit a job; returns `202 Accepted`                                   |
| `GET`  | `/jobs/{id}` | Read status; returns `404` if the ID is not retained                   |
| `GET`  | `/metrics`   | Prometheus text-format metrics                                         |

Example submission:

```sh
curl -i -X POST http://127.0.0.1:3000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "demo-1",
    "task": "noop",
    "payload": {"message": "hello"},
    "timeout": "5s",
    "priority": 10,
    "max_retries": 2,
    "base_delay": "200ms"
  }'
```

The response confirms queue acceptance; it does not promise the job is still
`pending` by the time the response is read:

```json
{ "id": "demo-1", "status": "pending" }
```

Check status (repeat while it is `pending` or `running`):

```sh
curl -i http://127.0.0.1:3000/jobs/demo-1
```

Successful status response:

```json
{ "id": "demo-1", "status": "done", "attempts": 1 }
```

Failed jobs include an `error` string. `timeout` and `base_delay` use Go
duration syntax such as `5s`, `200ms`, or `1m`. `id`, `task`, and a positive
`timeout` are required. `payload` defaults to JSON `null`; `priority` and
`max_retries` default to zero, and `base_delay` defaults to zero. Unknown JSON
fields are rejected.

Common error responses:

| Status                      | Meaning                                                                      |
| --------------------------- | ---------------------------------------------------------------------------- |
| `400 Bad Request`           | Invalid JSON, missing/invalid fields, negative retry values, or unknown task |
| `401 Unauthorized`          | Missing or invalid Bearer token when authentication is enabled               |
| `409 Conflict`              | Job ID already exists                                                        |
| `413 Content Too Large`     | Request exceeds 1 MiB                                                        |
| `408 Request Timeout`       | Submission did not finish before the request deadline                        |
| `503 Service Unavailable`   | Client is closed or submission was canceled                                  |
| `500 Internal Server Error` | Unexpected submission or persistence error                                   |

### Register application tasks

The application—not the caller—defines what each task name does:

```go
registry := httpapi.NewRegistry()
err := registry.Register("send-email", func(ctx context.Context, payload json.RawMessage) error {
	var request struct {
		To string `json:"to"`
	}
	if err := json.Unmarshal(payload, &request); err != nil {
		return err
	}
	return sendEmail(ctx, request.To)
})
if err != nil {
	log.Fatal(err)
}
```

Construct the handler using the same client and registry, then mount it on an
`http.Server`. The demo binds to loopback and configures a read-header timeout;
custom servers should also configure appropriate HTTP timeouts.

## Persistence and recovery

SQLite is enabled by setting `ClientOptions.SQLitePath`; the HTTP demo enables it
by default with `jobs.db` in the current working directory. The database file
is created with owner-only permissions. Without `SQLitePath`, client state is
in memory and is lost when the process exits.

Persistent jobs need both a stable task name and a resolver. For HTTP tasks, use
the registry's `Resolve` method:

```go
client, err := jobqueue.NewClientWithOptions(jobqueue.ClientOptions{
	WorkerCount:     3,
	QueueCapacity:   10,
	SQLitePath:      "jobs.db",
	ResolveTask:     registry.Resolve,
	MaxBackoff:      30 * time.Second,
	RetentionPeriod: 7 * 24 * time.Hour,
	MaxTerminalJobs: 10_000,
})
```

Register task handlers before creating the client so pending jobs can be
resolved during startup. Jobs persisted as `pending` or `running` are queued
again after restart; completed and failed statuses are restored for inspection.
This is at-least-once recovery, not exactly-once execution: a process can stop
after an external side effect but before saving the terminal status. Make
persistent tasks idempotent.

Terminal (`done` and `failed`) status rows are pruned after the retention period
and to the configured maximum count. Defaults are seven days and 10,000 terminal
jobs; cleanup runs at startup, after a job finishes, and periodically. Once a
record has been pruned, its status is no longer available and its ID can be
reused. Pending jobs are not subject to terminal-status retention.

### Manually verify persistence across restart

Use two terminals in the repository root. In terminal 1:

```sh
JOB_QUEUE_ADDR=127.0.0.1:3000 \
JOB_QUEUE_DB_PATH=jobs.db \
go run ./cmd/httpapi
```

In terminal 2, submit a job and check that it reaches `done`:

```sh
curl -X POST http://127.0.0.1:3000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"id":"persist-check-1","task":"noop","timeout":"5s"}'

curl http://127.0.0.1:3000/jobs/persist-check-1
```

Stop the server with `Ctrl+C`, then start it again with the same working
directory and `JOB_QUEUE_DB_PATH`. Repeat the status request. The terminal status
and attempt count should still be present. Do not remove `jobs.db` during this
test.

## Security

The demo allows unauthenticated access on loopback by default. If
`JOB_QUEUE_API_TOKEN` is set, all endpoints except `/health` require:

```http
Authorization: Bearer <token>
```

For example:

```sh
export JOB_QUEUE_API_TOKEN='replace-with-a-long-random-secret'
go run ./cmd/httpapi

curl http://127.0.0.1:3000/jobs/demo-1 \
  -H "Authorization: Bearer $JOB_QUEUE_API_TOKEN"
```

The demo refuses a non-loopback bind unless the token is at least 32 characters
and both `JOB_QUEUE_TLS_CERT` and `JOB_QUEUE_TLS_KEY` are configured. TLS uses a
minimum of TLS 1.2. When serving HTTPS, use `https://` in client URLs and provide
a certificate trusted by the client. Do not put production tokens in source
control, shell history, or logs.

`NewHandler` and `NewHandlerWithOptions` are library handlers; they do not
control which network interface a custom server listens on. Configure
authentication with `HandlerOptions.BearerToken` and configure HTTPS and safe
network binding in the application that owns the `http.Server`.

## Configuration

The demo server reads these environment variables:

| Variable                | Default          | Description                                          |
| ----------------------- | ---------------- | ---------------------------------------------------- |
| `JOB_QUEUE_ADDR`        | `127.0.0.1:3000` | HTTP(S) listen address                               |
| `JOB_QUEUE_API_TOKEN`   | unset            | Optional Bearer token; required off loopback         |
| `JOB_QUEUE_WORKERS`     | `3`              | Number of workers; must be positive                  |
| `JOB_QUEUE_CAPACITY`    | `10`             | Waiting queue capacity; zero is unbuffered           |
| `JOB_QUEUE_DB_PATH`     | `jobs.db`        | SQLite database path                                 |
| `JOB_QUEUE_MAX_BACKOFF` | `30s`            | Maximum retry delay; must be positive                |
| `JOB_QUEUE_LOG_LEVEL`   | `info`           | `debug`, `info`, `warn`, or `error`                  |
| `JOB_QUEUE_TLS_CERT`    | unset            | TLS certificate file                                 |
| `JOB_QUEUE_TLS_KEY`     | unset            | TLS private-key file; configure with the certificate |

## Logging and metrics

The demo writes structured JSON logs to stdout using `log/slog`. Set
`JOB_QUEUE_LOG_LEVEL` to control verbosity. Job execution, retries, completion,
HTTP request status, and persistence/shutdown errors are logged.

`GET /metrics` returns Prometheus text-format metrics:

- HTTP request and error counters.
- Jobs accepted, completed, failed, and retried.
- Current pending and running job counts.

Counters are process-local and reset on restart; they are not read from SQLite.
When a token is enabled, `/metrics` requires authentication. `/health` remains
public for health probes.

## Troubleshooting

- **Connection refused:** start the server in another terminal and confirm the
  address/port from its startup log. The default is port `3000`.
- **`401 Unauthorized`:** send the configured token with the `Authorization:
Bearer ...` header. `/health` does not require it.
- **`409 Conflict`:** choose a new job ID or wait until the old terminal record
  has been pruned.
- **Unknown task (`400`):** register the task name before serving requests.
- **Job remains pending:** check worker logs, queue capacity, and task
  registration. A saturated bounded queue may make `POST /jobs` wait until a
  worker frees capacity; the HTTP request context can cancel that submission.
- **Persistence recovery fails at startup:** ensure every persisted pending
  task name is registered and the configured database path is the same.
- **Non-loopback server refuses to start:** configure a 32-character-or-longer
  token and both TLS certificate and key paths.
