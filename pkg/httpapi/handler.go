package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	jobqueue "github.com/azrilpramudia/go-job-queue/pkg"
)

const maxRequestBodySize = 1 << 20

type Handler struct {
	client   *jobqueue.Client
	registry *Registry
	token    string
	logger   *slog.Logger
	requests atomic.Uint64
	errors   atomic.Uint64
}

type submitRequest struct {
	ID         string          `json:"id"`
	Task       string          `json:"task"`
	Payload    json.RawMessage `json:"payload"`
	Timeout    string          `json:"timeout"`
	Priority   int             `json:"priority"`
	MaxRetries int             `json:"max_retries"`
	BaseDelay  string          `json:"base_delay"`
}

type errorResponse struct {
	Error string `json:"error"`
}

type HandlerOptions struct {
	BearerToken string
	Logger      *slog.Logger
}

func NewHandler(client *jobqueue.Client, registry *Registry) (http.Handler, error) {
	return NewHandlerWithOptions(client, registry, HandlerOptions{})
}

func NewHandlerWithOptions(client *jobqueue.Client, registry *Registry, options HandlerOptions) (http.Handler, error) {
	if client == nil {
		return nil, errors.New("client cannot be nil")
	}
	if registry == nil {
		return nil, errors.New("task registry cannot be nil")
	}

	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	h := &Handler{client: client, registry: registry, token: options.BearerToken, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("GET /metrics", h.metrics)
	mux.HandleFunc("POST /jobs", h.submit)
	mux.HandleFunc("GET /jobs/{id}", h.status)
	return h.instrument(h.authenticate(mux)), nil
}

func (h *Handler) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.token == "" || r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		provided, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !found || subtle.ConstantTimeCompare([]byte(provided), []byte(h.token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer`)
			writeError(w, http.StatusUnauthorized, "valid bearer token required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (h *Handler) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(recorder, r)
		h.requests.Add(1)
		if recorder.status >= http.StatusBadRequest {
			h.errors.Add(1)
			h.logger.Warn("HTTP request completed", "method", r.Method, "route", r.Pattern,
				"status", recorder.status, "duration", time.Since(start))
			return
		}
		h.logger.Info("HTTP request completed", "method", r.Method, "route", r.Pattern,
			"status", recorder.status, "duration", time.Since(start))
	})
}

func (h *Handler) metrics(w http.ResponseWriter, _ *http.Request) {
	queueMetrics := h.client.Metrics()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, err := fmt.Fprintf(w,
		"# TYPE jobqueue_http_requests_total counter\njobqueue_http_requests_total %d\n"+
			"# TYPE jobqueue_http_errors_total counter\njobqueue_http_errors_total %d\n"+
			"# TYPE jobqueue_jobs_accepted_total counter\njobqueue_jobs_accepted_total %d\n"+
			"# TYPE jobqueue_jobs_completed_total counter\njobqueue_jobs_completed_total %d\n"+
			"# TYPE jobqueue_jobs_failed_total counter\njobqueue_jobs_failed_total %d\n"+
			"# TYPE jobqueue_jobs_retried_total counter\njobqueue_jobs_retried_total %d\n"+
			"# TYPE jobqueue_jobs_pending gauge\njobqueue_jobs_pending %d\n"+
			"# TYPE jobqueue_jobs_running gauge\njobqueue_jobs_running %d\n",
		h.requests.Load(), h.errors.Load(), queueMetrics.Accepted, queueMetrics.Completed,
		queueMetrics.Failed, queueMetrics.Retried, queueMetrics.Pending, queueMetrics.Running)
	if err != nil {
		slog.Error("write HTTP metrics response", "error", err)
	}
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	if h.client.IsClosed() {
		writeError(w, http.StatusServiceUnavailable, "job client is closed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) submit(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var request submitRequest
	if err := decoder.Decode(&request); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body is too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid JSON request")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "request body must contain a single JSON object")
		return
	}

	request.ID = strings.TrimSpace(request.ID)
	request.Task = strings.TrimSpace(request.Task)
	if request.ID == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}
	if request.Task == "" {
		writeError(w, http.StatusBadRequest, "task is required")
		return
	}
	if len(request.Payload) == 0 {
		request.Payload = json.RawMessage("null")
	}

	task, exists := h.registry.lookup(request.Task)
	if !exists {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("unknown task %q", request.Task))
		return
	}

	timeout, err := time.ParseDuration(request.Timeout)
	if err != nil || timeout <= 0 {
		writeError(w, http.StatusBadRequest, "timeout must be a positive duration such as \"5s\"")
		return
	}
	if request.MaxRetries < 0 {
		writeError(w, http.StatusBadRequest, "max_retries cannot be negative")
		return
	}
	var baseDelay time.Duration
	if request.BaseDelay != "" {
		baseDelay, err = time.ParseDuration(request.BaseDelay)
		if err != nil || baseDelay < 0 {
			writeError(w, http.StatusBadRequest, "base_delay must be a non-negative duration")
			return
		}
	}

	err = h.client.SubmitContext(r.Context(), jobqueue.JobSpec{
		ID:         request.ID,
		TaskName:   request.Task,
		Payload:    request.Payload,
		Timeout:    timeout,
		Priority:   request.Priority,
		MaxRetries: request.MaxRetries,
		BaseDelay:  baseDelay,
		Task: func(ctx context.Context) error {
			payload := append(json.RawMessage(nil), request.Payload...)
			return task(ctx, payload)
		},
	})
	if err != nil {
		switch {
		case errors.Is(err, jobqueue.ErrDuplicateJobID):
			writeError(w, http.StatusConflict, err.Error())
		case errors.Is(err, jobqueue.ErrClientClosed):
			writeError(w, http.StatusServiceUnavailable, err.Error())
		case errors.Is(err, context.Canceled):
			writeError(w, http.StatusServiceUnavailable, "submission canceled")
		case errors.Is(err, context.DeadlineExceeded):
			writeError(w, http.StatusRequestTimeout, "submission timed out")
		case errors.Is(err, jobqueue.ErrInvalidJobSpec):
			writeError(w, http.StatusBadRequest, err.Error())
		default:
			h.logger.Error("submit job", "job_id", request.ID, "error", err)
			writeError(w, http.StatusInternalServerError, "could not submit job")
		}
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{
		"id":     request.ID,
		"status": string(jobqueue.StatusPending),
	})
}

func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	status, exists := h.client.Status(r.PathValue("id"))
	if !exists {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Error("write HTTP JSON response", "error", err)
	}
}
