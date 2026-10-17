package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	jobqueue "github.com/azrilpramudia/go-job-queue/pkg"
	"github.com/azrilpramudia/go-job-queue/pkg/httpapi"
)

func newTestHandler(t *testing.T, task httpapi.TaskHandler) (*jobqueue.Client, http.Handler) {
	t.Helper()

	client, err := jobqueue.NewClient(1, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })

	registry := httpapi.NewRegistry()
	if err := registry.Register("test", task); err != nil {
		t.Fatal(err)
	}
	handler, err := httpapi.NewHandler(client, registry)
	if err != nil {
		t.Fatal(err)
	}
	return client, handler
}

func TestHTTPAPI_SubmitAndReadStatus(t *testing.T) {
	var calls atomic.Int32
	client, handler := newTestHandler(t, func(_ context.Context, payload json.RawMessage) error {
		if got, want := string(payload), `{"value":7}`; got != want {
			t.Errorf("payload = %s, want %s", got, want)
		}
		calls.Add(1)
		return nil
	})

	request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(
		`{"id":"http-job","task":"test","payload":{"value":7},"timeout":"1s"}`,
	))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("POST /jobs status = %d, want %d: %s", response.Code, http.StatusAccepted, response.Body)
	}

	deadline := time.Now().Add(time.Second)
	completed := false
	for time.Now().Before(deadline) {
		status, ok := client.Status("http-job")
		if ok && status.Status == jobqueue.StatusDone {
			if got := calls.Load(); got != 1 {
				t.Errorf("task calls = %d, want 1", got)
			}
			completed = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !completed {
		t.Fatal("submitted job did not complete before deadline")
	}

	request = httptest.NewRequest(http.MethodGet, "/jobs/http-job", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET /jobs/http-job status = %d, want %d", response.Code, http.StatusOK)
	}

	var got jobqueue.JobStatus
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != jobqueue.StatusDone || got.Attempts != 1 {
		t.Errorf("status = %+v, want done with one attempt", got)
	}
}

func TestHTTPAPI_RejectsInvalidRequests(t *testing.T) {
	_, handler := newTestHandler(t, func(context.Context, json.RawMessage) error { return nil })

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "invalid JSON",
			body:       `{"id":`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unknown task",
			body:       `{"id":"job","task":"missing","timeout":"1s"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid duration",
			body:       `{"id":"job","task":"test","timeout":"later"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unknown field",
			body:       `{"id":"job","task":"test","timeout":"1s","extra":true}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(test.body))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Errorf("status = %d, want %d: %s", response.Code, test.wantStatus, response.Body)
			}
		})
	}
}

func TestHTTPAPI_DuplicateJobAndMissingStatus(t *testing.T) {
	_, handler := newTestHandler(t, func(context.Context, json.RawMessage) error { return nil })
	body := `{"id":"same","task":"test","timeout":"1m"}`

	for i := 0; i < 2; i++ {
		request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if i == 0 && response.Code != http.StatusAccepted {
			t.Fatalf("first POST status = %d, want %d", response.Code, http.StatusAccepted)
		}
		if i == 1 && response.Code != http.StatusConflict {
			t.Fatalf("duplicate POST status = %d, want %d", response.Code, http.StatusConflict)
		}
	}

	request := httptest.NewRequest(http.MethodGet, "/jobs/missing", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Errorf("missing GET status = %d, want %d", response.Code, http.StatusNotFound)
	}
}

func TestHTTPAPI_RejectsOversizedBody(t *testing.T) {
	_, handler := newTestHandler(t, func(context.Context, json.RawMessage) error { return nil })

	request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(
		`{"id":"job","task":"test","timeout":"1s","payload":"`+
			strings.Repeat("a", 1<<20)+`"}`,
	))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want %d", response.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestHTTPAPI_HealthAndClosedClient(t *testing.T) {
	client, handler := newTestHandler(t, func(context.Context, json.RawMessage) error { return nil })

	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("health status = %d, want %d", response.Code, http.StatusOK)
	}

	client.Close()
	request = httptest.NewRequest(http.MethodGet, "/health", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Errorf("closed health status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}

func TestHTTPAPI_BearerTokenAndMetrics(t *testing.T) {
	client, err := jobqueue.NewClient(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	registry := httpapi.NewRegistry()
	if err := registry.Register("test", func(context.Context, json.RawMessage) error { return nil }); err != nil {
		t.Fatal(err)
	}
	handler, err := httpapi.NewHandlerWithOptions(client, registry, httpapi.HandlerOptions{
		BearerToken: "secret",
	})
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodGet, "/jobs/nope", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want %d", response.Code, http.StatusUnauthorized)
	}

	request = httptest.NewRequest(http.MethodGet, "/health", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("health status = %d, want %d", response.Code, http.StatusOK)
	}

	request = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	request.Header.Set("Authorization", "Bearer secret")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "jobqueue_http_requests_total") {
		t.Errorf("metrics response = %d %q", response.Code, response.Body.String())
	}
}
