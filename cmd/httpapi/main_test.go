package main

import "testing"

func TestIsLoopbackAddress(t *testing.T) {
	tests := []struct {
		address string
		want    bool
	}{
		{address: "127.0.0.1:3000", want: true},
		{address: "localhost:3000", want: true},
		{address: "[::1]:3000", want: true},
		{address: "0.0.0.0:3000", want: false},
		{address: ":3000", want: false},
		{address: "192.0.2.1:3000", want: false},
	}

	for _, test := range tests {
		if got := isLoopbackAddress(test.address); got != test.want {
			t.Errorf("isLoopbackAddress(%q) = %t, want %t", test.address, got, test.want)
		}
	}
}

func TestLoadConfigDefaultsAndInvalidValues(t *testing.T) {
	for _, key := range []string{
		"JOB_QUEUE_ADDR",
		"JOB_QUEUE_API_TOKEN",
		"JOB_QUEUE_WORKERS",
		"JOB_QUEUE_CAPACITY",
		"JOB_QUEUE_DB_PATH",
		"JOB_QUEUE_MAX_BACKOFF",
		"JOB_QUEUE_LOG_LEVEL",
		"JOB_QUEUE_TLS_CERT",
		"JOB_QUEUE_TLS_KEY",
	} {
		t.Setenv(key, "")
	}

	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.address != "127.0.0.1:3000" || cfg.workers != 3 || cfg.queueCapacity != 10 {
		t.Errorf("unexpected config defaults: %+v", cfg)
	}
	if cfg.databasePath != "jobs.db" || cfg.maxBackoff.String() != "30s" {
		t.Errorf("unexpected persistence/backoff defaults: %+v", cfg)
	}

	t.Setenv("JOB_QUEUE_WORKERS", "not-an-int")
	if _, err := loadConfig(); err == nil {
		t.Fatal("loadConfig() should reject invalid worker count")
	}
}

func TestValidateAddressRequiresTokenOutsideLoopback(t *testing.T) {
	if err := validateAddress(config{address: "0.0.0.0:3000"}); err == nil {
		t.Fatal("non-loopback bind without API token should be rejected")
	}
	if err := validateAddress(config{address: "0.0.0.0:3000", token: "configured"}); err == nil {
		t.Fatal("non-loopback bind with a short token should be rejected")
	}
	if err := validateAddress(config{address: "0.0.0.0:3000", token: "0123456789abcdef0123456789abcdef"}); err != nil {
		if err.Error() == "" {
			t.Fatal("non-loopback validation returned an empty error")
		}
	}
	if err := validateAddress(config{
		address: "0.0.0.0:3000", token: "0123456789abcdef0123456789abcdef",
		tlsCert: "server.crt", tlsKey: "server.key",
	}); err != nil {
		t.Fatalf("non-loopback bind with token and TLS returned error: %v", err)
	}
	if err := validateAddress(config{address: "127.0.0.1:3000"}); err != nil {
		t.Fatalf("loopback bind without token returned error: %v", err)
	}
}
