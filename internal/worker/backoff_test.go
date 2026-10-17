package worker

import (
	"testing"
	"time"
)

func TestBackoffCapsWithoutOverflow(t *testing.T) {
	const maximum = 30 * time.Second

	tests := []struct {
		base    time.Duration
		attempt int
		want    time.Duration
	}{
		{base: 200 * time.Millisecond, attempt: 0, want: 200 * time.Millisecond},
		{base: 200 * time.Millisecond, attempt: 1, want: 400 * time.Millisecond},
		{base: 20 * time.Second, attempt: 1, want: maximum},
		{base: time.Second, attempt: 100_000, want: maximum},
		{base: 0, attempt: 5, want: 0},
	}

	for _, test := range tests {
		if got := backoff(test.base, test.attempt, maximum); got != test.want {
			t.Errorf("backoff(%v, %d) = %v, want %v", test.base, test.attempt, got, test.want)
		}
	}
}
