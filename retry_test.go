package fetch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultRetryPolicy(t *testing.T) {
	policy := DefaultRetryPolicy

	assert.True(t, policy(context.Background(), 0, nil, errors.New("boom")), "transport error should retry")
	assert.False(t, policy(context.Background(), 0, nil, nil), "nothing to decide on")

	for _, code := range []int{408, 425, 429, 500, 502, 503, 504} {
		ri := &ResponseInfo{StatusCode: code}
		assert.True(t, policy(context.Background(), 0, ri, nil), "status %d should retry", code)
	}
	for _, code := range []int{200, 201, 302, 400, 401, 403, 404, 409, 410, 418} {
		ri := &ResponseInfo{StatusCode: code}
		assert.False(t, policy(context.Background(), 0, ri, nil), "status %d should not retry", code)
	}

	// A response that ended prematurely is retried even though its status
	// looks fine.
	ri := &ResponseInfo{StatusCode: 200}
	err := fmt.Errorf("fetch: read response body: %w (%w)", ErrIncompleteResponse, io.ErrUnexpectedEOF)
	assert.True(t, policy(context.Background(), 0, ri, err), "incomplete response should retry")
}

func TestRetryOn500ThenSuccess(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, "recovered")
	})
	s := testSession(t, WithDefaultRetries(2))

	var buf bytes.Buffer
	require.NoError(t, s.Get(context.Background(), ts.URL, &buf))
	assert.Equal(t, "recovered", buf.String())
	assert.Equal(t, 2, ts.count())
}

func TestNoRetryOn404ByDefault(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	s := testSession(t, WithDefaultRetries(3))

	err := s.Get(context.Background(), ts.URL, nil)
	require.Error(t, err)
	var se *StatusError
	require.ErrorAs(t, err, &se)
	assert.Equal(t, http.StatusNotFound, se.StatusCode)
	assert.Equal(t, 1, se.Attempts)
	assert.Equal(t, 1, ts.count())
}

func TestRetriesExhaustedOn503(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	s := testSession(t, WithDefaultRetries(2))

	err := s.Get(context.Background(), ts.URL, nil)
	require.Error(t, err)
	var se *StatusError
	require.ErrorAs(t, err, &se)
	assert.Equal(t, http.StatusServiceUnavailable, se.StatusCode)
	assert.Equal(t, 3, se.Attempts)
	assert.Equal(t, 3, ts.count())
}

func TestCustomPolicyRetries404Once(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	s := testSession(t)

	calls := 0
	err := s.Get(context.Background(), ts.URL, nil,
		WithRetryPolicy(func(ctx context.Context, attempt int, ri *ResponseInfo, err error) bool {
			calls++
			return ri != nil && ri.StatusCode == http.StatusNotFound && attempt < 1
		}))
	require.Error(t, err)

	var se *StatusError
	require.ErrorAs(t, err, &se)
	assert.Equal(t, http.StatusNotFound, se.StatusCode)
	assert.Equal(t, 2, se.Attempts)
	assert.Equal(t, 2, calls)
	assert.Equal(t, 2, ts.count())
}

func TestCustomPolicyStopsRetries(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	s := testSession(t, WithDefaultRetries(3))

	err := s.Get(context.Background(), ts.URL, nil,
		WithRetryPolicy(func(ctx context.Context, attempt int, ri *ResponseInfo, err error) bool {
			return false
		}))
	require.Error(t, err)
	assert.Equal(t, 1, ts.count())
}

func TestCustomPolicySeesTransportErrors(t *testing.T) {
	srv := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	url := srv.URL
	srv.Close() // connection refused from here on

	s := testSession(t, WithDefaultRetries(2))

	var sawErr bool
	var sawNilRI bool
	err := s.Get(context.Background(), url, nil,
		WithRetryPolicy(func(ctx context.Context, attempt int, ri *ResponseInfo, err error) bool {
			if err != nil {
				sawErr = true
				sawNilRI = ri == nil
				return true
			}
			return false
		}))
	require.Error(t, err)
	assert.True(t, sawErr, "policy should see transport errors")
	assert.True(t, sawNilRI, "response info should be nil for transport errors")
}

func TestRetryTransportErrorWrappedWithAttempts(t *testing.T) {
	srv := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	url := srv.URL
	srv.Close()

	s := testSession(t, WithDefaultRetries(2))
	err := s.Get(context.Background(), url, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "3 attempt(s)")
}

func TestRetryNonReplayableBody(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	})
	s := testSession(t, WithDefaultRetries(2))

	body := &oneShotReader{data: "payload"}
	err := s.Post(context.Background(), ts.URL, body, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrBodyNotReplayable)
	// The first attempt went out; the retry aborted before sending.
	assert.Equal(t, 1, ts.count())
}

type oneShotReader struct {
	data string
	used bool
}

func (r *oneShotReader) Read(p []byte) (int, error) {
	if r.used {
		return 0, errors.New("already consumed")
	}
	r.used = true
	return copy(p, r.data), io.EOF
}

func TestWithBodyProvider(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	})
	s := testSession(t)

	providers := 0
	err := s.Post(context.Background(), ts.URL, nil, nil,
		WithBodyProvider(func() (io.Reader, error) {
			providers++
			return strings.NewReader("payload"), nil
		}))
	require.NoError(t, err)
	assert.Equal(t, 2, providers)
	assert.Equal(t, "payload", string(ts.record(2).Body))
}

func TestBackoffDelayBounds(t *testing.T) {
	// First retry waits exactly minWait.
	for i := 0; i < 50; i++ {
		assert.Equal(t, 500*time.Millisecond, backoffDelay(500*time.Millisecond, 30*time.Second, 1))
	}

	// min == max means fixed delay.
	assert.Equal(t, 10*time.Millisecond, backoffDelay(10*time.Millisecond, 10*time.Millisecond, 5))

	// Exponential cap: 10, 20, 40, 80, 160ms for retries 1..5.
	min, max := 10*time.Millisecond, time.Second
	for retry, cap := range map[int]time.Duration{1: 10, 2: 20, 3: 40, 4: 80, 5: 160} {
		for i := 0; i < 200; i++ {
			d := backoffDelay(min, max, retry)
			assert.GreaterOrEqual(t, d, min, "retry %d", retry)
			assert.LessOrEqual(t, d, cap*time.Millisecond, "retry %d", retry)
		}
	}

	// Cap never exceeds maxWait, even for absurd retry numbers.
	for i := 0; i < 200; i++ {
		d := backoffDelay(time.Millisecond, 100*time.Millisecond, 100)
		assert.GreaterOrEqual(t, d, time.Millisecond)
		assert.LessOrEqual(t, d, 100*time.Millisecond)
	}

	// min > max is sanitized; zero max disables waiting.
	assert.Equal(t, 100*time.Millisecond, backoffDelay(200*time.Millisecond, 100*time.Millisecond, 3))
	assert.Equal(t, time.Duration(0), backoffDelay(0, 0, 3))
	// min == 0 keeps delays at zero (documented contract).
	assert.Equal(t, time.Duration(0), backoffDelay(0, time.Second, 10))
}

func TestBackoffTimingWithJitter(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	s := testSession(t, WithDefaultRetries(2))

	start := time.Now()
	err := s.Get(context.Background(), ts.URL, nil,
		WithRetryBackoff(50*time.Millisecond, 100*time.Millisecond))
	elapsed := time.Since(start)
	require.Error(t, err)

	// Retry 1 waits 50ms; retry 2 waits somewhere in [50ms, 100ms].
	assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
	assert.Less(t, elapsed, 5*time.Second)
	assert.Equal(t, 3, ts.count())
}

func TestWithRetriesNegativeDisablesRetries(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	// The session default would give the request retries; the negative
	// per-request value must override it down to a single attempt.
	s := testSession(t, WithDefaultRetries(2))

	err := s.Get(context.Background(), ts.URL, nil, WithRetries(-1))
	require.Error(t, err)
	assert.Equal(t, 1, ts.count())
}

func TestRetryAfterSecondsHonored(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	})
	s := testSession(t, WithDefaultRetries(1))

	start := time.Now()
	err := s.Get(context.Background(), ts.URL, nil,
		WithRetryBackoff(time.Millisecond, 5*time.Second))
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Equal(t, 2, ts.count())
	assert.GreaterOrEqual(t, elapsed, time.Second,
		"the server-suggested delay must override the short backoff")
}

func TestRetryAfterCappedByMaxBackoff(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "86400")
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	s := testSession(t, WithDefaultRetries(1), WithDefaultBackoff(time.Millisecond, 40*time.Millisecond))

	start := time.Now()
	err := s.Get(context.Background(), ts.URL, nil)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Equal(t, 2, ts.count())
	assert.Less(t, elapsed, time.Second,
		"an absurd Retry-After must be capped at the maximum backoff")
}

func TestRetryAfterHTTPDateHonored(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", time.Now().UTC().Add(1500*time.Millisecond).Format(http.TimeFormat))
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	s := testSession(t, WithDefaultRetries(1))

	start := time.Now()
	err := s.Get(context.Background(), ts.URL, nil,
		WithRetryBackoff(time.Millisecond, 5*time.Second))
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Equal(t, 2, ts.count())
	// http.TimeFormat is second-resolution, so the effective wait can be
	// up to a second shorter than the requested 1500ms.
	assert.GreaterOrEqual(t, elapsed, 500*time.Millisecond)
	assert.Less(t, elapsed, 3*time.Second)
}

func TestRetryAfterDelay(t *testing.T) {
	d, ok := retryAfterDelay(nil)
	assert.False(t, ok)
	d, ok = retryAfterDelay(&ResponseInfo{})
	assert.False(t, ok)
	d, ok = retryAfterDelay(&ResponseInfo{Headers: http.Header{"Retry-After": {"soon"}}})
	assert.False(t, ok)

	d, ok = retryAfterDelay(&ResponseInfo{Headers: http.Header{"Retry-After": {" 2 "}}})
	assert.True(t, ok)
	assert.Equal(t, 2*time.Second, d)

	d, ok = retryAfterDelay(&ResponseInfo{Headers: http.Header{"Retry-After": {"-3"}}})
	assert.False(t, ok, "negative seconds are unusable")

	// An HTTP date roughly two seconds out; allow for clock and truncation slop.
	d, ok = retryAfterDelay(&ResponseInfo{Headers: http.Header{"Retry-After": {
		time.Now().UTC().Add(2 * time.Second).Format(http.TimeFormat)}}})
	assert.True(t, ok)
	assert.Greater(t, d, 1000*time.Millisecond)
	assert.LessOrEqual(t, d, 3*time.Second)

	// A stale date comes back as zero.
	d, ok = retryAfterDelay(&ResponseInfo{Headers: http.Header{"Retry-After": {
		time.Now().UTC().Add(-time.Hour).Format(http.TimeFormat)}}})
	assert.True(t, ok)
	assert.Equal(t, time.Duration(0), d)
}

func TestRetryStopsWhenContextCanceled(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	s := testSession(t, WithDefaultRetries(3))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := s.Get(ctx, ts.URL, nil,
		WithRetryBackoff(1*time.Second, 2*time.Second))
	elapsed := time.Since(start)

	// The context error comes back promptly instead of waiting out the backoff.
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, elapsed, 500*time.Millisecond)
	assert.Equal(t, 1, ts.count())
}
