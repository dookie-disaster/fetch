package fetch

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedirectStripsCredentialsCrossHost(t *testing.T) {
	var gotSecond http.Header
	ts1 := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/target")
		w.WriteHeader(http.StatusFound)
	})
	ts2 := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		gotSecond = r.Header.Clone()
	})
	_ = ts1
	// Simulate cross-host by redirecting ts1 -> ts2 via absolute URL.
	tsredir := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", ts2.URL+"/target")
		w.WriteHeader(http.StatusFound)
	})
	s := testSession(t)
	require.NoError(t, s.Get(context.Background(), tsredir.URL, nil,
		WithHeader("Authorization", "Bearer secret"),
		WithCookieHeader("sid=42"),
		WithHeader("X-Custom", "keep-me-if-same-host"),
		WithHeader("Accept", "text/plain"),
	))
	require.NotNil(t, gotSecond)
	assert.Empty(t, gotSecond.Get("Authorization"), "Authorization must not leak cross-host")
	assert.Empty(t, gotSecond.Get("Cookie"), "Cookie must not leak cross-host")
	assert.Empty(t, gotSecond.Get("X-Custom"), "custom headers must be stripped cross-host")
	assert.Equal(t, "text/plain", gotSecond.Get("Accept"), "safelist headers survive")
}

func TestRedirectPreservesHeadersSameHost(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/target", http.StatusFound)
			return
		}
	})
	s := testSession(t)
	require.NoError(t, s.Get(context.Background(), ts.URL+"/redirect", nil,
		WithHeader("Authorization", "Bearer same-host-ok"),
		WithHeader("X-Custom", "kept"),
	))
	assert.Equal(t, "Bearer same-host-ok", ts.record(2).Header.Get("Authorization"))
	assert.Equal(t, "kept", ts.record(2).Header.Get("X-Custom"))
}

func TestMaxRedirectsOption(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/loop", http.StatusFound)
	})
	s := testSession(t)
	err := s.Get(context.Background(), ts.URL+"/loop", nil, WithMaxRedirects(0))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTooManyRedirects)
	assert.Equal(t, 1, ts.count())

	_, err = NewSession(WithDefaultMaxRedirects(-1))
	require.Error(t, err)
}

func TestResume206WithoutContentRangeFails(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" {
			w.WriteHeader(http.StatusPartialContent)
			// Missing Content-Range on purpose.
			_, _ = w.Write([]byte(" world"))
			return
		}
		_, _ = w.Write([]byte(fullBody))
	})
	s := testSession(t, WithDefaultRetries(0))
	path := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))
	fw := NewFileWriter(path)
	defer fw.Close()
	err := s.Get(context.Background(), ts.URL, fw, WithResume(true))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Content-Range")
}

func TestRetryAfterHugeCapped(t *testing.T) {
	ri := &ResponseInfo{Headers: http.Header{"Retry-After": {"9999999999"}}}
	d, ok := retryAfterDelay(ri)
	require.True(t, ok)
	assert.Greater(t, d, time.Duration(0), "huge delta must saturate positive, not overflow negative")
	rc := &requestConfig{retryMinWait: time.Millisecond, retryMaxWait: 40 * time.Millisecond}
	assert.Equal(t, 40*time.Millisecond, retryWait(rc, ri, 1))
}

func TestBodyAndProviderConflictFails(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t)
	err := s.Post(context.Background(), ts.URL, strings.NewReader("x"), nil,
		WithBodyProvider(func() (io.Reader, error) { return strings.NewReader("y"), nil }))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrBodyNotReplayable)
	assert.Equal(t, 0, ts.count(), "conflicting request must fail before sending")
}

func TestBodyProviderConflict(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t)
	// Nil body + provider is the supported combination.
	require.NoError(t, s.Post(context.Background(), ts.URL, nil, nil,
		WithBodyProvider(func() (io.Reader, error) { return strings.NewReader("y"), nil })))
	assert.Equal(t, "y", string(ts.record(1).Body))
}

func TestInvalidSessionOptionsFail(t *testing.T) {
	for _, opt := range []SessionOption{
		WithDefaultRetries(-1),
		WithDefaultBackoff(2*time.Second, time.Second),
		WithDefaultProgressInterval(0),
		WithDefaultTimeout(-time.Second),
		WithDefaultMaxRedirects(-1),
		WithProxy("ftp://127.0.0.1:21"),
		WithProxy("https://127.0.0.1:8443"),
		WithProxy("http://"),
	} {
		_, err := NewSession(opt)
		require.Error(t, err)
	}
	_, err := NewSession(WithBaseURL("ftp://example.com/x"))
	require.Error(t, err)
	_, err = NewSession(WithBaseURL("https://user@example.com/"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "userinfo")
}

func TestInvalidRequestOptionsFail(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t)
	for _, opts := range [][]RequestOption{
		{WithRetryBackoff(2*time.Second, time.Second)},
		{WithProgressInterval(0)},
		{WithTimeout(-time.Second)},
		{WithMaxRedirects(-1)},
		{WithHeader("Bad\r\nKey", "v")},
		{WithHeader("X-Ok", "bad\r\nvalue")},
	} {
		err := s.Get(context.Background(), ts.URL, nil, opts...)
		require.Error(t, err, "opts %v", opts)
	}
}

func TestSchemeRestriction(t *testing.T) {
	s := testSession(t)
	err := s.Get(context.Background(), "ftp://example.com/file", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "http or https")
}

func TestCookieMerge(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t)
	require.NoError(t, s.Get(context.Background(), ts.URL, nil,
		WithCookieHeader("a=1"),
		WithCookies(&http.Cookie{Name: "b", Value: "2"}),
		WithCookieHeader("c=3"),
	))
	assert.Equal(t, "a=1; b=2; c=3", ts.record(1).Header.Get("Cookie"))
}

func TestFileWriterSeekAfterCloseFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path)
	_, err := fw.Write([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, fw.Close())
	before := fw.Pos()
	_, err = fw.Seek(0, 0)
	require.ErrorIs(t, err, os.ErrClosed)
	assert.Equal(t, before, fw.Pos(), "position must not mutate after Close")
}

func TestProgressCumulativeAcrossRetry(t *testing.T) {
	var wrote int
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "" && n == 1 {
			w.Header().Set("Content-Length", "11")
			_, _ = w.Write([]byte(fullBody[:10]))
			// Hijack-free truncation: close handler without writing the last
			// byte is not possible with net/http, so simulate by relying on
			// the Content-Length mismatch check on the client.
			wrote++
			return
		}
		// Range path serves the remainder.
		start := int64(0)
		_, _ = w.Write([]byte(fullBody[start:]))
		_ = wrote
	})
	_ = ts
	// The truncated-body resume path is already covered by
	// TestResumeOnRetryAfterTruncatedBody; here we assert the progress
	// callback never regresses across that retry.
	rangeTs := rangeServer(t, func(n int, w http.ResponseWriter, r *http.Request) bool {
		if r.Header.Get("Range") == "" && n == 1 {
			w.Header().Set("Content-Length", "11")
			_, _ = w.Write([]byte(fullBody[:10]))
			return true
		}
		return false
	})
	s := testSession(t, WithDefaultRetries(1))
	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path)
	defer fw.Close()
	var last int64 = -1
	monotonic := true
	require.NoError(t, s.Get(context.Background(), rangeTs.URL, fw,
		WithResume(true),
		WithProgress(func(st ProgressStats) {
			if st.Written < last {
				monotonic = false
			}
			last = st.Written
		}),
	))
	assert.True(t, monotonic, "progress Written must never regress across retries")
	var buf bytes.Buffer
	_ = buf
}

func TestParseContentRangeRejectsMaxInt64End(t *testing.T) {
	_, _, _, ok := parseContentRange("bytes 0-9223372036854775807/*")
	assert.False(t, ok, "e == MaxInt64 must be rejected to avoid int64 overflow")
}

func TestGetRejectsMaxInt64ContentRange(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-9223372036854775807/*")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("x"))
	})
	s := testSession(t, WithDefaultRetries(0))
	var out bytes.Buffer
	err := s.Get(context.Background(), ts.URL, &out)
	require.Error(t, err, "206 with MaxInt64 end must fail instead of overflowing expectedLen to negative")
}

func TestHostlessURLsWithPortsRejected(t *testing.T) {
	s := testSession(t)
	err := s.Get(context.Background(), "http://:80/x", nil)
	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "host")

	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "http://:80/x")
		w.WriteHeader(http.StatusFound)
	})
	err = s.Get(context.Background(), ts.URL, nil)
	require.Error(t, err)

	_, err = NewSession(WithBaseURL("http://:80/"))
	require.Error(t, err)

	_, err = NewSession(WithProxy("http://:8080"))
	require.Error(t, err)
}

func TestInvalidHeaderKeyEmptySliceRejected(t *testing.T) {
	_, err := NewSession(WithDefaultHeaders(http.Header{"Bad\r\nKey": {}}))
	require.Error(t, err)

	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t)
	err = s.Get(context.Background(), ts.URL, nil, WithHeaders(http.Header{"Bad\r\nKey": {}}))
	require.Error(t, err)
}

func TestNilOptionsDoNotPanic(t *testing.T) {
	var nilSessionOpt SessionOption
	require.NotPanics(t, func() {
		s, err := NewSession(nilSessionOpt)
		require.NoError(t, err)
		s.Close()
	})
	require.NotPanics(t, func() {
		_, _ = NewSession(nil)
	})

	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t)
	var nilReqOpt RequestOption
	require.NotPanics(t, func() {
		require.NoError(t, s.Get(context.Background(), ts.URL, nil, nilReqOpt))
	})
	require.NotPanics(t, func() {
		require.NoError(t, s.Get(context.Background(), ts.URL, nil, nil))
	})
}

func TestWithCookiesRejectsInjection(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t)
	cases := []*http.Cookie{
		{Name: "a", Value: "b;c"},
		{Name: "a=b", Value: "c"},
		{Name: "a;b", Value: "c"},
		{Name: "a b", Value: "c"},
		{Name: "a", Value: "b\x01c"},
		{Name: "bad\r\n", Value: "c"},
	}
	for _, c := range cases {
		err := s.Get(context.Background(), ts.URL, nil, WithCookies(c))
		require.Error(t, err, "cookie %+v should be rejected", c)
	}
	assert.Equal(t, 0, ts.count(), "invalid cookies must fail before sending")
}

func TestRetryAfterExceedingInt64Saturates(t *testing.T) {
	ri := &ResponseInfo{Headers: http.Header{"Retry-After": {"99999999999999999999"}}}
	d, ok := retryAfterDelay(ri)
	require.True(t, ok, "delta-seconds exceeding MaxInt64 must saturate, not return ok=false")
	assert.Greater(t, d, time.Duration(0), "saturated duration must be positive")
	rc := &requestConfig{retryMinWait: time.Millisecond, retryMaxWait: 40 * time.Millisecond}
	assert.Equal(t, 40*time.Millisecond, retryWait(rc, ri, 1))
}
