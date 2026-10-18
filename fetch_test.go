package fetch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reqRecord captures one request as seen by the test server.
type reqRecord struct {
	Method string
	Path   string
	Header http.Header
	Body   []byte
}

// testServer wraps an httptest server and records every request it receives.
// handler receives the 1-based number of the current request.
type testServer struct {
	*httptest.Server

	mu    sync.Mutex
	calls []reqRecord
}

func (ts *testServer) count() int {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return len(ts.calls)
}

func (ts *testServer) record(n int) reqRecord {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if n < 1 || n > len(ts.calls) {
		return reqRecord{}
	}
	return ts.calls[n-1]
}

func newTestServer(t *testing.T, handler func(n int, w http.ResponseWriter, r *http.Request)) *testServer {
	t.Helper()
	ts := &testServer{}
	ts.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		ts.mu.Lock()
		ts.calls = append(ts.calls, reqRecord{
			Method: r.Method,
			Path:   r.URL.Path,
			Header: r.Header.Clone(),
			Body:   body,
		})
		n := len(ts.calls)
		ts.mu.Unlock()

		handler(n, w, r)
	}))
	t.Cleanup(ts.Close)
	return ts
}

// testSession builds a session with fast backoff so retry tests stay quick,
// overridden by any options passed in.
func testSession(t *testing.T, opts ...SessionOption) *Session {
	t.Helper()
	all := append([]SessionOption{
		WithDefaultBackoff(time.Millisecond, 2*time.Millisecond),
	}, opts...)
	s, err := NewSession(all...)
	require.NoError(t, err)
	t.Cleanup(s.Close)
	return s
}

func TestGet(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "payload")
	})
	s := testSession(t)

	var buf bytes.Buffer
	require.NoError(t, s.Get(context.Background(), ts.URL, &buf))

	assert.Equal(t, "payload", buf.String())
	rec := ts.record(1)
	assert.Equal(t, "GET", rec.Method)
}

func TestGetDiscardsBodyWhenWriterNil(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "payload")
	})
	s := testSession(t)
	require.NoError(t, s.Get(context.Background(), ts.URL, nil))
}

func TestRequestHeaders(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t)

	h := http.Header{}
	h.Set("X-Bulk", "bulk")
	require.NoError(t, s.Get(context.Background(), ts.URL, nil,
		WithHeader("X-Token", "abc"),
		WithHeaders(h),
	))

	rec := ts.record(1)
	assert.Equal(t, "abc", rec.Header.Get("X-Token"))
	assert.Equal(t, "bulk", rec.Header.Get("X-Bulk"))
}

func TestSessionDefaultHeadersOverriddenByRequest(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t, WithDefaultHeaders(http.Header{
		"X-Def":    {"session"},
		"X-Keepme": {"kept"},
	}))

	require.NoError(t, s.Get(context.Background(), ts.URL, nil, WithHeader("X-Def", "request")))

	rec := ts.record(1)
	assert.Equal(t, "request", rec.Header.Get("X-Def"))
	assert.Equal(t, "kept", rec.Header.Get("X-Keepme"))
}

func TestPostEchoesBody(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t)

	var buf bytes.Buffer
	body := bytes.NewBufferString(`{"hello":"world"}`)
	require.NoError(t, s.Post(context.Background(), ts.URL, body, &buf,
		WithHeader("Content-Type", "application/json")))

	rec := ts.record(1)
	assert.Equal(t, "POST", rec.Method)
	assert.JSONEq(t, `{"hello":"world"}`, string(rec.Body))
	assert.Equal(t, "application/json", rec.Header.Get("Content-Type"))
}

func TestPostReplaysBufferBodyOnRetry(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	})
	s := testSession(t)

	var buf bytes.Buffer
	body := bytes.NewBufferString("payload")
	require.NoError(t, s.Post(context.Background(), ts.URL, body, &buf))

	assert.Equal(t, "payload", string(ts.record(1).Body))
	assert.Equal(t, "payload", string(ts.record(2).Body))
	assert.Equal(t, 2, ts.count())
}

func TestOtherMethods(t *testing.T) {
	s := testSession(t)
	for _, tc := range []struct {
		name   string
		do     func(url string) error
		method string
	}{
		{"head", func(url string) error { return s.Head(context.Background(), url) }, "HEAD"},
		{"put", func(url string) error { return s.Put(context.Background(), url, strings.NewReader("x"), nil) }, "PUT"},
		{"patch", func(url string) error { return s.Patch(context.Background(), url, strings.NewReader("x"), nil) }, "PATCH"},
		{"delete", func(url string) error { return s.Delete(context.Background(), url, nil) }, "DELETE"},
		{"options", func(url string) error { return s.Options(context.Background(), url, nil) }, "OPTIONS"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
			require.NoError(t, tc.do(ts.URL))
			assert.Equal(t, tc.method, ts.record(1).Method)
		})
	}
}

func TestHeadWithContentLength(t *testing.T) {
	// A HEAD response has no body but still advertises the Content-Length
	// a GET would produce; that must not read as a truncated transfer.
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1048576")
		w.WriteHeader(http.StatusOK)
	})
	s := testSession(t, WithDefaultRetries(0))

	require.NoError(t, s.Head(context.Background(), ts.URL))
	assert.Equal(t, 1, ts.count(), "HEAD must succeed on the first attempt")
}

func TestOnResponseCalledOnRedirectHops(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if n == 1 {
			w.Header().Set("Location", "/final")
			w.WriteHeader(http.StatusFound)
			return
		}
		fmt.Fprint(w, "final")
	})
	s := testSession(t)

	var codes []int
	require.NoError(t, s.Get(context.Background(), ts.URL, nil,
		WithOnResponse(func(ri *ResponseInfo) { codes = append(codes, ri.StatusCode) })))
	assert.Equal(t, []int{302, 200}, codes)
}

func TestBaseURLResolution(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t, WithBaseURL(ts.URL+"/api/"))

	// A relative reference resolves against the base path.
	require.NoError(t, s.Get(context.Background(), "items/1", nil))
	assert.Equal(t, "/api/items/1", ts.record(1).Path)

	// An absolute path replaces the base path, per RFC 3986.
	require.NoError(t, s.Get(context.Background(), "/other", nil))
	assert.Equal(t, "/other", ts.record(2).Path)

	// An absolute URL ignores the base entirely.
	require.NoError(t, s.Get(context.Background(), ts.URL+"/third", nil))
	assert.Equal(t, "/third", ts.record(3).Path)
}

func TestInvalidURL(t *testing.T) {
	s := testSession(t)
	err := s.Get(context.Background(), "://bad", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid url")

	err = s.Get(context.Background(), "not-a-url", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing scheme or host")
}

func TestStatusError(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Reject", "yes")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, "nope")
	})
	s := testSession(t)

	err := s.Get(context.Background(), ts.URL, nil)
	require.Error(t, err)

	var se *StatusError
	require.ErrorAs(t, err, &se)
	assert.Equal(t, http.StatusNotFound, se.StatusCode)
	assert.Equal(t, "404 Not Found", se.Status)
	assert.Equal(t, "yes", se.Headers.Get("X-Reject"))
	assert.Equal(t, 1, se.Attempts)
	assert.Contains(t, err.Error(), "404")
}

func TestOnResponse(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	s := testSession(t)

	var codes []int
	err := s.Get(context.Background(), ts.URL, nil,
		WithOnResponse(func(ri *ResponseInfo) { codes = append(codes, ri.StatusCode) }))
	require.NoError(t, err)
	assert.Equal(t, []int{500, 200}, codes)

	// ResponseInfo headers must be canonical so http.Header.Get works.
	err = s.Get(context.Background(), ts.URL, nil, WithOnResponse(func(ri *ResponseInfo) {
		assert.Equal(t, 200, ri.StatusCode)
		assert.NotEmpty(t, ri.FinalURL)
	}))
	require.NoError(t, err)
}

func TestContextDeadline(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
	})
	s := testSession(t, WithDefaultRetries(0))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := s.Get(ctx, ts.URL, nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded), "want deadline exceeded, got: %v", err)
}

func TestPerAttemptTimeout(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	})
	s := testSession(t)

	err := s.Get(context.Background(), ts.URL, nil, WithTimeout(50*time.Millisecond), WithRetries(0))
	require.Error(t, err)
	assert.Equal(t, 1, ts.count())
}

func TestConcurrentGets(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "resp-%d", n)
	})
	s := testSession(t)

	var wg sync.WaitGroup
	errs := make([]error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var buf bytes.Buffer
			errs[i] = s.Get(context.Background(), ts.URL, &buf)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		assert.NoError(t, err)
	}
	assert.Equal(t, 20, ts.count())
}
