package fetch

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sardanioss/httpcloak"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithClientOwnership(t *testing.T) {
	cloak := httpcloak.NewSession(DefaultPreset)
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	})

	s, err := NewSession(WithClient(cloak))
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, s.Get(context.Background(), ts.URL, &buf))
	assert.Equal(t, "ok", buf.String())

	assert.Same(t, cloak, s.Client(), "Client() returns the supplied session")

	// Closing the fetch session leaves the user-owned client usable.
	s.Close()
	assert.True(t, cloak.IsActive())
	cloak.Close()
}

func TestNewSessionEmptyPreset(t *testing.T) {
	_, err := NewSession(WithPreset(""))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "preset")
}

func TestNewSessionInvalidBaseURLFails(t *testing.T) {
	// An unparseable base and a base without a scheme/host must surface at
	// construction time instead of silently weakening every request.
	for _, bad := range []string{"http://[::1", "relative-only", ""} {
		_, err := NewSession(WithBaseURL(bad))
		require.Error(t, err, "base %q", bad)
		assert.Contains(t, err.Error(), "base url", "base %q", bad)
	}

	// A valid base keeps working and no setup error surfaces elsewhere.
	s, err := NewSession(WithBaseURL("https://api.example.com/v1/"), WithPreset("chrome-latest"))
	require.NoError(t, err)
	defer s.Close()
	assert.Equal(t, "https://api.example.com/v1/", s.cfg.baseURL.String())
}

func TestWithProxyWiresThrough(t *testing.T) {
	for _, proxyURL := range []string{
		"http://127.0.0.1:8080",
		"http://user:pass@127.0.0.1:8080",
		"socks5://127.0.0.1:1080",
		"socks5h://user:pass@127.0.0.1:1080",
	} {
		t.Run(proxyURL, func(t *testing.T) {
			s, err := NewSession(WithProxy(proxyURL))
			require.NoError(t, err)
			defer s.Close()
			assert.Equal(t, proxyURL, s.Client().GetProxy())
		})
	}
}

func TestWithProxyLastWins(t *testing.T) {
	s, err := NewSession(WithProxy("http://127.0.0.1:1"), WithProxy("socks5://127.0.0.1:2"))
	require.NoError(t, err)
	defer s.Close()
	assert.Equal(t, "socks5://127.0.0.1:2", s.Client().GetProxy())
}

func TestWithProxyInvalidFails(t *testing.T) {
	for _, bad := range []struct {
		url, want string
	}{
		{"://bad", "proxy"},
		{"ftp://127.0.0.1:21", "proxy"},
		{"http://", "proxy"},
		{"http:///nohost", "proxy"},
		{"socks4://127.0.0.1:1080", "proxy"},
		// The https scheme is rejected outright: the underlying client
		// connects to HTTP proxies in plaintext, so accepting it would send
		// the CONNECT request (and any credentials) in the clear.
		{"https://127.0.0.1:8443", "plaintext"},
	} {
		_, err := NewSession(WithProxy(bad.url))
		require.Error(t, err, "proxy %q", bad.url)
		assert.Contains(t, err.Error(), bad.want)
	}
}

func TestWithProxyAndClientConflict(t *testing.T) {
	cloak := httpcloak.NewSession(DefaultPreset)
	defer cloak.Close()

	_, err := NewSession(WithClient(cloak), WithProxy("http://127.0.0.1:8080"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WithProxy")
	assert.True(t, cloak.IsActive(), "supplied client must be left untouched")
}

// serveCONNECTProxy runs a minimal HTTP CONNECT proxy on a listener of the
// test's choosing until the listener is closed: it counts the CONNECT
// requests it receives and tunnels the accepted sockets to the requested
// host:port. It exists so proxy tests can prove traffic actually flows
// through the configured proxy instead of only checking configuration
// pass-through.
func serveCONNECTProxy(t *testing.T, ln net.Listener, connectCount *atomic.Int64) {
	t.Helper()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				br := bufio.NewReader(c)
				line, err := br.ReadString('\n')
				if err != nil {
					return
				}
				fields := strings.Fields(line)
				if len(fields) != 3 || fields[0] != "CONNECT" {
					return
				}
				connectCount.Add(1)
				// Consume the remainder of the CONNECT request before answering.
				for {
					hdr, err := br.ReadString('\n')
					if err != nil {
						return
					}
					if hdr == "\r\n" {
						break
					}
				}
				up, err := net.DialTimeout("tcp", fields[1], 5*time.Second)
				if err != nil {
					return
				}
				defer up.Close()
				if _, err := c.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n")); err != nil {
					return
				}
				done := make(chan struct{}, 2)
				go func() { io.Copy(up, c); done <- struct{}{} }()
				go func() { io.Copy(c, up); done <- struct{}{} }()
				<-done
			}(conn)
		}
	}()
}

func TestWithProxyRoutesThroughProxy(t *testing.T) {
	// A local CONNECT proxy stands between the session and the target server.
	// The response is delivered through the tunnel, so the GET can only
	// succeed if the session really honors the configured proxy.
	var connects atomic.Int64
	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { proxyLn.Close() })
	serveCONNECTProxy(t, proxyLn, &connects)

	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "tunneled")
	})

	s := testSession(t, WithProxy("http://"+proxyLn.Addr().String()))
	var buf bytes.Buffer
	require.NoError(t, s.Get(context.Background(), ts.URL, &buf))
	assert.Equal(t, "tunneled", buf.String())
	assert.Positive(t, connects.Load(), "the request must be tunneled through the configured proxy")
}

func TestWithProxyDeadProxyFailsInsteadOfBypassing(t *testing.T) {
	// Nothing listens on 127.0.0.1:1. If the session ever lost the proxy
	// wiring and dialed direct instead, this request would succeed against
	// the running target server and the Error assertion below would fail.
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "direct")
	})
	s := testSession(t, WithProxy("http://127.0.0.1:1"))
	var buf bytes.Buffer
	err := s.Get(context.Background(), ts.URL, &buf, WithRetries(0))
	require.Error(t, err, "a dead proxy must fail the request rather than dial direct")
	assert.NotContains(t, buf.String(), "direct")
	assert.Contains(t, err.Error(), "proxy")
}

func TestWithPreset(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	})
	s, err := NewSession(WithPreset("chrome-146"))
	require.NoError(t, err)
	defer s.Close()

	var buf bytes.Buffer
	require.NoError(t, s.Get(context.Background(), ts.URL, &buf))
	assert.Equal(t, "ok", buf.String())
}

func TestDefaultRetriesApply(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	s := testSession(t, WithDefaultRetries(3))

	require.NoError(t, s.Get(context.Background(), ts.URL, nil))
	assert.Equal(t, 3, ts.count())
}

func TestDefaultRetryPolicyApplies(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	s := testSession(t, WithDefaultRetryPolicy(func(ctx context.Context, attempt int, ri *ResponseInfo, err error) bool {
		return false
	}))

	err := s.Get(context.Background(), ts.URL, nil)
	require.Error(t, err)
	assert.Equal(t, 1, ts.count())
}

func TestDefaultBackoffInvalidValuesIgnored(t *testing.T) {
	// min > max is invalid and must fail at construction instead of being
	// silently ignored.
	_, err := NewSession(WithDefaultBackoff(5*time.Second, 1*time.Second))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backoff")
}

func TestDefaultProgressIntervalApplies(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data")
	})
	s := testSession(t, WithDefaultProgressInterval(5*time.Millisecond))

	called := 0
	var buf bytes.Buffer
	require.NoError(t, s.Get(context.Background(), ts.URL, &buf,
		WithProgress(func(st ProgressStats) { called++ })))
	assert.Greater(t, called, 0)
	assert.Equal(t, "data", buf.String())
}

func TestDefaultResumeApplies(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if rng := r.Header.Get("Range"); rng == "bytes=2-" {
			w.Header().Set("Content-Range", "bytes 2-10/11")
			w.Header().Set("Content-Length", "9")
			w.WriteHeader(http.StatusPartialContent)
			fmt.Fprint(w, "llo world")
			return
		}
		w.Header().Set("Content-Length", "11")
		fmt.Fprint(w, fullBody)
	})
	s := testSession(t, WithDefaultResume(true))

	path := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("he"), 0o644))

	fw := NewFileWriter(path)
	defer fw.Close()

	require.NoError(t, s.Get(context.Background(), ts.URL, fw))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, fullBody, string(data))
	assert.Equal(t, "bytes=2-", ts.record(1).Header.Get("Range"))
}

func TestDefaultTimeoutApplies(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
	})
	s := testSession(t, WithDefaultTimeout(50*time.Millisecond), WithDefaultRetries(0))

	err := s.Get(context.Background(), ts.URL, nil)
	require.Error(t, err)
	assert.Equal(t, 1, ts.count())
}

func TestParseContentRange(t *testing.T) {
	tests := []struct {
		in                string
		start, end, total int64
		ok                bool
	}{
		{"bytes 5-10/11", 5, 10, 11, true},
		{"bytes 0-0/1", 0, 0, 1, true},
		{"bytes 5-10/*", 5, 10, -1, true},
		{"BYTES 5-10/11", 5, 10, 11, true}, // unit is case-insensitive
		{"5-10/11", 0, 0, -1, false},       // missing unit is rejected
		{"items 5-10/11", 0, 0, -1, false}, // wrong unit
		{"bytes /11", 0, 0, -1, false},
		{"bytes 5-10/", 0, 0, -1, false},
		{"bytes -5-10/11", 0, 0, -1, false},
		{"bytes 5-4/11", 0, 0, -1, false},  // end < start
		{"bytes 5-11/11", 0, 0, -1, false}, // end beyond total
		{"bytes 5-/11", 0, 0, -1, false},   // missing end
		{"", 0, 0, -1, false},
		{"garbage", 0, 0, -1, false},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			start, end, total, ok := parseContentRange(tc.in)
			assert.Equal(t, tc.ok, ok)
			if tc.ok {
				assert.Equal(t, tc.start, start)
				assert.Equal(t, tc.end, end)
				assert.Equal(t, tc.total, total)
			}
		})
	}
}

func TestResetWriterWithPlainSeeker(t *testing.T) {
	// A truncating seeker is rewound and truncated so a shorter re-sent body
	// cannot leave a stale tail.
	path := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("partial data"), 0o644))

	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer f.Close()

	require.NoError(t, resetWriter(f))

	pos, err := f.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(0), pos)

	_, err = f.WriteString(fullBody)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, fullBody, string(data), "truncate must discard the stale tail")
}

func TestCanonicalHeaders(t *testing.T) {
	assert.Nil(t, canonicalHeaders(nil))

	h := canonicalHeaders(map[string][]string{
		"content-type":   {"text/plain"},
		"x-multi":        {"a", "b"},
		"weird-key-name": {"v"},
	})
	assert.Equal(t, "text/plain", h.Get("Content-Type"))
	assert.Equal(t, []string{"a", "b"}, h.Values("X-Multi"))
	assert.Equal(t, "v", h.Get("Weird-Key-Name"))
}

func TestStatusText(t *testing.T) {
	assert.Equal(t, "404 Not Found", statusText(404))
	assert.Equal(t, "599", statusText(599), "unknown codes render as the bare number")
}

func TestBodyReplayerReaderTypes(t *testing.T) {
	// Each in-memory reader type must yield independent fresh readers.
	buffers := []io.Reader{
		bytes.NewBufferString("buf"),
		bytes.NewReader([]byte("bred")),
		strings.NewReader("sred"),
	}
	for i, b := range buffers {
		fn := bodyReplayer(b)
		r1, err := fn()
		require.NoError(t, err, "reader %d", i)
		data1, err := io.ReadAll(r1)
		require.NoError(t, err, "reader %d", i)

		r2, err := fn()
		require.NoError(t, err, "reader %d", i)
		data2, err := io.ReadAll(r2)
		require.NoError(t, err, "reader %d", i)

		assert.Equal(t, data1, data2, "reader %d must replay identically", i)
	}

	// nil body stays nil.
	r, err := bodyReplayer(nil)()
	require.NoError(t, err)
	assert.Nil(t, r)
}

func TestStatusErrorAsError(t *testing.T) {
	se := &StatusError{URL: "http://x", StatusCode: 418, Status: "418 I'm a teapot", Attempts: 2}
	var err error = se
	assert.Contains(t, err.Error(), "418")
	assert.Contains(t, err.Error(), "2 attempt(s)")

	var target *StatusError
	require.True(t, errors.As(err, &target))
	assert.Equal(t, 418, target.StatusCode)
	assert.False(t, errors.Is(err, ErrIncompleteResponse))
}
