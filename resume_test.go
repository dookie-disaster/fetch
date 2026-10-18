package fetch

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fullBody = "hello world" // 11 bytes

// rangeServer serves fullBody, honoring Range: bytes=N- requests with 206.
func rangeServer(t *testing.T, extra func(n int, w http.ResponseWriter, r *http.Request) bool) *testServer {
	t.Helper()
	return newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if extra != nil && extra(n, w, r) {
			return
		}
		rng := r.Header.Get("Range")
		if rng == "" {
			w.Header().Set("Content-Length", fmt.Sprint(len(fullBody)))
			fmt.Fprint(w, fullBody)
			return
		}
		var start int64
		if _, err := fmt.Sscanf(rng, "bytes=%d-", &start); err != nil || start < 0 || start > int64(len(fullBody)) {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(fullBody)))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		rest := fullBody[start:]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, int64(len(fullBody))-1, len(fullBody)))
		w.Header().Set("Content-Length", fmt.Sprint(len(rest)))
		w.WriteHeader(http.StatusPartialContent)
		fmt.Fprint(w, rest)
	})
}

func TestAutoResumeFromPartialFile(t *testing.T) {
	ts := rangeServer(t, nil)
	s := testSession(t)

	path := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644)) // 5 bytes done

	fw := NewFileWriter(path)
	defer fw.Close()

	require.NoError(t, s.Get(context.Background(), ts.URL, fw, WithResume(true)))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, fullBody, string(data))

	// The very first request carried the range header.
	assert.Equal(t, "bytes=5-", ts.record(1).Header.Get("Range"))
	assert.Equal(t, 1, ts.count())
}

func TestResumeDisabledByDefault(t *testing.T) {
	ts := rangeServer(t, nil)
	s := testSession(t)

	path := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))

	fw := NewFileWriter(path)
	defer fw.Close()

	require.NoError(t, s.Get(context.Background(), ts.URL, fw))

	assert.Empty(t, ts.record(1).Header.Get("Range"), "no Range without WithResume")
	data, _ := os.ReadFile(path)
	assert.Equal(t, "hello"+fullBody, string(data), "body appended without resume")
}

func TestResumeFallsBackWhenServerIgnoresRange(t *testing.T) {
	ts := rangeServer(t, func(n int, w http.ResponseWriter, r *http.Request) bool {
		// Strip the Range header so rangeServer serves the full body with
		// a plain 200, ignoring the client's resume request.
		_ = n
		r.Header.Del("Range")
		return false
	})
	s := testSession(t)

	path := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))

	fw := NewFileWriter(path)
	defer fw.Close()

	require.NoError(t, s.Get(context.Background(), ts.URL, fw, WithResume(true)))

	// The stale prefix was discarded and the full body written cleanly.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, fullBody, string(data))
	assert.Equal(t, int64(len(fullBody)), fw.Pos())
}

func TestResumeOnRetryAfterTruncatedBody(t *testing.T) {
	var truncated bool
	ts := rangeServer(t, func(n int, w http.ResponseWriter, r *http.Request) bool {
		if r.Header.Get("Range") == "" && !truncated {
			truncated = true
			// Promise the full body but deliver only 10 bytes, then cut
			// the connection: the client sees an unexpected EOF mid-body.
			w.Header().Set("Content-Length", fmt.Sprint(len(fullBody)))
			fmt.Fprint(w, fullBody[:10])
			return true
		}
		return false
	})
	s := testSession(t, WithDefaultRetries(1))

	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path)
	defer fw.Close()

	require.NoError(t, s.Get(context.Background(), ts.URL, fw, WithResume(true)))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, fullBody, string(data))

	require.Equal(t, 2, ts.count())
	assert.Equal(t, "bytes=10-", ts.record(2).Header.Get("Range"), "retry must resume from written offset")
}

func TestResumeRangeRespectsUserHeader(t *testing.T) {
	ts := rangeServer(t, nil)
	s := testSession(t)

	path := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))

	fw := NewFileWriter(path)
	defer fw.Close()

	// A user-supplied Range wins over the writer-derived one.
	require.NoError(t, s.Get(context.Background(), ts.URL, fw,
		WithResume(true),
		WithHeader("Range", "bytes=2-")))

	assert.Equal(t, "bytes=2-", ts.record(1).Header.Get("Range"))
}

func TestResumeMismatchFails(t *testing.T) {
	ts := rangeServer(t, func(n int, w http.ResponseWriter, r *http.Request) bool {
		rng := r.Header.Get("Range")
		if rng != "" {
			// Lie about where the partial content starts.
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 99-%d/%d", len(fullBody)-1, len(fullBody)))
			w.Header().Set("Content-Length", "10")
			w.WriteHeader(http.StatusPartialContent)
			fmt.Fprint(w, fullBody[1:])
			return true
		}
		return false
	})
	s := testSession(t, WithDefaultRetries(0))

	path := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))

	fw := NewFileWriter(path)
	defer fw.Close()

	err := s.Get(context.Background(), ts.URL, fw, WithResume(true))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mismatch")

	data, _ := os.ReadFile(path)
	assert.Equal(t, "hello", string(data), "file must be untouched on mismatch")
}

func TestResumeWithNonSeekerWriter(t *testing.T) {
	ts := rangeServer(t, nil)
	s := testSession(t)

	// A plain buffer cannot seek: no Range is sent, everything is written.
	var buf bytes.Buffer
	require.NoError(t, s.Get(context.Background(), ts.URL, &buf, WithResume(true)))
	assert.Equal(t, fullBody, buf.String())
	assert.Empty(t, ts.record(1).Header.Get("Range"))
}

func TestResumeAcrossSessionInstances(t *testing.T) {
	// Simulate a worker restarting: one session writes partial data, the
	// file is closed, and a new session resumes the same file.
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if rng := r.Header.Get("Range"); rng == "bytes=5-" {
			w.Header().Set("Content-Range", "bytes 5-10/11")
			w.Header().Set("Content-Length", "6")
			w.WriteHeader(http.StatusPartialContent)
			fmt.Fprint(w, " world")
			return
		}
		w.Header().Set("Content-Length", "5")
		fmt.Fprint(w, "hello")
	})

	path := filepath.Join(t.TempDir(), "out.bin")

	s1 := testSession(t)
	fw1 := NewFileWriter(path)
	require.NoError(t, s1.Get(context.Background(), ts.URL, fw1))
	require.NoError(t, fw1.Close())
	data, _ := os.ReadFile(path)
	require.Equal(t, "hello", string(data))
	require.Equal(t, 1, ts.count())

	s2 := testSession(t)
	fw2 := NewFileWriter(path)
	defer fw2.Close()
	require.NoError(t, s2.Get(context.Background(), ts.URL, fw2, WithResume(true)))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, fullBody, string(data))
	assert.Equal(t, "bytes=5-", ts.record(2).Header.Get("Range"))
}
