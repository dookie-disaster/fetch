package fetch

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
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

func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, err := gz.Write(data)
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	return buf.Bytes()
}

func TestCompressedResponseWithContentLength(t *testing.T) {
	payload := []byte("hello world compressed payload hello world")
	compressed := gzipBytes(t, payload)
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Length", fmt.Sprint(len(compressed)))
		w.Write(compressed)
	})
	s := testSession(t)

	var buf bytes.Buffer
	require.NoError(t, s.Get(context.Background(), ts.URL, &buf))
	assert.Equal(t, payload, buf.Bytes())
	assert.Equal(t, 1, ts.count(), "decompressed response must not trigger a retry")
}

func TestCompressedResponseProgressUnknownTotal(t *testing.T) {
	payload := []byte("progress over gzip progress over gzip")
	compressed := gzipBytes(t, payload)
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Length", fmt.Sprint(len(compressed)))
		w.Write(compressed)
	})
	s := testSession(t)

	var got ProgressStats
	var buf bytes.Buffer
	require.NoError(t, s.Get(context.Background(), ts.URL, &buf,
		WithProgress(func(st ProgressStats) { got = st }),
		WithProgressInterval(time.Millisecond),
	))
	assert.Equal(t, payload, buf.Bytes())
	assert.Equal(t, int64(len(payload)), got.Written)
	assert.Equal(t, int64(-1), got.Total, "wire Content-Length describes compressed bytes")
}

func TestHeadWithCompressedLength(t *testing.T) {
	payload := []byte("head payload")
	compressed := gzipBytes(t, payload)
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Length", fmt.Sprint(len(compressed)))
	})
	s := testSession(t, WithDefaultRetries(0))
	require.NoError(t, s.Head(context.Background(), ts.URL))
	assert.Equal(t, 1, ts.count())
}

func truncatedServer(t *testing.T, full string) *testServer {
	return newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if n == 1 {
			w.Header().Set("Content-Length", fmt.Sprint(len(full)))
			_, _ = w.Write([]byte(full[:len(full)-1]))
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(full)))
		fmt.Fprint(w, full)
	})
}

func TestNonResumedRetryBufferNoDuplication(t *testing.T) {
	ts := truncatedServer(t, fullBody)
	s := testSession(t, WithDefaultRetries(1))

	var buf bytes.Buffer
	require.NoError(t, s.Get(context.Background(), ts.URL, &buf))
	assert.Equal(t, fullBody, buf.String(), "retry must reset buffer, not append a second copy")
	assert.Equal(t, 2, ts.count())
}

func TestNonResumedRetryFileWriterNoDuplication(t *testing.T) {
	ts := truncatedServer(t, fullBody)
	s := testSession(t, WithDefaultRetries(1))

	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path)
	defer fw.Close()

	require.NoError(t, s.Get(context.Background(), ts.URL, fw))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, fullBody, string(data))
	assert.Equal(t, 2, ts.count())
}

func TestNonResumedRetryOSFileNoDuplication(t *testing.T) {
	ts := truncatedServer(t, fullBody)
	s := testSession(t, WithDefaultRetries(1))

	path := filepath.Join(t.TempDir(), "out.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer f.Close()

	require.NoError(t, s.Get(context.Background(), ts.URL, f))
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, fullBody, string(data))
	assert.Equal(t, 2, ts.count())
}

func TestNonResumedRetryBufferWithPreexisting(t *testing.T) {
	ts := truncatedServer(t, fullBody)
	s := testSession(t, WithDefaultRetries(1))

	var buf bytes.Buffer
	buf.WriteString("pre-")
	initLen := buf.Len()
	require.NoError(t, s.Get(context.Background(), ts.URL, &buf))
	assert.Equal(t, "pre-"+fullBody, buf.String())
	assert.Equal(t, initLen+len(fullBody), buf.Len())
}

func TestRedirectDisallowedSchemesFail(t *testing.T) {
	for _, loc := range []string{
		"ftp://example.com/file",
		"http://user:pass@example.com/",
		"http:///nohost",
		"javascript:alert(1)",
	} {
		t.Run(loc, func(t *testing.T) {
			ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", loc)
				w.WriteHeader(http.StatusFound)
			})
			s := testSession(t)
			err := s.Get(context.Background(), ts.URL, nil, WithMaxRedirects(5))
			require.Error(t, err)
			assert.Equal(t, 1, ts.count(), "disallowed redirect must not be followed")
		})
	}
}

func TestRedirectStickyCrossOrigin(t *testing.T) {
	var final http.Header
	var tsATarget, tsBTarget string
	tsA := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/start":
			w.Header().Set("Location", tsATarget)
			w.WriteHeader(http.StatusFound)
		case "/final":
			final = r.Header.Clone()
		default:
			http.NotFound(w, r)
		}
	})
	tsB := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", tsBTarget)
		w.WriteHeader(http.StatusFound)
	})
	tsATarget = tsB.URL + "/mid"
	tsBTarget = tsA.URL + "/final"

	s := testSession(t)
	require.NoError(t, s.Get(context.Background(), tsA.URL+"/start", nil,
		WithHeader("Authorization", "Bearer secret"),
		WithCookieHeader("sid=42"),
		WithHeader("X-Custom", "nope"),
		WithHeader("Accept", "text/plain"),
	))
	require.NotNil(t, final)
	assert.Empty(t, final.Get("Authorization"))
	assert.Empty(t, final.Get("Cookie"))
	assert.Empty(t, final.Get("X-Custom"), "sticky taint must not re-attach after bouncing back")
	assert.Equal(t, "text/plain", final.Get("Accept"))
}

func TestSameOriginDefaultPorts(t *testing.T) {
	assert.True(t, sameOrigin("https://example.com:443/a", "https://example.com/b"))
	assert.True(t, sameOrigin("http://example.com:80/a", "http://example.com/b"))
	assert.False(t, sameOrigin("https://example.com:444/a", "https://example.com/b"))
	assert.False(t, sameOrigin("https://example.com/a", "http://example.com/a"))
	assert.False(t, sameOrigin("https://a.example.com/", "https://b.example.com/"))
}

func TestPost302StripsBodyHeaders(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			w.Header().Set("Location", "/target")
			w.WriteHeader(http.StatusFound)
			return
		}
		fmt.Fprintf(w, "%s|%s|%s|%s", r.Method, r.Header.Get("Content-Type"), r.Header.Get("Content-Length"), string(mustRead(t, r)))
	})
	s := testSession(t)

	var buf bytes.Buffer
	require.NoError(t, s.Post(context.Background(), ts.URL+"/redirect",
		bytes.NewBufferString("payload"), &buf,
		WithHeader("Content-Type", "application/json")))
	parts := strings.Split(buf.String(), "|")
	require.Len(t, parts, 4)
	assert.Equal(t, "GET", parts[0])
	assert.Empty(t, parts[1], "Content-Type must be stripped on GET rewrite")
	assert.Empty(t, parts[2], "Content-Length must be stripped on GET rewrite")
	assert.Empty(t, parts[3])
}

func mustRead(t *testing.T, r *http.Request) []byte {
	t.Helper()
	b, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	return b
}

func TestResumedProgressFullFile(t *testing.T) {
	ts := rangeServer(t, nil)
	s := testSession(t)

	path := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))

	fw := NewFileWriter(path)
	defer fw.Close()

	var last ProgressStats
	require.NoError(t, s.Get(context.Background(), ts.URL, fw,
		WithResume(true),
		WithProgress(func(st ProgressStats) { last = st }),
		WithProgressInterval(time.Millisecond),
	))
	assert.Equal(t, int64(len(fullBody)), last.Total)
	assert.Equal(t, int64(len(fullBody)), last.Written)
}

func TestProgressWriterWrappingFileWriterResumes(t *testing.T) {
	ts := rangeServer(t, nil)
	s := testSession(t)

	path := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))

	fw := NewFileWriter(path)
	defer fw.Close()
	pw := NewProgressWriter(fw, time.Millisecond, nil)

	require.NoError(t, s.Get(context.Background(), ts.URL, pw, WithResume(true)))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, fullBody, string(data))
	assert.Equal(t, "bytes=5-", ts.record(1).Header.Get("Range"))
}

func TestUnexpected206Fails(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 5-10/11")
		w.Header().Set("Content-Length", "6")
		w.WriteHeader(http.StatusPartialContent)
		fmt.Fprint(w, " world")
	})
	s := testSession(t, WithDefaultRetries(0))

	var buf bytes.Buffer
	err := s.Get(context.Background(), ts.URL, &buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected 206")
	assert.Equal(t, 0, buf.Len())
}

func Test206ContentLengthMismatchFails(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		// Claims Range 0-10/11 but Content-Length disagrees.
		w.Header().Set("Content-Range", "bytes 0-10/11")
		w.Header().Set("Content-Length", "5")
		w.WriteHeader(http.StatusPartialContent)
		fmt.Fprint(w, "hello")
	})
	s := testSession(t, WithDefaultRetries(0))

	var buf bytes.Buffer
	err := s.Get(context.Background(), ts.URL, &buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "disagrees")
}

func TestOvershootTruncated(t *testing.T) {
	// Valid full-resource 206 (0-10/11) sent chunked with 16 bytes on the
	// wire: only the advertised 11 may reach the file.
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-10/11")
		w.WriteHeader(http.StatusPartialContent)
		if f, ok := w.(http.Flusher); ok {
			f.Flush() // force chunked encoding: no Content-Length on the wire
		}
		fmt.Fprint(w, "toolong-payload!")
	})
	s := testSession(t, WithDefaultRetries(0))

	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path)
	defer fw.Close()
	err := s.Get(context.Background(), ts.URL, fw)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrIncompleteResponse)
	data, _ := os.ReadFile(path)
	assert.Equal(t, "toolong-pay", string(data), "excess bytes must never reach the file")
	assert.Equal(t, 11, len(data))
}

func TestNonCanonicalDefaultHeaders(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t, WithDefaultHeaders(http.Header{
		"x-custom": {"kept"},
		"cookie":   {"a=1"},
	}))
	require.NoError(t, s.Get(context.Background(), ts.URL, nil,
		WithHeader("X-Other", "v"),
		WithCookieHeader("b=2"),
	))
	rec := ts.record(1)
	assert.Equal(t, "kept", rec.Header.Get("X-Custom"))
	assert.Equal(t, "v", rec.Header.Get("X-Other"))
	assert.Equal(t, "a=1; b=2", rec.Header.Get("Cookie"))
}

func TestInvalidHeaderControlCharsFail(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t)
	err := s.Get(context.Background(), ts.URL, nil, WithHeader("X-Ok", "bad\x00value"))
	require.Error(t, err)
	err = s.Get(context.Background(), ts.URL, nil, WithHeader("Bad\x7fKey", "v"))
	require.Error(t, err)
}

func TestSessionOptionLastWins(t *testing.T) {
	s, err := NewSession(
		WithDefaultRetries(-1),
		WithDefaultRetries(2),
		WithDefaultBackoff(5*time.Second, time.Second),
		WithDefaultBackoff(time.Millisecond, 2*time.Millisecond),
		WithBaseURL("ftp://example.com/"),
		WithBaseURL("https://example.com/"),
	)
	require.NoError(t, err)
	defer s.Close()
	assert.Equal(t, 2, s.cfg.maxRetries)
	assert.Equal(t, "https://example.com/", s.cfg.baseURL.String())
}

func TestStatusErrorAttemptsVisibleInPolicy(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	s := testSession(t, WithDefaultRetries(2))

	var attempts []int
	err := s.Get(context.Background(), ts.URL, nil,
		WithRetryPolicy(func(ctx context.Context, attempt int, ri *ResponseInfo, err error) bool {
			var se *StatusError
			if e, ok := err.(*StatusError); ok {
				_ = e
			}
			// errors.As works because StatusError is wrapped with %w only in wrapFinal;
			// inside the loop the error is the *StatusError itself.
			require.ErrorAs(t, err, &se)
			attempts = append(attempts, se.Attempts)
			return DefaultRetryPolicy(ctx, attempt, ri, err)
		}))
	require.Error(t, err)
	assert.Equal(t, []int{1, 2, 3}, attempts)
}

func TestFileWriterSeekPastEOFHonored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path)
	defer fw.Close()

	_, err := fw.Seek(10, io.SeekStart)
	require.NoError(t, err)
	_, err = fw.Write([]byte("x"))
	require.NoError(t, err)
	st, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, int64(11), st.Size())
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, byte('x'), data[10])
}

func TestProgressWriterSeekResetTruncate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))
	fw := NewFileWriter(path)
	defer fw.Close()
	pw := NewProgressWriter(fw, time.Millisecond, nil)

	pos, err := pw.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(5), pos)

	_, err = pw.Seek(0, io.SeekStart)
	require.NoError(t, err)
	pos, err = pw.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(0), pos)

	_, err = pw.Write([]byte("hi"))
	require.NoError(t, err)
	require.NoError(t, pw.Reset())
	pos, err = pw.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(0), pos)

	// Truncate on non-truncating underlying fails cleanly.
	bpw := NewProgressWriter(bytes.NewBufferString("x"), time.Millisecond, nil)
	assert.Error(t, bpw.Truncate(0))
	_, err = bpw.Seek(0, io.SeekStart)
	assert.Error(t, err)
}

func TestProgressWriterBufferReset(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteString("abc")
	pw := NewProgressWriter(&buf, time.Millisecond, nil)
	require.NoError(t, pw.Reset())
	assert.Equal(t, 0, buf.Len())
}

func TestFileWriterSyncAndSyncOption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path, WithFileSync(true), WithFilePerm(0o600))
	defer fw.Close()
	_, err := fw.Write([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, fw.Sync())
	require.NoError(t, fw.Reset())
}

func TestRestoreWriterVarious(t *testing.T) {
	// *os.File truncating restore.
	path := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.Seek(5, io.SeekStart)
	require.NoError(t, err)
	_, err = f.Write([]byte(" world"))
	require.NoError(t, err)
	require.NoError(t, restoreWriterToInitial(f, 5, true, 0, false))
	pos, err := f.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(5), pos)
	require.NoError(t, f.Close())

	// Generic seeker restore.
	var buf bytes.Buffer
	pw := NewProgressWriter(&buf, time.Millisecond, nil)
	assert.True(t, canRestoreWriter(pw))
	assert.True(t, canRestoreWriter(nil))
	assert.True(t, canRestoreWriter(io.Discard))
	assert.False(t, canRestoreWriter(struct{ io.Writer }{bytes.NewBuffer(nil)}))
}

// nonRewindableWriter accepts writes but offers no rewind mechanism: no
// Seek, Reset, Truncate, nor *bytes.Buffer semantics.
type nonRewindableWriter struct {
	buf []byte
}

func (w *nonRewindableWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

// errSeekerWriter implements io.Seeker statically but fails at runtime,
// like pipes/FIFOs where Seek(0, io.SeekCurrent) errors.
type errSeekerWriter struct {
	buf []byte
}

func (w *errSeekerWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func (w *errSeekerWriter) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("seek not supported")
}

// resetOnlyWriter can discard contents via Reset but does not implement
// io.Seeker.
type resetOnlyWriter struct {
	buf    []byte
	resets int
}

func (w *resetOnlyWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func (w *resetOnlyWriter) Reset() error {
	w.resets++
	w.buf = nil
	return nil
}

func TestCanRestoreWriterProgressWrappedNonRewindable(t *testing.T) {
	nr := &nonRewindableWriter{}
	assert.False(t, canRestoreWriter(nr))
	pw := NewProgressWriter(nr, time.Millisecond, nil)
	assert.False(t, canRestoreWriter(pw), "non-rewindable wrapped in ProgressWriter must not be restorable")
	// Doubly wrapped stays unrestorable.
	pw2 := NewProgressWriter(pw, time.Millisecond, nil)
	assert.False(t, canRestoreWriter(pw2))
	// A failing seeker is not restorable either, bare or wrapped.
	fs := &errSeekerWriter{}
	assert.False(t, canRestoreWriter(fs))
	assert.False(t, canRestoreWriter(NewProgressWriter(fs, time.Millisecond, nil)))
	// Sanity: buffers (bare or wrapped) stay restorable.
	var buf bytes.Buffer
	assert.True(t, canRestoreWriter(NewProgressWriter(&buf, time.Millisecond, nil)))
	assert.True(t, canRestoreWriter(NewProgressWriter(
		NewProgressWriter(&buf, time.Millisecond, nil), time.Millisecond, nil)))
}

func TestNonRewindableWithProgressFailsFast(t *testing.T) {
	ts := truncatedServer(t, fullBody)
	s := testSession(t, WithDefaultRetries(1))

	w := &nonRewindableWriter{}
	err := s.Get(context.Background(), ts.URL, w,
		WithProgress(func(ProgressStats) {}),
		WithProgressInterval(time.Millisecond),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-rewindable")
	assert.Equal(t, 1, ts.count(), "must fail fast without retrying")
	assert.Equal(t, fullBody[:len(fullBody)-1], string(w.buf))
}

func TestRetryReseterWriterReset(t *testing.T) {
	ts := truncatedServer(t, fullBody)
	s := testSession(t, WithDefaultRetries(1))

	w := &resetOnlyWriter{}
	require.NoError(t, s.Get(context.Background(), ts.URL, w))
	assert.Equal(t, fullBody, string(w.buf), "retry must Reset, not append a second copy")
	assert.Equal(t, 2, ts.count())
	assert.Equal(t, 1, w.resets, "Reset must be called once before retry")
}

func TestRetryCallerProgressWriterBuffer(t *testing.T) {
	ts := truncatedServer(t, fullBody)
	s := testSession(t, WithDefaultRetries(1))

	var buf bytes.Buffer
	callerPW := NewProgressWriter(&buf, time.Millisecond, nil)
	require.NoError(t, s.Get(context.Background(), ts.URL, callerPW,
		WithProgress(func(ProgressStats) {}),
		WithProgressInterval(time.Millisecond),
	))
	assert.Equal(t, fullBody, buf.String(), "stacked ProgressWriters must restore buffer without duplication")
	assert.Equal(t, 2, ts.count())
}
