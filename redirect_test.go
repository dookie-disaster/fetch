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

func TestRedirectFollowed(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/redirect":
			http.Redirect(w, r, "/target", http.StatusFound)
		case "/target":
			fmt.Fprint(w, "landed")
		default:
			http.NotFound(w, r)
		}
	})
	s := testSession(t)

	var buf bytes.Buffer
	var finalURL string
	require.NoError(t, s.Get(context.Background(), ts.URL+"/redirect", &buf,
		WithOnResponse(func(ri *ResponseInfo) { finalURL = ri.FinalURL })))

	assert.Equal(t, "landed", buf.String())
	assert.Contains(t, finalURL, "/target")
	assert.Equal(t, 2, ts.count())
}

func TestRedirectRelativeLocation(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/a/b/c":
			// Relative location resolves against the current path.
			w.Header().Set("Location", "../d")
			w.WriteHeader(http.StatusFound)
		case "/a/d":
			fmt.Fprint(w, "relative-ok")
		default:
			http.NotFound(w, r)
		}
	})
	s := testSession(t)

	var buf bytes.Buffer
	require.NoError(t, s.Get(context.Background(), ts.URL+"/a/b/c", &buf))
	assert.Equal(t, "relative-ok", buf.String())
}

func TestRedirectRewritesPostToGet(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/redirect":
			http.Redirect(w, r, "/target", http.StatusMovedPermanently)
		case "/target":
			fmt.Fprint(w, r.Method)
		}
	})
	s := testSession(t)

	var buf bytes.Buffer
	require.NoError(t, s.Post(context.Background(), ts.URL+"/redirect", bytes.NewBufferString("payload"), &buf))

	assert.Equal(t, "GET", buf.String(), "301 rewrites POST to GET")
	rec := ts.record(2)
	assert.Equal(t, "GET", rec.Method)
	assert.Empty(t, rec.Body, "the body is dropped on the GET rewrite")
}

func TestRedirect307PreservesMethodAndBody(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/target" {
			fmt.Fprint(w, r.Method)
		} else {
			http.Redirect(w, r, "/target", http.StatusTemporaryRedirect)
		}
	})
	s := testSession(t)

	var buf bytes.Buffer
	require.NoError(t, s.Post(context.Background(), ts.URL+"/redirect", bytes.NewBufferString("payload"), &buf))

	assert.Equal(t, "POST", buf.String(), "307 keeps the method")
	rec := ts.record(2)
	assert.Equal(t, "POST", rec.Method)
	assert.Equal(t, "payload", string(rec.Body), "307 replays the body")
}

func TestRedirect307ReplaysOneShotBody(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/target" {
			fmt.Fprint(w, "ok")
		} else {
			http.Redirect(w, r, "/target", http.StatusPermanentRedirect)
		}
	})
	s := testSession(t)

	err := s.Post(context.Background(), ts.URL+"/redirect", &oneShotReader{data: "x"}, &bytes.Buffer{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrBodyNotReplayable)
}

func TestTooManyRedirects(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/loop", http.StatusFound)
	})
	s := testSession(t)

	err := s.Get(context.Background(), ts.URL+"/loop", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTooManyRedirects)
	// The initial request plus maxRedirects hops.
	assert.Equal(t, DefaultMaxRedirects+1, ts.count())
}

func TestRedirectChainThenError(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		switch n {
		case 1:
			http.Redirect(w, r, "/hop2", http.StatusFound)
		case 2:
			http.Redirect(w, r, "/hop3", http.StatusFound)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	s := testSession(t)

	err := s.Get(context.Background(), ts.URL, nil)
	require.Error(t, err)

	var se *StatusError
	require.ErrorAs(t, err, &se)
	assert.Equal(t, http.StatusNotFound, se.StatusCode)
	assert.Contains(t, se.URL, "/hop3", "the error reports the URL of the final response")
}

func TestRedirectWithResume(t *testing.T) {
	// Resume state must survive a redirect: the range is recomputed for the
	// redirect target.
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file":
			http.Redirect(w, r, "/cdn/file", http.StatusFound)
		case "/cdn/file":
			if rng := r.Header.Get("Range"); rng == "bytes=5-" {
				w.Header().Set("Content-Range", "bytes 5-10/11")
				w.Header().Set("Content-Length", "6")
				w.WriteHeader(http.StatusPartialContent)
				fmt.Fprint(w, " world")
				return
			}
			w.Header().Set("Content-Length", "11")
			fmt.Fprint(w, fullBody)
		}
	})
	s := testSession(t)

	path := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644)) // 5 bytes done

	fw := NewFileWriter(path)
	defer fw.Close()

	require.NoError(t, s.Get(context.Background(), ts.URL+"/file", fw, WithResume(true)))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, fullBody, string(data))
	assert.Equal(t, "bytes=5-", ts.record(2).Header.Get("Range"), "range recomputed for the redirect target")
}
