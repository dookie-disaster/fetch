package fetch

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultProgressInterval(t *testing.T) {
	assert.Equal(t, time.Second, DefaultProgressInterval)
}

func TestProgressWriterPassesDataThrough(t *testing.T) {
	var dst bytes.Buffer
	pw := NewProgressWriter(&dst, time.Hour, nil)

	n, err := pw.Write([]byte("hello "))
	require.NoError(t, err)
	assert.Equal(t, 6, n)
	_, err = pw.Write([]byte("world"))
	require.NoError(t, err)

	assert.Equal(t, "hello world", dst.String())
	assert.Equal(t, int64(11), pw.Written())
}

func TestProgressWriterNilCallback(t *testing.T) {
	var dst bytes.Buffer
	pw := NewProgressWriter(&dst, 0, nil) // zero interval falls back to default
	_, err := pw.Write([]byte("data"))
	require.NoError(t, err)
	pw.Flush()
	assert.Equal(t, "data", dst.String())
}

func TestProgressWriterIntervalCallbacks(t *testing.T) {
	var dst bytes.Buffer

	var mu sync.Mutex
	var stats []ProgressStats
	pw := NewProgressWriter(&dst, 25*time.Millisecond, func(st ProgressStats) {
		mu.Lock()
		stats = append(stats, st)
		mu.Unlock()
	})

	go func() {
		for i := 0; i < 10; i++ {
			pw.Write([]byte("0123456789")) // 10 bytes per write
			time.Sleep(10 * time.Millisecond)
		}
		pw.Flush()
	}()

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		done := len(stats) > 0 && stats[len(stats)-1].Written == 100
		mu.Unlock()
		if done {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for final progress callback")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, stats)

	// Interval-driven callbacks fired at least twice before the final flush.
	require.GreaterOrEqual(t, len(stats), 3)

	// Written is monotonically increasing and ends at the total.
	for i, st := range stats {
		assert.GreaterOrEqual(t, st.Written, int64(0), "stats[%d]", i)
		if i > 0 {
			assert.GreaterOrEqual(t, st.Written, stats[i-1].Written, "stats[%d]", i)
			assert.Greater(t, st.BytesPerSec, 0.0, "throughput should be positive, stats[%d]", i)
		}
	}
	assert.Equal(t, int64(100), stats[len(stats)-1].Written)
	assert.Greater(t, stats[len(stats)-1].Elapsed, time.Duration(0))
}

func TestProgressWriterFlushDeliversFinalStats(t *testing.T) {
	var dst bytes.Buffer

	called := 0
	var final ProgressStats
	pw := NewProgressWriter(&dst, time.Hour, func(st ProgressStats) { // interval never reached
		called++
		final = st
	})

	_, err := pw.Write([]byte("12345"))
	require.NoError(t, err)
	assert.Equal(t, 0, called, "no callback before the interval elapses")

	pw.Flush()
	assert.Equal(t, 1, called)
	assert.Equal(t, int64(5), final.Written)

	// A second Flush with no new data is a no-op.
	pw.Flush()
	assert.Equal(t, 1, called)
}

func TestProgressWriterSetTotal(t *testing.T) {
	var dst bytes.Buffer
	var got ProgressStats
	pw := NewProgressWriter(&dst, time.Hour, func(st ProgressStats) { got = st })
	pw.SetTotal(1000)

	_, err := pw.Write(make([]byte, 100))
	require.NoError(t, err)
	pw.Flush()

	assert.Equal(t, int64(1000), got.Total)
	assert.Equal(t, int64(100), got.Written)
}

func TestProgressWriterFlushWithoutData(t *testing.T) {
	called := 0
	pw := NewProgressWriter(&bytes.Buffer{}, time.Millisecond, func(ProgressStats) { called++ })
	pw.Flush()
	assert.Equal(t, 0, called, "nothing written, nothing to report")
}

func TestProgressThroughSession(t *testing.T) {
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		w.Write(payload)
	})
	s := testSession(t)

	var mu sync.Mutex
	var last ProgressStats
	var buf bytes.Buffer
	err := s.Get(context.Background(), ts.URL, &buf,
		WithProgress(func(st ProgressStats) {
			mu.Lock()
			last = st
			mu.Unlock()
		}),
		WithProgressInterval(10*time.Millisecond),
	)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, len(payload), buf.Len())
	assert.Equal(t, int64(len(payload)), last.Written)
	assert.Equal(t, int64(len(payload)), last.Total)
	assert.GreaterOrEqual(t, last.BytesPerSec, 0.0)
}

func TestProgressChunkedResponse(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		// Chunked response: no Content-Length header on the wire.
		for i := 0; i < 3; i++ {
			fmt.Fprint(w, "chunk")
		}
	})
	s := testSession(t)

	var got ProgressStats
	var buf bytes.Buffer
	err := s.Get(context.Background(), ts.URL, &buf,
		WithProgress(func(st ProgressStats) { got = st }),
		WithProgressInterval(time.Millisecond),
	)
	require.NoError(t, err)
	// httpcloak computes the body size even without a Content-Length
	// header, so the total is still known.
	assert.Equal(t, int64(15), got.Total)
	assert.Equal(t, int64(15), got.Written)
	assert.Equal(t, 15, buf.Len())
}
