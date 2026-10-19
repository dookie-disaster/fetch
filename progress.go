package fetch

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"
)

// ProgressStats is a snapshot of download progress, delivered to the callback
// registered with WithProgress.
type ProgressStats struct {
	// Written is the number of bytes of the complete resource received so
	// far. For a fresh download it equals the bytes written in this Fetch.
	// For a 206 resume it includes bytes already present on the writer
	// before the fetch, so a completed resume ends with Written == Total
	// (e.g. 100/100). BytesPerSec still measures only newly transferred
	// bytes.
	Written int64
	// Total is the expected size of the complete resource. For a 200
	// response it is the response Content-Length; for a 206 resume it is
	// the full file size from Content-Range (falling back to
	// resumeOffset+ContentLength); -1 when unknown (e.g. transparently
	// decompressed responses whose wire Content-Length describes the
	// compressed bytes).
	Total int64
	// BytesPerSec is the throughput since the previous callback.
	BytesPerSec float64
	// Elapsed is the time since the first byte was written.
	Elapsed time.Duration
}

// ProgressFunc receives periodic ProgressStats while a response body is
// being copied. It runs on the goroutine performing the copy and should
// return quickly; long-running work should be handed off to another
// goroutine.
type ProgressFunc func(ProgressStats)

// ProgressWriter is an io.Writer that reports throughput statistics by
// invoking a callback at a configured interval as data is written through it.
// It wraps the real destination writer, so it composes with FileWriter,
// buffers, network sinks, and so on.
type ProgressWriter struct {
	w        io.Writer
	interval time.Duration
	fn       ProgressFunc

	mu          sync.Mutex
	total       int64
	written     int64
	lastWritten int64
	started     bool
	start       time.Time
	last        time.Time
}

// NewProgressWriter wraps w and calls fn at most once per interval as data
// is written through. An interval of zero or less defaults to
// DefaultProgressInterval (one second). fn may be nil, in which case the
// writer only passes data through. A nil w writes to io.Discard.
func NewProgressWriter(w io.Writer, interval time.Duration, fn ProgressFunc) *ProgressWriter {
	if interval <= 0 {
		interval = DefaultProgressInterval
	}
	if w == nil {
		w = io.Discard
	}
	return &ProgressWriter{w: w, interval: interval, fn: fn, total: -1}
}

// SetTotal sets the expected total size reported in ProgressStats, e.g. the
// Content-Length of the response. -1 means unknown.
func (pw *ProgressWriter) SetTotal(n int64) {
	pw.mu.Lock()
	pw.total = n
	pw.mu.Unlock()
	if u, ok := pw.w.(*ProgressWriter); ok && u != nil {
		u.SetTotal(n)
	}
}

// SetWritten sets the starting byte count, used to position progress at the
// beginning of an attempt within a single Fetch. For a 206 resume it is set
// to the resume offset so Written reflects full-file progress; for a 200
// response (including the fallback that truncates after an ignored Range)
// it is reset to 0. lastWritten is set alongside so BytesPerSec measures
// only newly transferred bytes.
func (pw *ProgressWriter) SetWritten(n int64) {
	pw.mu.Lock()
	pw.written = n
	pw.lastWritten = n
	pw.mu.Unlock()
	if u, ok := pw.w.(*ProgressWriter); ok && u != nil {
		u.SetWritten(n)
	}
}

// Written returns the number of bytes written through so far.
func (pw *ProgressWriter) Written() int64 {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	return pw.written
}

// Write implements io.Writer, forwarding to the wrapped writer and firing the
// progress callback when the configured interval has elapsed. A short write
// without an error is reported as io.ErrShortWrite.
func (pw *ProgressWriter) Write(p []byte) (int, error) {
	n, err := pw.w.Write(p)
	if n > 0 {
		pw.report(int64(n))
	}
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}

// Flush invokes the callback immediately if data has been written since the
// last callback, delivering a final snapshot. It is a no-op when no data has
// been written or no callback is configured.
func (pw *ProgressWriter) Flush() {
	pw.mu.Lock()
	now := time.Now()
	fire := pw.fn != nil && pw.started && pw.written != pw.lastWritten
	var stats ProgressStats
	if fire {
		stats = pw.snapshotLocked(now)
	}
	pw.mu.Unlock()
	if fire {
		pw.fn(stats)
	}
	if u, ok := pw.w.(*ProgressWriter); ok && u != nil {
		u.Flush()
	}
}

// Unwrap returns the underlying destination writer.
func (pw *ProgressWriter) Unwrap() io.Writer { return pw.w }

// Seek forwards to the underlying writer when it implements io.Seeker,
// so a ProgressWriter wrapping a *FileWriter (or *os.File) keeps working
// with WithResume. It reports an error when the underlying writer is not
// seekable. Querying the position via Seek(0, io.SeekCurrent) does not
// affect the progress counters; Fetch repositions progress explicitly via
// SetWritten after resume negotiation.
func (pw *ProgressWriter) Seek(offset int64, whence int) (int64, error) {
	s, ok := pw.w.(io.Seeker)
	if !ok {
		// Unwrap one level: callers may stack wrappers in the future.
		if u, ok := pw.w.(*ProgressWriter); ok {
			return u.Seek(offset, whence)
		}
		return 0, fmt.Errorf("fetch: ProgressWriter: underlying writer does not support seeking")
	}
	return s.Seek(offset, whence)
}

// Reset discards the underlying contents (truncating to zero) and resets
// progress counters to 0. It forwards to Reset() error, *bytes.Buffer, or
// Truncate(0)+Seek(0) where available, falling back to Seek(0) for plain
// seekers.
func (pw *ProgressWriter) Reset() error {
	if r, ok := pw.w.(interface{ Reset() error }); ok {
		if err := r.Reset(); err != nil {
			return err
		}
		pw.SetWritten(0)
		return nil
	}
	if b, ok := pw.w.(*bytes.Buffer); ok {
		b.Reset()
		pw.SetWritten(0)
		return nil
	}
	if u, ok := pw.w.(*ProgressWriter); ok {
		if err := u.Reset(); err != nil {
			return err
		}
		pw.SetWritten(0)
		return nil
	}
	if t, ok := pw.w.(interface{ Truncate(int64) error }); ok {
		if err := t.Truncate(0); err != nil {
			return err
		}
		if s, ok := pw.w.(io.Seeker); ok {
			if _, err := s.Seek(0, io.SeekStart); err != nil {
				return err
			}
		}
		pw.SetWritten(0)
		return nil
	}
	if s, ok := pw.w.(io.Seeker); ok {
		if _, err := s.Seek(0, io.SeekStart); err != nil {
			return err
		}
		pw.SetWritten(0)
		return nil
	}
	pw.SetWritten(0)
	return nil
}

// Truncate forwards Truncate(n) to the underlying writer when supported.
// Progress counters are left for the caller to reposition via SetWritten;
// Reset (n == 0) resets them to 0 for convenience.
func (pw *ProgressWriter) Truncate(n int64) error {
	if t, ok := pw.w.(interface{ Truncate(int64) error }); ok {
		if err := t.Truncate(n); err != nil {
			return err
		}
		if n == 0 {
			pw.SetWritten(0)
		}
		return nil
	}
	if u, ok := pw.w.(*ProgressWriter); ok {
		return u.Truncate(n)
	}
	return fmt.Errorf("fetch: ProgressWriter: underlying writer does not support truncate")
}

func (pw *ProgressWriter) report(n int64) {
	pw.mu.Lock()
	now := time.Now()
	if !pw.started {
		pw.started = true
		pw.start = now
		pw.last = now
	}
	pw.written += n
	fire := false
	var stats ProgressStats
	if pw.fn != nil && now.Sub(pw.last) >= pw.interval {
		stats = pw.snapshotLocked(now)
		fire = true
	}
	pw.mu.Unlock()
	if fire {
		pw.fn(stats)
	}
}

// snapshotLocked computes stats and advances the reporting window. It must
// be called with pw.mu held.
func (pw *ProgressWriter) snapshotLocked(now time.Time) ProgressStats {
	dt := now.Sub(pw.last)
	bps := 0.0
	if dt > 0 {
		bps = float64(pw.written-pw.lastWritten) / dt.Seconds()
	}
	stats := ProgressStats{
		Written:     pw.written,
		Total:       pw.total,
		BytesPerSec: bps,
		Elapsed:     now.Sub(pw.start),
	}
	pw.last = now
	pw.lastWritten = pw.written
	return stats
}
