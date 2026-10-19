package fetch

import (
	"bytes"
	"io"
	"strconv"
	"strings"
)

// seekerPos reports the current seek position of w when w is an io.Seeker
// whose position can be determined. This is how much valid data the writer
// already holds, and therefore the offset a resume should continue from.
// A *ProgressWriter wrapping a seeker forwards Seek, so resume keeps
// working through the progress wrapper.
func seekerPos(w io.Writer) (int64, bool) {
	s, ok := w.(io.Seeker)
	if !ok {
		return 0, false
	}
	pos, err := s.Seek(0, io.SeekCurrent)
	if err != nil || pos < 0 {
		return 0, false
	}
	return pos, true
}

// unwrapWriter unwraps all *ProgressWriter layers, returning the underlying
// writer. It stops if w is not a *ProgressWriter or is a nil *ProgressWriter.
func unwrapWriter(w io.Writer) io.Writer {
	for {
		pw, ok := w.(*ProgressWriter)
		if !ok || pw == nil {
			return w
		}
		w = pw.Unwrap()
	}
}

// bufferLen reports the length of w when w is (or wraps) a *bytes.Buffer.
func bufferLen(w io.Writer) (int, bool) {
	w = unwrapWriter(w)
	if b, ok := w.(*bytes.Buffer); ok && b != nil {
		return b.Len(), true
	}
	return 0, false
}

// isDiscard reports whether w discards all output (nil or io.Discard,
// possibly wrapped in a ProgressWriter). Retrying after a partial write is
// safe for discarded output.
func isDiscard(w io.Writer) bool {
	w = unwrapWriter(w)
	if w == nil {
		return true
	}
	if w == io.Discard {
		return true
	}
	return false
}

// reseter is implemented by writers that can discard their contents. It is
// preferred over a plain seek-to-start because a rewind without a truncate
// would leave stale bytes at the end of the file.
type reseter interface {
	Reset() error
}

// truncater is implemented by *os.File and similar writers that can truncate
// without a full Reset method.
type truncater interface {
	Truncate(int64) error
}

// resetWriter rewinds w to the start, discarding any data it holds, so a
// body the server re-sent in full (after ignoring a range request) can be
// written from scratch. Writers with Reset use it (truncating); *bytes.Buffer
// is truncated via its own Reset/Truncate; writers with Truncate(0) truncate
// then seek; plain seekers fall back to Seek(0) and the caller must ensure
// the new body is not shorter than the stale prefix (FileWriter and
// truncating files are safe; other seekers may retain a stale tail).
// A *ProgressWriter forwards to its underlying writer and resets its own
// counters to 0.
func resetWriter(w io.Writer) error {
	if pw, ok := w.(*ProgressWriter); ok {
		return pw.Reset()
	}
	if r, ok := w.(reseter); ok {
		return r.Reset()
	}
	if b, ok := w.(*bytes.Buffer); ok {
		b.Reset()
		return nil
	}
	if t, ok := w.(truncater); ok {
		if err := t.Truncate(0); err != nil {
			return err
		}
		if s, ok := w.(io.Seeker); ok {
			_, err := s.Seek(0, io.SeekStart)
			return err
		}
		return nil
	}
	if s, ok := w.(io.Seeker); ok {
		_, err := s.Seek(0, io.SeekStart)
		return err
	}
	return nil
}

// restoreWriterToInitial rewinds w to the state captured before Fetch
// started, discarding session bytes written by a failed attempt so a retry
// starts clean. initPos/initSeekOK come from seekerPos(w) before the first
// attempt; initBufLen/initBufOK come from bufferLen(w).
func restoreWriterToInitial(w io.Writer, initPos int64, initSeekOK bool, initBufLen int, initBufOK bool) error {
	// Reset progress counters on all *ProgressWriter wrappers.
	for tmp := w; ; {
		pw, ok := tmp.(*ProgressWriter)
		if !ok || pw == nil {
			break
		}
		pw.SetWritten(0)
		tmp = pw.Unwrap()
	}
	under := unwrapWriter(w)
	if under == nil {
		return nil
	}
	if pw, ok := under.(*ProgressWriter); ok && pw == nil {
		return nil
	}
	if under == io.Discard {
		return nil
	}
	// Buffers restore by length, independent of seeking.
	if initBufOK {
		if b, ok := under.(*bytes.Buffer); ok && b != nil {
			b.Truncate(initBufLen)
		}
		return nil
	}
	if initSeekOK {
		// Seekable writers: truncate any partial tail beyond initPos when
		// possible, then seek back.
		if fw, ok := under.(*FileWriter); ok && fw != nil {
			return fw.truncateTo(initPos)
		}
		if t, ok := under.(truncater); ok {
			if err := t.Truncate(initPos); err != nil {
				return err
			}
			if s, ok := under.(io.Seeker); ok {
				_, err := s.Seek(initPos, io.SeekStart)
				return err
			}
			return nil
		}
		if s, ok := under.(io.Seeker); ok {
			_, err := s.Seek(initPos, io.SeekStart)
			return err
		}
		return nil
	}
	// Non-seekable, non-buffer writers: reset truncating writers so a retry
	// does not append onto partial bytes.
	if r, ok := under.(reseter); ok {
		return r.Reset()
	}
	if t, ok := under.(truncater); ok {
		return t.Truncate(0)
	}
	return nil
}

// canRestoreWriter reports whether w can be rewound to its initial state
// after a partial write. Seekers, *bytes.Buffer, resetters, truncaters and
// discarded output are restorable; any other streaming writer is not, and a
// retry after partial bytes would duplicate data.
func canRestoreWriter(w io.Writer) bool {
	w = unwrapWriter(w)
	if w == nil {
		return true
	}
	if pw, ok := w.(*ProgressWriter); ok && pw == nil {
		return true
	}
	if isDiscard(w) {
		return true
	}
	if _, ok := bufferLen(w); ok {
		return true
	}
	if _, ok := seekerPos(w); ok {
		return true
	}
	if _, ok := w.(reseter); ok {
		return true
	}
	if _, ok := w.(truncater); ok {
		return true
	}
	if b, ok := w.(*bytes.Buffer); ok && b != nil {
		return true
	}
	return false
}

// isDecompressedByClient reports whether httpcloak transparently
// decompresses the given Content-Encoding on the streaming path (see
// transport.setupStreamDecompressor). Only the single-token forms are
// decoded; anything else arrives as raw bytes.
func isDecompressedByClient(contentEncoding string) bool {
	switch strings.ToLower(strings.TrimSpace(contentEncoding)) {
	case "gzip", "br", "deflate", "zstd":
		return true
	}
	return false
}

// parseContentRange parses a Content-Range header value of the form
// "bytes <start>-<end>/<total>" (e.g. "bytes 5-10/11"). A total of "*" is
// reported as -1. The unit is required and case-insensitive; end must parse
// and satisfy end >= start, and total (unless "*") must be >= 0 and
// consistent (end < total when both are known).
func parseContentRange(v string) (start, end, total int64, ok bool) {
	v = strings.TrimSpace(v)
	i := strings.IndexByte(v, ' ')
	if i < 0 {
		return 0, 0, -1, false
	}
	if unit := strings.ToLower(strings.TrimSpace(v[:i])); unit != "bytes" {
		return 0, 0, -1, false
	}
	v = strings.TrimSpace(v[i+1:])
	spec, totalPart, found := strings.Cut(v, "/")
	if !found {
		return 0, 0, -1, false
	}
	if t := strings.TrimSpace(totalPart); t == "*" {
		total = -1
	} else {
		n, err := strconv.ParseInt(t, 10, 64)
		if err != nil || n < 0 {
			return 0, 0, -1, false
		}
		total = n
	}
	rangePart := strings.TrimSpace(spec)
	dash := strings.IndexByte(rangePart, '-')
	if dash < 0 {
		return 0, 0, total, false
	}
	s, err := strconv.ParseInt(strings.TrimSpace(rangePart[:dash]), 10, 64)
	if err != nil || s < 0 {
		return 0, 0, total, false
	}
	e, err := strconv.ParseInt(strings.TrimSpace(rangePart[dash+1:]), 10, 64)
	if err != nil || e < 0 || e < s {
		return 0, 0, total, false
	}
	if total >= 0 && e >= total {
		return 0, 0, total, false
	}
	return s, e, total, true
}


