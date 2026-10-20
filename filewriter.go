package fetch

import (
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"sync"
)

// FileOption configures a FileWriter.
type FileOption func(*FileWriter)

// WithFilePerm sets the permissions used when the FileWriter creates its
// file. Defaults to 0644. Only the low 9 permission bits are used; other
// mode bits are masked off.
func WithFilePerm(perm os.FileMode) FileOption {
	return func(fw *FileWriter) {
		if perm == 0 {
			return
		}
		fw.perm = perm & 0o777
		if fw.perm == 0 {
			fw.perm = 0o644
		}
	}
}

// WithFileSync, when true, makes Close and Reset fsync the file before
// returning so downloaded data is durable on disk.
func WithFileSync(enabled bool) FileOption {
	return func(fw *FileWriter) {
		fw.syncOnClose = enabled
	}
}

// WithFileHash makes the writer feed every byte written to h so Sum can
// return a digest of the complete file, including any partial data that is
// already on disk when the digest is first computed. h is reset when the
// option is applied and is owned by the writer; it must not be used
// concurrently. Passing nil is ignored.
func WithFileHash(h hash.Hash) FileOption {
	return func(fw *FileWriter) {
		if h == nil {
			return
		}
		h.Reset()
		fw.hash = h
		fw.hashPos = 0
	}
}

// FileWriter is an io.WriteSeeker that opens its file lazily, on demand,
// when the first write (or seek) happens. It is meant for download workers
// that queue work behind a semaphore: the writer can be created - and carry
// the size of any partial file for auto-resume - long before the download
// actually starts, keeping concurrently open files to a minimum.
//
// If the target path already holds partial data, the writer positions itself
// at the end of that data, so combined with WithResume a retry or a fresh
// session continues the download where it left off.
//
// Individual methods are safe for concurrent use, but a single FileWriter
// must not be shared across concurrent Fetch calls: the resume logic reads
// the position (Seek) and then writes, and that check-then-act sequence is
// not atomic across goroutines. Give each concurrent download its own
// FileWriter.
//
// WithFileHash optionally maintains a digest of the complete file as data is
// written; Sum returns it.
type FileWriter struct {
	mu          sync.Mutex
	path        string
	perm        os.FileMode
	f           *os.File
	pos         int64
	closed      bool
	canceled    bool
	syncOnClose bool
	// seeked tracks whether Seek was called explicitly. ensureOpen only
	// reconciles pos to the on-disk size when pos still came from the initial
	// NewFileWriter stat; an explicit Seek past EOF must be honored so the
	// next write creates a sparse hole instead of being silently rewound.
	seeked bool
	// hash, when non-nil, covers the complete file from offset 0. hashPos is
	// the number of leading file bytes represented by hash; -1 means the hash
	// state is stale and must be rebuilt from disk before it is used again.
	hash    hash.Hash
	hashPos int64
}

// Compile-time check: a FileWriter can be used anywhere the fetch resume
// logic expects a seeking writer.
var _ io.WriteSeeker = (*FileWriter)(nil)

// NewFileWriter creates a writer that will save a request body to path,
// opening the file only when data actually arrives. If path already exists
// and holds partial data, the writer starts at the end of it.
func NewFileWriter(path string, opts ...FileOption) *FileWriter {
	fw := &FileWriter{path: path, perm: 0o644}
	for _, opt := range opts {
		if opt != nil {
			opt(fw)
		}
	}
	if st, err := os.Stat(path); err == nil && st.Mode().IsRegular() {
		fw.pos = st.Size()
	}
	return fw
}

// Path returns the file path the writer saves to.
func (fw *FileWriter) Path() string { return fw.path }

// Pos returns the writer's current position, which for a FileWriter equals
// the number of valid bytes in the file (assuming no out-of-band edits).
func (fw *FileWriter) Pos() int64 {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	return fw.pos
}

// Sum returns the digest of the complete file from offset 0 through the
// writer's current position, including any partial data that was on disk when
// the writer was created. It returns an error when no hash was configured
// with WithFileHash, after Cancel, and when the file is shorter than the
// writer's position: bytes past the end of the file (an unwritten sparse gap
// after Seek past EOF) have no file content to digest yet. Sum may be called
// after Close.
func (fw *FileWriter) Sum() ([]byte, error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if fw.canceled {
		return nil, os.ErrClosed
	}
	if fw.hash == nil {
		return nil, fmt.Errorf("fetch: FileWriter.Sum: no hash configured")
	}
	if fw.pos > 0 {
		st, err := os.Stat(fw.path)
		if err != nil {
			return nil, fmt.Errorf("fetch: FileWriter.Sum: %w", err)
		}
		if st.Size() < fw.pos {
			return nil, fmt.Errorf("fetch: FileWriter.Sum: file is %d bytes, shorter than the writer position %d", st.Size(), fw.pos)
		}
	}
	if err := fw.hashSyncLocked(fw.pos); err != nil {
		return nil, err
	}
	return fw.hash.Sum(nil), nil
}

// hashSyncLocked makes the hash represent exactly the file bytes in
// [0, to). It must be called with fw.mu held. Rehashing from disk is needed
// when a resumed file already has a prefix, after a rewind/truncate, or when a
// sparse gap has to be represented as zero bytes. Zero padding stands in for
// hole bytes that the next write materializes; Sum refuses to report a digest
// while such a gap is still shorter than the writer position on disk.
func (fw *FileWriter) hashSyncLocked(to int64) error {
	if fw.hash == nil || fw.hashPos == to {
		return nil
	}
	if fw.hashPos < 0 || fw.hashPos > to {
		fw.hash.Reset()
		fw.hashPos = 0
	}
	from := fw.hashPos
	if from == to {
		return nil
	}

	f, err := os.Open(fw.path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			fw.hashPos = -1
			return err
		}
		if from > 0 {
			fw.hashPos = -1
			return err
		}
	} else {
		defer f.Close()
		if _, err := f.Seek(from, io.SeekStart); err != nil {
			fw.hashPos = -1
			return err
		}
		n, err := io.CopyN(fw.hash, f, to-from)
		fw.hashPos += n
		if err != nil && !errors.Is(err, io.EOF) {
			fw.hashPos = -1
			return err
		}
	}

	if fw.hashPos < to {
		zeros := make([]byte, 32*1024)
		for fw.hashPos < to {
			n := int64(len(zeros))
			if remaining := to - fw.hashPos; remaining < n {
				n = remaining
			}
			if _, err := fw.hash.Write(zeros[:n]); err != nil {
				fw.hashPos = -1
				return err
			}
			fw.hashPos += n
		}
	}
	return nil
}

// Write implements io.Writer, opening the file on the first call.
// Short writes are looped until all of p is written or an error occurs.
func (fw *FileWriter) Write(p []byte) (int, error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if err := fw.ensureOpen(); err != nil {
		return 0, err
	}
	if err := fw.hashSyncLocked(fw.pos); err != nil {
		return 0, err
	}
	written := 0
	for written < len(p) {
		n, err := fw.f.Write(p[written:])
		written += n
		fw.pos += int64(n)
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	// Feed the hash once with exactly the bytes that landed on disk. On a
	// hash failure the state is marked stale (hashPos -1) and rebuilt from
	// disk before it is used again; the write itself still succeeds.
	if written > 0 && fw.hash != nil {
		if _, herr := fw.hash.Write(p[:written]); herr != nil {
			fw.hashPos = -1
		} else {
			fw.hashPos += int64(written)
		}
	}
	return written, nil
}

// Seek implements io.Seeker. Seeking before the file is open answers from the
// on-disk state without opening it; the file is opened and positioned
// accordingly on the next write. Seeking after Close or Cancel fails with
// os.ErrClosed and leaves the position unchanged.
func (fw *FileWriter) Seek(offset int64, whence int) (int64, error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if fw.closed {
		return fw.pos, os.ErrClosed
	}

	var np int64
	switch whence {
	case io.SeekStart:
		np = offset
	case io.SeekCurrent:
		np = fw.pos + offset
	case io.SeekEnd:
		size := fw.pos
		if fw.f != nil {
			if st, err := fw.f.Stat(); err == nil {
				size = st.Size()
			}
		} else if st, err := os.Stat(fw.path); err == nil {
			if st.Mode().IsRegular() {
				size = st.Size()
			}
		} else if errors.Is(err, fs.ErrNotExist) && !fw.seeked {
			size = 0
		}
		np = size + offset
	default:
		return fw.pos, fmt.Errorf("fetch: FileWriter.Seek: invalid whence %d", whence)
	}
	if np < 0 {
		return fw.pos, fmt.Errorf("fetch: FileWriter.Seek: negative position %d", np)
	}
	if fw.f != nil {
		if _, err := fw.f.Seek(np, io.SeekStart); err != nil {
			return fw.pos, err
		}
	}
	fw.pos = np
	if fw.hash != nil && np < fw.hashPos {
		fw.hashPos = -1
	}
	if !(whence == io.SeekCurrent && offset == 0) {
		fw.seeked = true
	}
	return np, nil
}

// Sync flushes buffered data to stable storage.
func (fw *FileWriter) Sync() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if fw.closed {
		return os.ErrClosed
	}
	if fw.f == nil {
		return nil
	}
	return fw.f.Sync()
}

// Close releases the file handle if one is open. The file itself is left on
// disk. The writer cannot be used for further writes or seeks after Close.
// When WithFileSync(true) was set, the file is fsynced first.
func (fw *FileWriter) Close() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if fw.closed {
		return nil
	}
	fw.closed = true
	if fw.f == nil {
		return nil
	}
	var syncErr error
	if fw.syncOnClose {
		syncErr = fw.f.Sync()
	}
	err := fw.f.Close()
	fw.f = nil
	if syncErr != nil {
		return syncErr
	}
	return err
}

// Reset implements the interface the fetch resume logic uses to discard
// partial data when a server ignores a range request: it truncates the file
// back to zero bytes.
func (fw *FileWriter) Reset() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	return fw.resetLocked()
}

func (fw *FileWriter) resetLocked() error {
	if fw.closed {
		return os.ErrClosed
	}
	if fw.f != nil {
		if err := fw.f.Truncate(0); err != nil {
			return err
		}
		if _, err := fw.f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if fw.syncOnClose {
			if err := fw.f.Sync(); err != nil {
				return err
			}
		}
	} else if fw.path != "" {
		if err := os.Truncate(fw.path, 0); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	fw.pos = 0
	if fw.hash != nil {
		fw.hash.Reset()
		fw.hashPos = 0
	}
	fw.seeked = true
	return nil
}

// Truncate discards session bytes beyond pos, restoring the file to the
// size it had before a failed attempt. pos == 0 is equivalent to Reset.
func (fw *FileWriter) Truncate(pos int64) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	return fw.truncateLocked(pos)
}

// truncateTo discards session bytes beyond pos, restoring the file to the
// size it had before a failed attempt. pos == 0 is equivalent to Reset.
func (fw *FileWriter) truncateTo(pos int64) error {
	return fw.Truncate(pos)
}

func (fw *FileWriter) truncateLocked(pos int64) error {
	if fw.closed {
		return os.ErrClosed
	}
	if pos < 0 {
		return fmt.Errorf("fetch: FileWriter: negative truncate position %d", pos)
	}
	if pos == 0 {
		return fw.resetLocked()
	}
	if fw.f != nil {
		if err := fw.f.Truncate(pos); err != nil {
			return err
		}
		if _, err := fw.f.Seek(pos, io.SeekStart); err != nil {
			return err
		}
		if fw.syncOnClose {
			if err := fw.f.Sync(); err != nil {
				return err
			}
		}
	} else {
		// Unopened: shrink the on-disk file when it grew past pos.
		if st, err := os.Stat(fw.path); err == nil && st.Size() > pos {
			if err := os.Truncate(fw.path, pos); err != nil {
				return err
			}
		}
	}
	fw.pos = pos
	if fw.hash != nil && pos < fw.hashPos {
		fw.hashPos = -1
	}
	fw.seeked = true
	return nil
}

// Cancel aborts the download: the file handle is closed and the file,
// including any partial data, is removed from disk. Cancel is idempotent.
// Cancel after a successful Close fails with os.ErrClosed and leaves the
// file in place, since Close promised to keep it.
func (fw *FileWriter) Cancel() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if fw.canceled {
		return nil
	}
	if fw.closed {
		return os.ErrClosed
	}
	fw.closed = true
	fw.canceled = true
	if fw.f != nil {
		fw.f.Close()
		fw.f = nil
	}
	fw.pos = 0
	if fw.hash != nil {
		fw.hash.Reset()
		fw.hashPos = 0
	}
	err := os.Remove(fw.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// prepare reconciles the implicit position with the on-disk size and pins it
// before the first write. Fetch calls it before reading the position for a
// resume range, so the range header and the eventual write start at the same
// offset even if the file changed after NewFileWriter. An explicit Seek or an
// already-open file is authoritative and left untouched.
func (fw *FileWriter) prepare() error {
	if fw == nil {
		return nil
	}
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if fw.closed {
		return os.ErrClosed
	}
	if fw.f != nil || fw.seeked {
		return nil
	}
	if st, err := os.Stat(fw.path); err == nil {
		if st.Mode().IsRegular() {
			fw.pos = st.Size()
		}
	} else if errors.Is(err, fs.ErrNotExist) {
		fw.pos = 0
	}
	fw.seeked = true
	return nil
}

// ensureOpen opens the file and positions it at the writer's logical
// position. It must be called with fw.mu held.
func (fw *FileWriter) ensureOpen() error {
	if fw.closed {
		return os.ErrClosed
	}
	if fw.path == "" {
		return fmt.Errorf("fetch: FileWriter: empty path")
	}
	if fw.f != nil {
		return nil
	}
	f, err := os.OpenFile(fw.path, os.O_CREATE|os.O_WRONLY, fw.perm)
	if err != nil {
		return err
	}
	// Reconcile with on-disk state to avoid sparse holes or stale positions
	// when the file changed between NewFileWriter and the first write. An
	// explicit Seek past EOF is honored (sparse hole); only the implicit
	// position from the initial stat is reconciled to the on-disk size
	// (both shrink and growth).
	if !fw.seeked {
		if st, err := f.Stat(); err == nil {
			fw.pos = st.Size()
		}
	}
	if fw.pos > 0 {
		if _, err := f.Seek(fw.pos, io.SeekStart); err != nil {
			f.Close()
			return err
		}
	}
	fw.f = f
	return nil
}
