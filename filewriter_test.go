package fetch

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileWriterOpensLazily(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path)

	// Creating the writer must not create or open the file.
	_, err := os.Stat(path)
	assert.True(t, errors.Is(err, os.ErrNotExist), "file must not exist before the first write")

	_, err = fw.Write([]byte("data"))
	require.NoError(t, err)
	assert.Equal(t, int64(4), fw.Pos())

	st, err := os.Stat(path)
	require.NoError(t, err, "file must exist after the first write")
	assert.Equal(t, int64(4), st.Size())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "data", string(data))
}

func TestFileWriterAppendsToPartialFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))

	fw := NewFileWriter(path)
	assert.Equal(t, int64(5), fw.Pos(), "partial data size should be visible before opening")

	pos, err := fw.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(5), pos)

	_, err = fw.Write([]byte(" world"))
	require.NoError(t, err)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(data))
}

func TestFileWriterSeek(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path)
	defer fw.Close()

	_, err := fw.Write([]byte("hello"))
	require.NoError(t, err)

	// SeekStart
	pos, err := fw.Seek(0, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(0), pos)
	_, err = fw.Write([]byte("J"))
	require.NoError(t, err)

	data, _ := os.ReadFile(path)
	assert.Equal(t, "Jello", string(data))

	// SeekCurrent moves relative to the write position (currently 1).
	pos, err = fw.Seek(3, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(4), pos)

	// SeekEnd positions relative to the file size.
	pos, err = fw.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, int64(5), pos)
	pos, err = fw.Seek(-3, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, int64(2), pos)

	// Invalid whence and negative position.
	_, err = fw.Seek(0, 42)
	assert.Error(t, err)
	_, err = fw.Seek(-1, io.SeekStart)
	assert.Error(t, err)
}

func TestFileWriterSeekBeforeOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("12345"), 0o644))

	fw := NewFileWriter(path)
	defer fw.Close()

	// SeekEnd on an unopened writer answers from the on-disk size.
	pos, err := fw.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, int64(5), pos)

	// A seek to the start is honored when the file is eventually opened.
	_, err = fw.Seek(0, io.SeekStart)
	require.NoError(t, err)
	_, err = fw.Write([]byte("X"))
	require.NoError(t, err)

	data, _ := os.ReadFile(path)
	assert.Equal(t, "X2345", string(data))
}

func TestFileWriterReset(t *testing.T) {
	t.Run("opened", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "out.bin")
		fw := NewFileWriter(path)
		defer fw.Close()

		_, err := fw.Write([]byte("0123456789"))
		require.NoError(t, err)
		require.NoError(t, fw.Reset())

		assert.Equal(t, int64(0), fw.Pos())
		st, err := os.Stat(path)
		require.NoError(t, err)
		assert.Equal(t, int64(0), st.Size())

		_, err = fw.Write([]byte("ab"))
		require.NoError(t, err)
		data, _ := os.ReadFile(path)
		assert.Equal(t, "ab", string(data))
	})

	t.Run("unopened", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "out.bin")
		require.NoError(t, os.WriteFile(path, []byte("partial"), 0o644))

		fw := NewFileWriter(path)
		defer fw.Close()
		require.Equal(t, int64(7), fw.Pos())

		require.NoError(t, fw.Reset())
		assert.Equal(t, int64(0), fw.Pos())

		st, err := os.Stat(path)
		require.NoError(t, err)
		assert.Equal(t, int64(0), st.Size(), "existing partial data must be truncated")

		_, err = fw.Write([]byte("fresh"))
		require.NoError(t, err)
		data, _ := os.ReadFile(path)
		assert.Equal(t, "fresh", string(data))
	})

	t.Run("missing file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "never-created.bin")
		fw := NewFileWriter(path)
		defer fw.Close()
		require.NoError(t, fw.Reset())
		assert.Equal(t, int64(0), fw.Pos())
	})
}

func TestFileWriterCancelRemovesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path)

	_, err := fw.Write([]byte("partial"))
	require.NoError(t, err)
	require.NoError(t, fw.Cancel())

	_, err = os.Stat(path)
	assert.True(t, errors.Is(err, os.ErrNotExist), "cancel must remove the file")

	// Canceling again (no file) is not an error.
	require.NoError(t, fw.Cancel())
}

func TestFileWriterClosedIsNotUsable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path)
	_, err := fw.Write([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, fw.Close())
	require.NoError(t, fw.Close(), "closing twice is a no-op")

	_, err = fw.Write([]byte("y"))
	assert.True(t, errors.Is(err, os.ErrClosed), "write after close must fail with os.ErrClosed, got: %v", err)
	assert.Error(t, fw.Reset())
	assert.ErrorIs(t, fw.Cancel(), os.ErrClosed, "cancel after close must fail since Close promised to keep the file")
	_, statErr := os.Stat(path)
	assert.NoError(t, statErr, "file must be left in place after Close")
}

func TestFileWriterPerm(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path, WithFilePerm(0o600))
	defer fw.Close()

	_, err := fw.Write([]byte("x"))
	require.NoError(t, err)

	st, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), st.Mode().Perm())
}

func TestFileWriterPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path)
	defer fw.Close()
	assert.Equal(t, path, fw.Path())
}

func TestFileWriterIsWriteSeeker(t *testing.T) {
	var _ io.WriteSeeker = (*FileWriter)(nil)
	var _ io.Closer = (*FileWriter)(nil)
}

func TestFileWriterNilOption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	require.NotPanics(t, func() {
		fw := NewFileWriter(path, nil)
		defer fw.Close()
		_, err := fw.Write([]byte("x"))
		require.NoError(t, err)
	})
}

func TestFileWriterReconcilesExternalShrinkBeforeFirstWrite(t *testing.T) {
	t.Run("removed", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "out.bin")
		require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))

		fw := NewFileWriter(path)
		defer fw.Close()
		require.Equal(t, int64(5), fw.Pos())

		// External removal between NewFileWriter and the first write.
		require.NoError(t, os.Remove(path))

		// Read-only position query must not mark the writer as seeked.
		pos, err := fw.Seek(0, io.SeekCurrent)
		require.NoError(t, err)
		require.Equal(t, int64(5), pos)

		_, err = fw.Write([]byte("hi"))
		require.NoError(t, err)

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, "hi", string(data), "ensureOpen must reconcile pos with on-disk size instead of creating a sparse hole")
		assert.Equal(t, int64(2), fw.Pos())
		st, err := os.Stat(path)
		require.NoError(t, err)
		assert.Equal(t, int64(2), st.Size())
	})

	t.Run("externally truncated", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "out.bin")
		require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))

		fw := NewFileWriter(path)
		defer fw.Close()
		require.Equal(t, int64(5), fw.Pos())

		// External truncation between NewFileWriter and the first write.
		require.NoError(t, os.Truncate(path, 2))

		pos, err := fw.Seek(0, io.SeekCurrent)
		require.NoError(t, err)
		require.Equal(t, int64(5), pos)

		_, err = fw.Write([]byte("XY"))
		require.NoError(t, err)

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, "heXY", string(data), "ensureOpen must reconcile pos with on-disk size instead of creating a sparse hole")
		assert.Equal(t, int64(4), fw.Pos())
		st, err := os.Stat(path)
		require.NoError(t, err)
		assert.Equal(t, int64(4), st.Size())
	})

	t.Run("SeekEnd on missing file reconciles to zero", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "out.bin")
		require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))

		fw := NewFileWriter(path)
		defer fw.Close()
		require.Equal(t, int64(5), fw.Pos())

		require.NoError(t, os.Remove(path))

		pos, err := fw.Seek(0, io.SeekEnd)
		require.NoError(t, err)
		assert.Equal(t, int64(0), pos)
	})
}

func TestFileWriterReconcilesExternalGrowBeforeFirstWrite(t *testing.T) {
	t.Run("externally grown", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "out.bin")
		require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))

		fw := NewFileWriter(path)
		defer fw.Close()
		require.Equal(t, int64(5), fw.Pos())

		// External growth between NewFileWriter and the first write.
		require.NoError(t, os.WriteFile(path, []byte("hello world!"), 0o644))

		_, err := fw.Write([]byte("XY"))
		require.NoError(t, err)

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, "hello world!XY", string(data))
		assert.Equal(t, int64(14), fw.Pos())
		st, err := os.Stat(path)
		require.NoError(t, err)
		assert.Equal(t, int64(14), st.Size())
	})

	t.Run("grown after position query", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "out.bin")
		require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))

		fw := NewFileWriter(path)
		defer fw.Close()
		require.Equal(t, int64(5), fw.Pos())

		// Read-only position query must not mark the writer as seeked.
		pos, err := fw.Seek(0, io.SeekCurrent)
		require.NoError(t, err)
		require.Equal(t, int64(5), pos)

		// External growth between NewFileWriter and the first write.
		require.NoError(t, os.WriteFile(path, []byte("hello world!"), 0o644))

		_, err = fw.Write([]byte("XY"))
		require.NoError(t, err)

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, "hello world!XY", string(data))
		assert.Equal(t, int64(14), fw.Pos())
		st, err := os.Stat(path)
		require.NoError(t, err)
		assert.Equal(t, int64(14), st.Size())
	})
}

func TestFileWriterResetAfterSeekStartOnUnopenedTruncates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("partial"), 0o644))

	fw := NewFileWriter(path)
	defer fw.Close()
	require.Equal(t, int64(7), fw.Pos())

	_, err := fw.Seek(0, io.SeekStart)
	require.NoError(t, err)

	require.NoError(t, fw.Reset())
	assert.Equal(t, int64(0), fw.Pos())

	st, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, int64(0), st.Size(), "Reset on unopened writer must truncate on-disk file even when pos is already 0")
}

func TestFileWriterTruncate(t *testing.T) {
	t.Run("direct", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "out.bin")
		fw := NewFileWriter(path)
		defer fw.Close()

		// *FileWriter must implement interface{ Truncate(int64) error }.
		var _ interface{ Truncate(int64) error } = (*FileWriter)(nil)
		_, ok := interface{}(fw).(interface{ Truncate(int64) error })
		require.True(t, ok, "*FileWriter must implement Truncate(int64) error")

		_, err := fw.Write([]byte("0123456789"))
		require.NoError(t, err)

		require.NoError(t, fw.Truncate(4))
		assert.Equal(t, int64(4), fw.Pos())

		st, err := os.Stat(path)
		require.NoError(t, err)
		assert.Equal(t, int64(4), st.Size())
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, "0123", string(data))

		pos, err := fw.Seek(0, io.SeekCurrent)
		require.NoError(t, err)
		assert.Equal(t, int64(4), pos)

		_, err = fw.Write([]byte("AB"))
		require.NoError(t, err)
		data, err = os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, "0123AB", string(data))
	})

	t.Run("via ProgressWriter", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "out.bin")
		fw := NewFileWriter(path)
		defer fw.Close()

		_, err := fw.Write([]byte("0123456789"))
		require.NoError(t, err)

		pw := NewProgressWriter(fw, time.Millisecond, nil)
		require.NoError(t, pw.Truncate(3))

		assert.Equal(t, int64(3), fw.Pos())
		st, err := os.Stat(path)
		require.NoError(t, err)
		assert.Equal(t, int64(3), st.Size())
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, "012", string(data))
	})

	t.Run("unopened", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "out.bin")
		require.NoError(t, os.WriteFile(path, []byte("0123456789"), 0o644))
		fw := NewFileWriter(path)
		defer fw.Close()
		require.Equal(t, int64(10), fw.Pos())

		require.NoError(t, fw.Truncate(4))
		assert.Equal(t, int64(4), fw.Pos())
		st, err := os.Stat(path)
		require.NoError(t, err)
		assert.Equal(t, int64(4), st.Size())
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, "0123", string(data))
	})
}
