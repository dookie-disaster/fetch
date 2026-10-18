package fetch

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

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
