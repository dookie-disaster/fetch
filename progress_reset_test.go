package fetch

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type resetUnsupportedWriter struct{ buf bytes.Buffer }

func (w *resetUnsupportedWriter) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *resetUnsupportedWriter) String() string { return w.buf.String() }

func TestProgressWriterResetUnsupportedWriter(t *testing.T) {
	plain := &resetUnsupportedWriter{}
	pw := NewProgressWriter(plain, time.Millisecond, nil)

	_, err := pw.Write([]byte("hello"))
	require.NoError(t, err)
	require.Equal(t, int64(5), pw.Written())

	err = pw.Reset()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support reset")
	assert.Equal(t, "hello", plain.String(), "underlying content must be unchanged")
	assert.Equal(t, int64(5), pw.Written(), "Written must be unchanged, not zeroed")
}

func TestProgressWriterResetNestedUnsupportedWriter(t *testing.T) {
	plain := &resetUnsupportedWriter{}
	inner := NewProgressWriter(plain, time.Millisecond, nil)
	outer := NewProgressWriter(inner, time.Millisecond, nil)

	_, err := outer.Write([]byte("hello"))
	require.NoError(t, err)
	require.Equal(t, int64(5), inner.Written())
	require.Equal(t, int64(5), outer.Written())

	err = outer.Reset()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support reset")
	assert.Equal(t, "hello", plain.String(), "underlying content must be unchanged")
	assert.Equal(t, int64(5), inner.Written(), "inner Written must be unchanged")
	assert.Equal(t, int64(5), outer.Written(), "outer Written must be unchanged")
}

func TestProgressWriterResetSupportedWriters(t *testing.T) {
	t.Run("bytes.Buffer", func(t *testing.T) {
		var buf bytes.Buffer
		pw := NewProgressWriter(&buf, time.Millisecond, nil)

		_, err := pw.Write([]byte("hello"))
		require.NoError(t, err)
		require.Equal(t, int64(5), pw.Written())

		require.NoError(t, pw.Reset())
		assert.Equal(t, 0, buf.Len())
		assert.Equal(t, int64(0), pw.Written())
	})

	t.Run("FileWriter", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "out.bin")
		fw := NewFileWriter(path)
		defer fw.Close()
		pw := NewProgressWriter(fw, time.Millisecond, nil)

		_, err := pw.Write([]byte("hello"))
		require.NoError(t, err)
		require.Equal(t, int64(5), pw.Written())

		require.NoError(t, pw.Reset())
		assert.Equal(t, int64(0), pw.Written())
		assert.Equal(t, int64(0), fw.Pos())
	})

	t.Run("os.File", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "out.bin")
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
		require.NoError(t, err)
		defer f.Close()
		pw := NewProgressWriter(f, time.Millisecond, nil)

		_, err = pw.Write([]byte("hello"))
		require.NoError(t, err)
		require.Equal(t, int64(5), pw.Written())

		require.NoError(t, pw.Reset())
		assert.Equal(t, int64(0), pw.Written())

		pos, err := f.Seek(0, io.SeekCurrent)
		require.NoError(t, err)
		assert.Equal(t, int64(0), pos)

		st, err := f.Stat()
		require.NoError(t, err)
		assert.Equal(t, int64(0), st.Size())
	})
}

func TestResetWriterUnsupportedProgressWriter(t *testing.T) {
	plain := &resetUnsupportedWriter{}
	pw := NewProgressWriter(plain, time.Millisecond, nil)

	_, err := pw.Write([]byte("hello"))
	require.NoError(t, err)
	require.Equal(t, int64(5), pw.Written())

	err = resetWriter(pw)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support reset")
	assert.Equal(t, "hello", plain.String(), "underlying content must be unchanged")
	assert.Equal(t, int64(5), pw.Written(), "Written must be unchanged, not zeroed")
}
