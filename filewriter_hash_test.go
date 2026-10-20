package fetch

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func digestSHA256(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}

func TestFileWriterHashFresh(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	h := sha256.New()
	_, err := h.Write([]byte("stale"))
	require.NoError(t, err)

	fw := NewFileWriter(path, WithFileHash(h))
	defer fw.Close()

	_, err = fw.Write([]byte("hello"))
	require.NoError(t, err)
	got, err := fw.Sum()
	require.NoError(t, err)
	assert.Equal(t, digestSHA256([]byte("hello")), got, "WithFileHash must reset the supplied hash")

	_, err = fw.Write([]byte(" world"))
	require.NoError(t, err)
	got, err = fw.Sum()
	require.NoError(t, err)
	assert.Equal(t, digestSHA256([]byte("hello world")), got)

	// Position queries must not invalidate the streaming hash.
	_, err = fw.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	got, err = fw.Sum()
	require.NoError(t, err)
	assert.Equal(t, digestSHA256([]byte("hello world")), got)

	require.NoError(t, fw.Close())
	got, err = fw.Sum()
	require.NoError(t, err)
	assert.Equal(t, digestSHA256([]byte("hello world")), got, "Sum must remain available after Close")
}

func TestFileWriterHashIncludesExistingPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))

	fw := NewFileWriter(path, WithFileHash(sha256.New()))
	defer fw.Close()

	got, err := fw.Sum()
	require.NoError(t, err)
	assert.Equal(t, digestSHA256([]byte("hello")), got, "Sum without writes must hash the existing file")

	_, err = fw.Write([]byte(" world"))
	require.NoError(t, err)
	got, err = fw.Sum()
	require.NoError(t, err)
	assert.Equal(t, digestSHA256([]byte("hello world")), got)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(data))
}

func TestFileWriterHashReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path, WithFileHash(sha256.New()))
	defer fw.Close()

	_, err := fw.Write([]byte("discard"))
	require.NoError(t, err)
	require.NoError(t, fw.Reset())
	_, err = fw.Write([]byte("fresh"))
	require.NoError(t, err)

	got, err := fw.Sum()
	require.NoError(t, err)
	assert.Equal(t, digestSHA256([]byte("fresh")), got)
}

func TestFileWriterHashSeekAndTruncate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path, WithFileHash(sha256.New()))
	defer fw.Close()

	_, err := fw.Write([]byte("0123456789"))
	require.NoError(t, err)

	_, err = fw.Seek(2, io.SeekStart)
	require.NoError(t, err)
	_, err = fw.Write([]byte("AB"))
	require.NoError(t, err)

	got, err := fw.Sum()
	require.NoError(t, err)
	assert.Equal(t, digestSHA256([]byte("01AB")), got, "Sum covers bytes through the current position")

	require.NoError(t, fw.Truncate(2))
	_, err = fw.Write([]byte("Z"))
	require.NoError(t, err)

	got, err = fw.Sum()
	require.NoError(t, err)
	assert.Equal(t, digestSHA256([]byte("01Z")), got)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "01Z", string(data))
}

func TestFileWriterHashSparseSeek(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path, WithFileHash(sha256.New()))
	defer fw.Close()

	_, err := fw.Seek(4, io.SeekStart)
	require.NoError(t, err)
	_, err = fw.Write([]byte("x"))
	require.NoError(t, err)

	want := []byte{0, 0, 0, 0, 'x'}
	got, err := fw.Sum()
	require.NoError(t, err)
	assert.Equal(t, digestSHA256(want), got)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, want, data)
}

func TestFileWriterHashThroughProgressWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path, WithFileHash(sha256.New()))
	defer fw.Close()

	pw := NewProgressWriter(fw, time.Millisecond, nil)
	_, err := pw.Write([]byte("hello world"))
	require.NoError(t, err)

	got, err := fw.Sum()
	require.NoError(t, err)
	assert.Equal(t, digestSHA256([]byte("hello world")), got)
}

func TestFileWriterHashUnconfigured(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path)
	defer fw.Close()
	_, err := fw.Sum()
	require.Error(t, err)

	nilPath := filepath.Join(t.TempDir(), "nil.bin")
	nilFW := NewFileWriter(nilPath, WithFileHash(nil))
	defer nilFW.Close()
	_, err = nilFW.Write([]byte("x"))
	require.NoError(t, err)
	_, err = nilFW.Sum()
	require.Error(t, err)
}

func TestFileWriterHashCancelFailsSum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path, WithFileHash(sha256.New()))
	_, err := fw.Write([]byte("partial"))
	require.NoError(t, err)
	require.NoError(t, fw.Cancel())

	_, err = fw.Sum()
	assert.ErrorIs(t, err, os.ErrClosed)
}

func TestFileWriterHashResumeFetchesRemainder(t *testing.T) {
	ts := rangeServer(t, nil)
	s := testSession(t)

	path := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))

	fw := NewFileWriter(path, WithFileHash(sha256.New()))
	defer fw.Close()

	require.NoError(t, s.Get(context.Background(), ts.URL, fw, WithResume(true)))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, fullBody, string(data))

	got, err := fw.Sum()
	require.NoError(t, err)
	assert.Equal(t, digestSHA256([]byte(fullBody)), got, "resumed digest must include the existing prefix")
	assert.Equal(t, "bytes=5-", ts.record(1).Header.Get("Range"))
}

func TestFileWriterHashIgnoredRangeResetsDigest(t *testing.T) {
	ts := rangeServer(t, func(n int, w http.ResponseWriter, r *http.Request) bool {
		if r.Header.Get("Range") == "" {
			return false
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(fullBody)))
		fmt.Fprint(w, fullBody)
		return true
	})
	s := testSession(t)

	path := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))

	fw := NewFileWriter(path, WithFileHash(sha256.New()))
	defer fw.Close()

	require.NoError(t, s.Get(context.Background(), ts.URL, fw, WithResume(true)))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, fullBody, string(data))

	got, err := fw.Sum()
	require.NoError(t, err)
	assert.Equal(t, digestSHA256([]byte(fullBody)), got, "digest must discard the prefix when the server ignores Range")
}

func TestFileWriterHashRetryRestoresDigest(t *testing.T) {
	ts := truncatedServer(t, fullBody)
	s := testSession(t, WithDefaultRetries(1))

	path := filepath.Join(t.TempDir(), "out.bin")
	fw := NewFileWriter(path, WithFileHash(sha256.New()))
	defer fw.Close()

	require.NoError(t, s.Get(context.Background(), ts.URL, fw))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, fullBody, string(data))

	got, err := fw.Sum()
	require.NoError(t, err)
	assert.Equal(t, digestSHA256([]byte(fullBody)), got, "retry restore must not leave partial bytes in the digest")
	assert.Equal(t, 2, ts.count())
}
