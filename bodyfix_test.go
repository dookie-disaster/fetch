package fetch

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBodyReplayerTypedNilDoesNotPanic(t *testing.T) {
	cases := []io.Reader{
		(*bytes.Reader)(nil),
		(*strings.Reader)(nil),
		(*bytes.Buffer)(nil),
	}
	for _, c := range cases {
		fn := bodyReplayer(c)
		require.NotPanics(t, func() {
			r, err := fn()
			require.NoError(t, err)
			assert.Nil(t, r)
		})
	}
}

func TestFetchTypedNilBodyDoesNotPanic(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t)

	var br *bytes.Reader
	require.NoError(t, s.Post(context.Background(), ts.URL, br, nil))

	var sr *strings.Reader
	require.NoError(t, s.Post(context.Background(), ts.URL, sr, nil))
}

func TestWithBodyProviderClosesOnSuccess(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t)

	var closes atomic.Int64
	err := s.Post(context.Background(), ts.URL, nil, nil,
		WithBodyProvider(func() (io.Reader, error) {
			return &closeCountingReader{Reader: strings.NewReader("payload"), closes: &closes}, nil
		}),
	)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, closes.Load(), int64(1), "Close must be called on the provider's reader after a successful request")
}

func TestWithBodyProviderClosesOnRedirectHop(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/target" {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Redirect(w, r, "/target", http.StatusTemporaryRedirect)
	})
	s := testSession(t)

	var closes atomic.Int64
	var providers atomic.Int64
	err := s.Post(context.Background(), ts.URL+"/redirect", nil, nil,
		WithBodyProvider(func() (io.Reader, error) {
			providers.Add(1)
			return &closeCountingReader{Reader: strings.NewReader("payload"), closes: &closes}, nil
		}),
	)
	require.NoError(t, err)
	assert.Equal(t, int64(2), providers.Load(), "provider must be called once per redirect hop")
	assert.GreaterOrEqual(t, closes.Load(), int64(2), "Close must be called on the provider's reader for every redirect hop")
	assert.Equal(t, 2, ts.count())
}
