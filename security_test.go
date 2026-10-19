package fetch

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProxyPasswordNotLeaked(t *testing.T) {
	_, err := NewSession(WithProxy("https://user:supersecret@127.0.0.1:8443"))
	require.Error(t, err)
	msg := err.Error()
	assert.NotContains(t, msg, "supersecret")
	assert.Contains(t, msg, "proxy")
	assert.Contains(t, strings.ToLower(msg), "https")
	assert.Contains(t, strings.ToLower(msg), "plaintext")
	// Password must be redacted, username context may remain.
	assert.NotContains(t, msg, "user:supersecret")
}

func TestBaseURLUserinfoNotLeaked(t *testing.T) {
	_, err := NewSession(WithBaseURL("https://user:supersecret@example.com/"))
	require.Error(t, err)
	msg := err.Error()
	assert.NotContains(t, msg, "supersecret")
	assert.Contains(t, msg, "userinfo")
	assert.Contains(t, msg, "base url")
}

func TestRequestURLUserinfoNotLeaked(t *testing.T) {
	s := testSession(t)
	err := s.Get(context.Background(), "http://user:supersecret@example.com/", nil)
	require.Error(t, err)
	msg := err.Error()
	assert.NotContains(t, msg, "supersecret")
	assert.Contains(t, msg, "userinfo")
	assert.Contains(t, msg, "invalid url")
}

func TestRedirectUserinfoNotLeaked(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "http://user:supersecret@example.com/")
		w.WriteHeader(http.StatusFound)
	})
	s := testSession(t)
	err := s.Get(context.Background(), ts.URL, nil)
	require.Error(t, err)
	msg := err.Error()
	assert.NotContains(t, msg, "supersecret")
	assert.Contains(t, strings.ToLower(msg), "redirect")
	assert.Contains(t, msg, "userinfo")
	assert.Equal(t, 1, ts.count(), "invalid redirect must not be followed")
}

func TestMalformedURLPasswordNotLeaked(t *testing.T) {
	_, err := NewSession(WithBaseURL("https://user:supersecret@example.com/%zz"))
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "supersecret")
	assert.Contains(t, err.Error(), "base url")
}

func TestOpaqueRequestURLPasswordNotLeaked(t *testing.T) {
	s := testSession(t)
	err := s.Get(context.Background(), "http:user:supersecret@example.com", nil, WithRetries(0))
	require.Error(t, err)
	msg := err.Error()
	assert.NotContains(t, msg, "supersecret")
	assert.Contains(t, msg, "invalid url")
	assert.Contains(t, strings.ToLower(msg), "host")
}

func TestOpaqueBaseURLPasswordNotLeaked(t *testing.T) {
	_, err := NewSession(WithBaseURL("http:user:supersecret@example.com"))
	require.Error(t, err)
	msg := err.Error()
	assert.NotContains(t, msg, "supersecret")
	assert.Contains(t, msg, "base url")
}

func TestOpaqueProxyPasswordNotLeaked(t *testing.T) {
	_, err := NewSession(WithProxy("http:user:supersecret@127.0.0.1:8080"))
	require.Error(t, err)
	msg := err.Error()
	assert.NotContains(t, msg, "supersecret")
	assert.Contains(t, msg, "proxy")
}

func TestOpaqueRedirectPasswordNotLeaked(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "http:user:supersecret@example.com")
		w.WriteHeader(http.StatusFound)
	})
	s := testSession(t)
	err := s.Get(context.Background(), ts.URL, nil)
	require.Error(t, err)
	msg := err.Error()
	assert.NotContains(t, msg, "supersecret")
	assert.Contains(t, strings.ToLower(msg), "redirect")
	assert.Equal(t, 1, ts.count(), "invalid redirect must not be followed")
}
