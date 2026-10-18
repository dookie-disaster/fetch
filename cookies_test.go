package fetch

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCookieHeader(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want map[string]string // name -> value, order-insensitive
	}{
		{"simple", "a=1; b=2", map[string]string{"a": "1", "b": "2"}},
		{"whitespace", "  a = 1 ;   b=2  ", map[string]string{"a": "1", "b": "2"}},
		{"flag cookie", "consent; b=2", map[string]string{"consent": "", "b": "2"}},
		{"value with equals", "a=1=2; b=3", map[string]string{"a": "1=2", "b": "3"}},
		{"empty segments", ";; a=1; ;", map[string]string{"a": "1"}},
		{"nameless dropped", "=x; a=1", map[string]string{"a": "1"}},
		{"empty", "", map[string]string{}},
		{"only semicolons", " ; ; ", map[string]string{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseCookieHeader(tc.raw)
			if len(tc.want) == 0 {
				assert.Empty(t, got)
				return
			}
			require.Len(t, got, len(tc.want))
			m := map[string]string{}
			for _, c := range got {
				m[c.Name] = c.Value
			}
			assert.Equal(t, tc.want, m)
		})
	}
}

func TestWithCookieHeaderSendsRawValue(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t)

	// A realistic post-anti-bot-challenge Cookie header, pasted verbatim.
	raw := "cf_clearance=AbCdEf012345; __cf_bm=XyZ.123~; csrftoken=Q9w8e7; sessionid=42; consent"
	require.NoError(t, s.Get(context.Background(), ts.URL, nil, WithCookieHeader(raw)))

	got := ts.record(1).Header.Get("Cookie")
	assert.Equal(t, raw, got, "raw cookie header must be sent byte-for-byte")
}

func TestWithCookiesSerializes(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t)

	require.NoError(t, s.Get(context.Background(), ts.URL, nil,
		WithCookies(
			&http.Cookie{Name: "a", Value: "1"},
			&http.Cookie{Name: "b", Value: "2"},
			&http.Cookie{Name: "flagged"},
			nil,
			&http.Cookie{Name: "", Value: "dropped"},
		)))

	assert.Equal(t, "a=1; b=2; flagged", ts.record(1).Header.Get("Cookie"))
}

func TestCookieOptionOverridesHeader(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t)

	h := http.Header{}
	h.Set("Cookie", "old=1")
	require.NoError(t, s.Get(context.Background(), ts.URL, nil,
		WithHeaders(h),
		WithCookieHeader("new=2")))

	assert.Equal(t, "old=1; new=2", ts.record(1).Header.Get("Cookie"),
		"cookie options merge with existing Cookie headers")
}

func TestEmptyCookieOptionsSendNoHeader(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {})
	s := testSession(t)

	require.NoError(t, s.Get(context.Background(), ts.URL, nil,
		WithCookieHeader("   "),
		WithCookies()))

	rec := ts.record(1)
	_, ok := rec.Header["Cookie"]
	assert.False(t, ok, "no Cookie header should be sent")
}

func TestCookiesPreservedOnRetry(t *testing.T) {
	ts := newTestServer(t, func(n int, w http.ResponseWriter, r *http.Request) {
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	s := testSession(t)

	require.NoError(t, s.Get(context.Background(), ts.URL, nil, WithCookieHeader("sid=42")))
	assert.Equal(t, "sid=42", ts.record(1).Header.Get("Cookie"))
	assert.Equal(t, "sid=42", ts.record(2).Header.Get("Cookie"))
}
