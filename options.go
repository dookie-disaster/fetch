package fetch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sardanioss/httpcloak"
)

// DefaultPreset is the httpcloak browser fingerprint preset used when no
// preset or client is configured explicitly.
const DefaultPreset = "chrome-latest"

const (
	// DefaultMaxRetries is the number of retries after the initial attempt.
	DefaultMaxRetries = 3
	// DefaultRetryMinWait is the smallest backoff delay before a retry.
	DefaultRetryMinWait = 500 * time.Millisecond
	// DefaultRetryMaxWait caps the exponential backoff delay.
	DefaultRetryMaxWait = 30 * time.Second
	// DefaultProgressInterval is how often progress callbacks fire.
	DefaultProgressInterval = time.Second
	// DefaultCopyBufferSize is the buffer used to stream response bodies.
	DefaultCopyBufferSize = 64 * 1024
	// DefaultMaxRedirects bounds the number of redirect hops followed per attempt.
	DefaultMaxRedirects = 10
)

// RetryFunc decides whether a failed attempt should be retried. attempt is
// the zero-based number of the attempt that just failed, ri summarizes the
// HTTP response (nil when the attempt failed at the transport level) and err
// is the attempt's error. Return true to schedule another attempt.
//
// It is never consulted for a canceled context or for a request whose body
// cannot be replayed; those fail immediately.
type RetryFunc func(ctx context.Context, attempt int, ri *ResponseInfo, err error) bool

// ResponseFunc receives a summary of every HTTP response received, including
// responses that are later retried or rejected as errors.
type ResponseFunc func(*ResponseInfo)

// sessionConfig holds Session-wide settings.
type sessionConfig struct {
	client           *httpcloak.Session
	preset           string
	proxy            string
	baseURL          *url.URL
	baseURLRaw       string
	baseURLSet       bool
	defaultHeaders   http.Header
	maxRetries       int
	retryPolicy      RetryFunc
	retryMinWait     time.Duration
	retryMaxWait     time.Duration
	progressInterval time.Duration
	resume           bool
	timeout          time.Duration
	maxRedirects     int

	setupErr error
}

func defaultSessionConfig() sessionConfig {
	return sessionConfig{
		preset:           DefaultPreset,
		maxRetries:       DefaultMaxRetries,
		retryMinWait:     DefaultRetryMinWait,
		retryMaxWait:     DefaultRetryMaxWait,
		progressInterval: DefaultProgressInterval,
		maxRedirects:     DefaultMaxRedirects,
	}
}

func (c *sessionConfig) validate() error {
	if c.setupErr != nil {
		return c.setupErr
	}
	if c.baseURLSet {
		u, err := url.Parse(c.baseURLRaw)
		if err != nil {
			return fmt.Errorf("fetch: invalid base url: %w", urlCause(err))
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("fetch: invalid base url %q: scheme must be http or https", redactedURLString(c.baseURLRaw))
		}
		if u.Host == "" || u.Hostname() == "" {
			return fmt.Errorf("fetch: invalid base url %q: missing host", redactedURLString(c.baseURLRaw))
		}
		if u.User != nil {
			return fmt.Errorf("fetch: invalid base url %q: userinfo not allowed", redactedURLString(c.baseURLRaw))
		}
		c.baseURL = u
	}
	if c.client == nil && c.preset == "" {
		return errors.New("fetch: empty browser preset")
	}
	if c.proxy != "" {
		if c.client != nil {
			return errors.New("fetch: WithProxy cannot be combined with WithClient")
		}
		if err := validateProxyURL(c.proxy); err != nil {
			return err
		}
	}
	if c.maxRetries < 0 {
		return errors.New("fetch: max retries must be >= 0")
	}
	if c.retryMinWait < 0 || c.retryMaxWait < c.retryMinWait {
		return errors.New("fetch: invalid retry backoff: need 0 <= min <= max")
	}
	if c.progressInterval <= 0 {
		return errors.New("fetch: progress interval must be positive")
	}
	if c.timeout < 0 {
		return errors.New("fetch: timeout must be >= 0")
	}
	if c.maxRedirects < 0 {
		return errors.New("fetch: max redirects must be >= 0")
	}
	if err := validateHeaders(c.defaultHeaders); err != nil {
		return err
	}
	return nil
}

// SessionOption configures a Session.
type SessionOption func(*sessionConfig)

// WithClient supplies an existing httpcloak session to build on. The caller
// retains ownership: Session.Close will not close it. When set, WithPreset is
// ignored; combining it with WithProxy fails NewSession.
func WithClient(c *httpcloak.Session) SessionOption {
	return func(cfg *sessionConfig) {
		if c != nil {
			cfg.client = c
		}
	}
}

// WithPreset selects the httpcloak browser fingerprint preset (e.g.
// "chrome-latest", "firefox-latest", "safari-latest"). Defaults to
// DefaultPreset. Ignored when WithClient is used.
func WithPreset(preset string) SessionOption {
	return func(cfg *sessionConfig) {
		cfg.preset = preset
	}
}

// WithProxy routes every request through an HTTP or SOCKS5 proxy. Accepted
// URL schemes are http, socks5 and socks5h; credentials may be embedded as
// userinfo (e.g. "socks5://user:pass@host:1080"). An unparseable URL, an
// unsupported scheme or a missing host fails NewSession.
// The https scheme is rejected rather than passed through: the underlying
// client connects to HTTP proxies in plaintext, so an https:// proxy URL
// would send the CONNECT request (and any embedded credentials) in the clear
// instead of over the TLS the scheme suggests. Providers that serve plaintext
// CONNECT on port 443 should be spelled http://proxy:443.
// Combining WithProxy with WithClient fails NewSession: the supplied client
// owns its own transport configuration.
// When given multiple times the last value wins.
func WithProxy(proxyURL string) SessionOption {
	return func(cfg *sessionConfig) {
		cfg.proxy = proxyURL
	}
}

// WithBaseURL resolves relative request URLs against the given base. An
// invalid base (unparseable, missing scheme or host, non-http(s) scheme, or
// embedded userinfo) fails NewSession rather than being silently ignored.
// When given multiple times the last value wins.
func WithBaseURL(base string) SessionOption {
	return func(cfg *sessionConfig) {
		cfg.baseURLRaw = base
		cfg.baseURLSet = true
		cfg.baseURL = nil
	}
}

// WithDefaultHeaders sets headers sent with every request; per-request
// headers take precedence. Keys are canonicalized, so map literals with any
// casing behave like WithHeader. Invalid header keys or values fail
// NewSession. When given multiple times the last value wins.
func WithDefaultHeaders(h http.Header) SessionOption {
	return func(cfg *sessionConfig) {
		if h == nil {
			cfg.defaultHeaders = nil
			return
		}
		out := make(http.Header, len(h))
		for k, vs := range h {
			if !isValidHeaderKey(k) {
				out[k] = vs
				continue
			}
			for _, v := range vs {
				out.Add(k, v)
			}
		}
		cfg.defaultHeaders = out
	}
}

// WithDefaultRetries sets the number of retries after the initial attempt
// (DefaultMaxRetries by default). A negative value fails NewSession.
// When given multiple times the last value wins.
func WithDefaultRetries(n int) SessionOption {
	return func(cfg *sessionConfig) {
		cfg.maxRetries = n
	}
}

// WithDefaultRetryPolicy sets the session-wide retry decision function
// (DefaultRetryPolicy by default). See WithRetryPolicy.
func WithDefaultRetryPolicy(fn RetryFunc) SessionOption {
	return func(cfg *sessionConfig) {
		if fn != nil {
			cfg.retryPolicy = fn
		}
	}
}

// WithDefaultBackoff sets the session-wide exponential backoff bounds for
// retries: each retry waits a random duration between minWait and twice the
// previous delay, capped at maxWait. Invalid bounds (min < 0 or max < min)
// fail NewSession instead of being silently ignored.
// When given multiple times the last value wins.
func WithDefaultBackoff(minWait, maxWait time.Duration) SessionOption {
	return func(cfg *sessionConfig) {
		cfg.retryMinWait, cfg.retryMaxWait = minWait, maxWait
	}
}

// WithDefaultProgressInterval sets the session-wide progress callback
// interval (DefaultProgressInterval by default). A non-positive interval
// fails NewSession. When given multiple times the last value wins.
func WithDefaultProgressInterval(d time.Duration) SessionOption {
	return func(cfg *sessionConfig) {
		cfg.progressInterval = d
	}
}

// WithDefaultResume enables or disables automatic resume for every request
// (disabled by default). See WithResume.
func WithDefaultResume(enabled bool) SessionOption {
	return func(cfg *sessionConfig) {
		cfg.resume = enabled
	}
}

// WithDefaultTimeout sets a per-attempt timeout applied to every request.
// Zero means no timeout beyond the httpcloak session default. A negative
// timeout fails NewSession; use zero to clear.
// When given multiple times the last value wins.
func WithDefaultTimeout(d time.Duration) SessionOption {
	return func(cfg *sessionConfig) {
		cfg.timeout = d
	}
}

// WithDefaultMaxRedirects sets the session-wide cap on redirect hops followed
// per attempt (DefaultMaxRedirects by default). Zero disables following
// redirects; a negative value fails NewSession.
// When given multiple times the last value wins.
func WithDefaultMaxRedirects(n int) SessionOption {
	return func(cfg *sessionConfig) {
		cfg.maxRedirects = n
	}
}

// validateProxyURL accepts the proxy schemes the underlying client honors and
// insists on a host, so a mistyped proxy fails at construction rather than
// silently sending traffic direct. The https scheme is rejected because the
// underlying client never applies TLS to the proxy connection itself: the
// scheme would promise TLS the wire does not carry, quietly exposing
// Proxy-Authorization credentials in plaintext (verified: the CONNECT request
// goes out in the clear for an https:// proxy URL in httpcloak v1.7.3).
func validateProxyURL(proxyURL string) error {
	u, err := url.Parse(proxyURL)
	if err != nil {
		return fmt.Errorf("fetch: invalid proxy url: %w", urlCause(err))
	}
	switch u.Scheme {
	case "http", "socks5", "socks5h":
	case "https":
		return fmt.Errorf("fetch: invalid proxy url %q: https is not supported: the CONNECT request would be sent in plaintext, not over the TLS the scheme promises; use http, socks5 or socks5h", redactedURLString(proxyURL))
	default:
		return fmt.Errorf("fetch: invalid proxy url %q: scheme must be http, socks5 or socks5h", redactedURLString(proxyURL))
	}
	if u.Host == "" || u.Hostname() == "" {
		return fmt.Errorf("fetch: invalid proxy url %q: missing host", redactedURLString(proxyURL))
	}
	return nil
}

// requestConfig is the resolved configuration for a single request.
type requestConfig struct {
	headers          http.Header
	maxRetries       int
	retryPolicy      RetryFunc
	retryMinWait     time.Duration
	retryMaxWait     time.Duration
	progress         ProgressFunc
	progressInterval time.Duration
	resume           bool
	timeout          time.Duration
	maxRedirects     int
	bodyFn           func() (io.Reader, error)
	bodyFnCustom     bool
	onResponse       ResponseFunc

	setupErr error
}

func (rc *requestConfig) validate() error {
	if rc.setupErr != nil {
		return rc.setupErr
	}
	if rc.retryMinWait < 0 || rc.retryMaxWait < rc.retryMinWait {
		return fmt.Errorf("fetch: invalid retry backoff: need 0 <= min <= max, got min=%v max=%v", rc.retryMinWait, rc.retryMaxWait)
	}
	if rc.progressInterval <= 0 {
		return fmt.Errorf("fetch: progress interval must be positive, got %v", rc.progressInterval)
	}
	if rc.timeout < 0 {
		return fmt.Errorf("fetch: timeout must be >= 0, got %v", rc.timeout)
	}
	if rc.maxRedirects < 0 {
		return fmt.Errorf("fetch: max redirects must be >= 0, got %d", rc.maxRedirects)
	}
	if err := validateHeaders(rc.headers); err != nil {
		return err
	}
	return nil
}

// validateHeaders rejects empty keys, non-token header names (RFC 7230),
// and control characters in values. It prevents header smuggling via
// whitespace, colons, CR/LF or NUL bytes.
func validateHeaders(h http.Header) error {
	for k, vs := range h {
		if !isValidHeaderKey(k) {
			if strings.TrimSpace(k) == "" {
				return errors.New("fetch: empty header key")
			}
			return fmt.Errorf("fetch: invalid header key %q: must be a valid RFC 7230 token", k)
		}
		for _, v := range vs {
			if !isValidHeaderValue(v) {
				return fmt.Errorf("fetch: invalid header value for %q: must not contain CTLs, CR, LF or NUL", k)
			}
		}
	}
	return nil
}

func isValidHeaderKey(k string) bool {
	if k == "" {
		return false
	}
	for i := 0; i < len(k); i++ {
		c := k[i]
		if c < 33 || c > 126 {
			return false
		}
		switch c {
		case '(', ')', '<', '>', '@', ',', ';', ':', '\\', '"', '/', '[', ']', '?', '=', '{', '}', ' ', '\t':
			return false
		}
	}
	return true
}

func isValidHeaderValue(v string) bool {
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\r' || c == '\n' || c == 0 {
			return false
		}
		if (c < 0x20 && c != '\t') || c == 0x7f {
			return false
		}
	}
	return true
}

// headerSnapshot returns a fresh copy of the configured headers, ready to be
// extended and handed to the client.
func (rc *requestConfig) headerSnapshot() http.Header {
	if rc.headers == nil {
		return http.Header{}
	}
	return rc.headers.Clone()
}

// RequestOption configures a single request.
type RequestOption func(*requestConfig)

// WithHeader sets a single request header, replacing any existing value.
func WithHeader(key, value string) RequestOption {
	return func(rc *requestConfig) {
		if rc.headers == nil {
			rc.headers = http.Header{}
		}
		rc.headers.Set(key, value)
	}
}

// WithHeaders sets request headers in bulk, replacing values for the keys
// present in h.
func WithHeaders(h http.Header) RequestOption {
	return func(rc *requestConfig) {
		if rc.headers == nil {
			rc.headers = http.Header{}
		}
		for k, vs := range h {
			if !isValidHeaderKey(k) {
				rc.headers[k] = vs
				continue
			}
			rc.headers.Del(k)
			for _, v := range vs {
				rc.headers.Add(k, v)
			}
		}
	}
}

// WithCookieHeader sends the given raw Cookie header value, e.g. a string
// copied from the request headers in browser developer tools after completing
// an anti-bot challenge. The value is sent byte-for-byte, so it can carry
// cookie flags and exotic values that structured parsing would mangle. If a
// Cookie header is already present (from default headers or an earlier
// option), the values are merged with "; " rather than replacing.
func WithCookieHeader(raw string) RequestOption {
	return func(rc *requestConfig) {
		if strings.TrimSpace(raw) == "" {
			return
		}
		if rc.headers == nil {
			rc.headers = http.Header{}
		}
		mergeCookieHeader(rc.headers, raw)
	}
}

// WithCookies sends the given cookies serialized into a Cookie header. If a
// Cookie header is already present, the new cookies are appended with "; ".
func WithCookies(cookies ...*http.Cookie) RequestOption {
	return func(rc *requestConfig) {
		for _, c := range cookies {
			if c == nil || c.Name == "" {
				continue
			}
			if !isValidHeaderKey(c.Name) || !isValidHeaderValue(c.Value) || strings.ContainsRune(c.Value, ';') {
				if rc.setupErr == nil {
					rc.setupErr = fmt.Errorf("fetch: invalid cookie %q: name must be a valid RFC 7230 token and value must not contain CTLs or ';'", c.Name)
				}
				return
			}
		}
		raw := serializeCookies(cookies)
		if raw == "" {
			return
		}
		if rc.headers == nil {
			rc.headers = http.Header{}
		}
		mergeCookieHeader(rc.headers, raw)
	}
}

func mergeCookieHeader(h http.Header, raw string) {
	if existing := h.Get("Cookie"); existing != "" {
		h.Set("Cookie", existing+"; "+raw)
		return
	}
	h.Set("Cookie", raw)
}

// WithRetries sets the number of retries after the initial attempt for this
// request, overriding the session default. A negative value disables retries
// (clamped to 0) for backwards compatibility.
func WithRetries(n int) RequestOption {
	return func(rc *requestConfig) {
		if n >= 0 {
			rc.maxRetries = n
		} else {
			rc.maxRetries = 0
		}
	}
}

// WithRetryPolicy installs a custom retry decision function for this request,
// overriding the session default. It is consulted for every failed attempt -
// transport errors as well as non-2xx responses - and returns true when
// another attempt should be made. Use it to express rules like "retry
// anything except 404":
//
//	fetch.WithRetryPolicy(func(ctx context.Context, attempt int, ri *fetch.ResponseInfo, err error) bool {
//	    if ri != nil && ri.StatusCode == http.StatusNotFound {
//	        return false
//	    }
//	    return fetch.DefaultRetryPolicy(ctx, attempt, ri, err)
//	})
func WithRetryPolicy(fn RetryFunc) RequestOption {
	return func(rc *requestConfig) {
		if fn != nil {
			rc.retryPolicy = fn
		}
	}
}

// WithRetryBackoff overrides the session backoff bounds for this request.
// Invalid bounds are reported by Fetch when the request runs.
func WithRetryBackoff(minWait, maxWait time.Duration) RequestOption {
	return func(rc *requestConfig) {
		rc.retryMinWait, rc.retryMaxWait = minWait, maxWait
	}
}

// WithProgress registers a progress callback, invoked roughly every
// progressInterval (DefaultProgressInterval unless overridden) while the
// response body streams into the writer. See ProgressStats.
func WithProgress(fn ProgressFunc) RequestOption {
	return func(rc *requestConfig) {
		if fn != nil {
			rc.progress = fn
		}
	}
}

// WithProgressInterval overrides the session progress callback interval for
// this request. A non-positive interval is reported by Fetch when the
// request runs.
func WithProgressInterval(d time.Duration) RequestOption {
	return func(rc *requestConfig) {
		rc.progressInterval = d
	}
}

// WithResume enables automatic resume for this request. When the writer is
// an io.Seeker positioned past its start - a *FileWriter over a partially
// downloaded file, for instance - the request is sent as a range request
// ("Range: bytes=<pos>-") and the response is appended. If the server ignores
// the range and replies 200 with the full body, the writer is rewound and the
// download starts over. Resume also applies to every retry: after a mid-body
// failure the next attempt continues from the bytes already written. A
// partial 206 covering only part of the remaining range is continued
// automatically with further range requests within the same attempt until the
// Content-Range total is reached.
//
// A Range header set explicitly on the request (WithHeader("Range", ...))
// disables the automatic negotiation for it: the caller takes over both the
// range and the writer positioning.
//
// Note that a server may reject a range request with 416 when the local file
// is already complete; check the writer's size against the expected total
// before fetching if that case matters.
func WithResume(enabled bool) RequestOption {
	return func(rc *requestConfig) {
		rc.resume = enabled
	}
}

// WithTimeout sets a per-attempt timeout for this request. It bounds each
// attempt, not the whole retry sequence. Zero clears the timeout; a negative
// timeout is reported by Fetch when the request runs.
func WithTimeout(d time.Duration) RequestOption {
	return func(rc *requestConfig) {
		rc.timeout = d
	}
}

// WithMaxRedirects caps the redirect hops followed for this request,
// overriding the session default. Zero disables following redirects.
func WithMaxRedirects(n int) RequestOption {
	return func(rc *requestConfig) {
		rc.maxRedirects = n
	}
}

// WithBodyProvider supplies a function returning a fresh reader over the
// request body for every attempt. It is required for retries (and 307/308
// redirect replays) when the body is not replayable (see bodyReplayer).
// Supplying both a body and a provider fails the request with an error
// instead of silently discarding one of them; pass nil as the body when
// using a provider.
func WithBodyProvider(fn func() (io.Reader, error)) RequestOption {
	return func(rc *requestConfig) {
		if fn != nil {
			rc.bodyFn = fn
			rc.bodyFnCustom = true
		}
	}
}

// WithOnResponse registers a callback invoked with a summary of every HTTP
// response received for this request, including intermediate redirect
// responses and attempts that are subsequently retried or turned into a
// *StatusError. It is not called when a request fails at the transport level.
func WithOnResponse(fn ResponseFunc) RequestOption {
	return func(rc *requestConfig) {
		if fn != nil {
			rc.onResponse = fn
		}
	}
}

// newRequestConfig merges session defaults with per-request options and
// derives a replayable body provider for the given body. A request that
// supplies both a body and WithBodyProvider fails, as does an otherwise
// invalid option set; the error is returned here so Fetch can surface it.
func (s *Session) newRequestConfig(opts []RequestOption, body io.Reader) (*requestConfig, error) {
	rc := &requestConfig{
		headers:          s.cfg.defaultHeaders.Clone(),
		maxRetries:       s.cfg.maxRetries,
		retryPolicy:      s.cfg.retryPolicy,
		retryMinWait:     s.cfg.retryMinWait,
		retryMaxWait:     s.cfg.retryMaxWait,
		progressInterval: s.cfg.progressInterval,
		resume:           s.cfg.resume,
		timeout:          s.cfg.timeout,
		maxRedirects:     s.cfg.maxRedirects,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(rc)
		}
	}
	if rc.retryPolicy == nil {
		rc.retryPolicy = DefaultRetryPolicy
	}
	if rc.bodyFnCustom && body != nil {
		return nil, fmt.Errorf("%w: pass nil body when using WithBodyProvider", ErrBodyNotReplayable)
	}
	if rc.bodyFn == nil {
		rc.bodyFn = bodyReplayer(body)
	}
	if err := rc.validate(); err != nil {
		return nil, err
	}
	return rc, nil
}

// isNilReader reports whether r is nil, including typed-nil pointers like
// (*bytes.Reader)(nil), which are non-nil interfaces holding a nil pointer.
func isNilReader(r io.Reader) bool {
	if r == nil {
		return true
	}
	v := reflect.ValueOf(r)
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.UnsafePointer:
		return v.IsNil()
	}
	return false
}

// bodyReplayer wraps body so that each retry attempt gets a fresh reader over
// the same content.
//
//   - *bytes.Buffer is snapshotted by copying its unread bytes; each attempt
//     gets a fresh *bytes.Reader over the copy, so later appends by the caller
//     cannot race the in-flight retry.
//   - Any io.ReadSeeker (*bytes.Reader, *strings.Reader, *os.File, ...) is
//     rewound to the offset it held when the request started, so a
//     pre-positioned reader replays deterministically instead of sending a
//     suffix first and the full content on retry.
//   - Any other stream can be used exactly once, and further attempts report
//     ErrBodyNotReplayable.
//
// A body must not be shared across concurrent Fetch calls; the replay cursor
// is per-request state.
func bodyReplayer(body io.Reader) func() (io.Reader, error) {
	if isNilReader(body) {
		return func() (io.Reader, error) { return nil, nil }
	}
	switch b := body.(type) {
	case nil:
		return func() (io.Reader, error) { return nil, nil }
	case *bytes.Buffer:
		if b == nil {
			return func() (io.Reader, error) { return nil, nil }
		}
		data := append([]byte(nil), b.Bytes()...)
		return func() (io.Reader, error) { return bytes.NewReader(data), nil }
	default:
		if rs, ok := body.(io.ReadSeeker); ok {
			start, err := rs.Seek(0, io.SeekCurrent)
			if err != nil {
				var used atomic.Bool
				if _, ok := rs.(io.Closer); ok {
					return func() (io.Reader, error) {
						if used.Swap(true) {
							return nil, ErrBodyNotReplayable
						}
						return io.NopCloser(rs), nil
					}
				}
				return func() (io.Reader, error) {
					if used.Swap(true) {
						return nil, ErrBodyNotReplayable
					}
					return body, nil
				}
			}
			if _, ok := rs.(io.Closer); ok {
				return func() (io.Reader, error) {
					if _, err := rs.Seek(start, io.SeekStart); err != nil {
						return nil, fmt.Errorf("rewind request body for replay: %w", err)
					}
					return io.NopCloser(rs), nil
				}
			}
			return func() (io.Reader, error) {
				if _, err := rs.Seek(start, io.SeekStart); err != nil {
					return nil, fmt.Errorf("rewind request body for replay: %w", err)
				}
				return rs, nil
			}
		}
		var used atomic.Bool
		return func() (io.Reader, error) {
			if used.Swap(true) {
				return nil, ErrBodyNotReplayable
			}
			return body, nil
		}
	}
}
