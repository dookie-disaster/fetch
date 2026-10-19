// Package fetch provides high-level tools for HTTP client fetches built on
// github.com/sardanioss/httpcloak.
//
// The entry point is a Session, which wraps an httpcloak session (browser TLS
// and HTTP fingerprinting) and adds the plumbing a download worker usually
// ends up hand-rolling: automatic retries with exponential backoff and random
// jitter, a progress-reporting io.Writer, cookie injection from a raw Cookie
// header, a file writer that opens its target file on demand, and automatic
// resume of partially downloaded bodies via range requests.
//
// Bodies are streamed (never buffered whole), redirects are followed with
// browser-like method rewriting, and a response that ends before delivering
// the advertised number of bytes is treated as a failure so retries and
// resume can kick in.
//
// Basic usage:
//
//	s, err := fetch.NewSession()
//	if err != nil {
//	    return err
//	}
//	defer s.Close()
//
//	var buf bytes.Buffer
//	err = s.Get(ctx, "https://example.com", &buf)
//
// Download with progress and automatic resume into a file:
//
//	fw := fetch.NewFileWriter("/tmp/big.iso")
//	defer fw.Close()
//
//	err = s.Get(ctx, "https://example.com/big.iso", fw,
//	    fetch.WithResume(true),
//	    fetch.WithProgress(func(st fetch.ProgressStats) {
//	        log.Printf("%d/%d bytes (%.1f MB/s)", st.Written, st.Total, st.BytesPerSec/1e6)
//	    }),
//	)
package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"

	"github.com/sardanioss/httpcloak"
)

// ErrTooManyRedirects is returned when a redirect chain exceeds the
// configured maximum hops (DefaultMaxRedirects by default, see
// WithDefaultMaxRedirects and WithMaxRedirects).
var ErrTooManyRedirects = errors.New("fetch: too many redirects")

// ErrIncompleteResponse is wrapped into errors returned when a response body
// ends before delivering the expected number of bytes (a connection closed
// early, a read failure mid-body, or a short body under a known
// Content-Length). The default retry policy retries these; custom policies
// can check for it with errors.Is to make their own call.
var ErrIncompleteResponse = errors.New("fetch: incomplete response")

// Session is a high-level HTTP fetch session. It wraps an httpcloak session
// and adds retries, progress reporting, cookie injection, lazy file writing
// and automatic resume on top of it.
//
// A Session is safe for concurrent use by multiple goroutines.
type Session struct {
	cloak      *httpcloak.Session
	ownsClient bool
	cfg        sessionConfig
}

// NewSession creates a new Session. Options configure the underlying
// httpcloak client and the defaults applied to every request; individual
// requests can override most defaults through RequestOptions.
//
// By default the session owns a freshly created httpcloak session built from
// DefaultPreset, and Close shuts it down. When WithClient supplies an
// existing httpcloak session, the caller keeps ownership of it and Close
// leaves it untouched.
//
// The httpcloak session's ETag / If-Modified-Since conditional-request cache
// is disabled: this layer must always receive full bodies, and a stale 304
// would surface as a failed fetch. Re-enable it via Client() if the raw
// session semantics are preferred.
func NewSession(opts ...SessionOption) (*Session, error) {
	cfg := defaultSessionConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	var cloak *httpcloak.Session
	owns := true
	if cfg.client != nil {
		cloak, owns = cfg.client, false
		// See NewSession doc comment.
		cloak.SetConditionalCacheEnabled(false)
	} else {
		clientOpts := []httpcloak.SessionOption{httpcloak.WithoutConditionalCache()}
		if cfg.proxy != "" {
			clientOpts = append(clientOpts, httpcloak.WithSessionProxy(cfg.proxy))
		}
		cloak = httpcloak.NewSession(cfg.preset, clientOpts...)
	}
	return &Session{cloak: cloak, ownsClient: owns, cfg: cfg}, nil
}

// Client returns the underlying httpcloak session. Use it for advanced
// configuration that this package does not surface (fingerprints, the cookie
// jar, ...). Proxies are configured through WithProxy.
func (s *Session) Client() *httpcloak.Session { return s.cloak }

// Close releases the session. If the underlying httpcloak session was created
// by NewSession it is closed as well; one supplied via WithClient is left
// open for its owner.
func (s *Session) Close() {
	if s.ownsClient && s.cloak != nil {
		s.cloak.Close()
	}
}

// Get fetches url and copies the response body to wr. wr may be nil, in which
// case the body is discarded.
func (s *Session) Get(ctx context.Context, url string, wr io.Writer, opts ...RequestOption) error {
	return s.Fetch(ctx, http.MethodGet, url, nil, wr, opts...)
}

// Head issues a HEAD request. There is no body to copy, so no writer is taken.
func (s *Session) Head(ctx context.Context, url string, opts ...RequestOption) error {
	return s.Fetch(ctx, http.MethodHead, url, nil, nil, opts...)
}

// Post issues a POST request with body and copies the response body to wr.
func (s *Session) Post(ctx context.Context, url string, body io.Reader, wr io.Writer, opts ...RequestOption) error {
	return s.Fetch(ctx, http.MethodPost, url, body, wr, opts...)
}

// Put issues a PUT request with body and copies the response body to wr.
func (s *Session) Put(ctx context.Context, url string, body io.Reader, wr io.Writer, opts ...RequestOption) error {
	return s.Fetch(ctx, http.MethodPut, url, body, wr, opts...)
}

// Patch issues a PATCH request with body and copies the response body to wr.
func (s *Session) Patch(ctx context.Context, url string, body io.Reader, wr io.Writer, opts ...RequestOption) error {
	return s.Fetch(ctx, http.MethodPatch, url, body, wr, opts...)
}

// Delete issues a DELETE request and copies the response body to wr.
func (s *Session) Delete(ctx context.Context, url string, wr io.Writer, opts ...RequestOption) error {
	return s.Fetch(ctx, http.MethodDelete, url, nil, wr, opts...)
}

// Options issues an OPTIONS request and copies the response body to wr.
func (s *Session) Options(ctx context.Context, url string, wr io.Writer, opts ...RequestOption) error {
	return s.Fetch(ctx, http.MethodOptions, url, nil, wr, opts...)
}

// Fetch issues an arbitrary HTTP request and copies the response body to wr
// (nil wr discards it). This is the core of the high-level interface; the
// method helpers are thin wrappers around it.
//
// Redirects are followed automatically (up to the configured maximum hops per
// attempt, DefaultMaxRedirects by default): 301, 302 and 303 rewrite
// non-GET/HEAD methods to GET and drop the body, 307 and 308 replay the body,
// mirroring net/http. On same-origin redirects all request headers are
// re-sent; once a chain leaves the origin (including https to http
// downgrades) only a safelist (Accept, Accept-Language, Accept-Encoding,
// Cache-Control, If-Modified-Since, If-None-Match, If-Range, Range,
// Content-Type, User-Agent) is forwarded, so Authorization and Cookie never
// leak across hosts, even if a later hop bounces back.
//
// The request is retried automatically according to the retry settings in
// effect (see WithRetries, WithRetryBackoff and WithRetryPolicy). When the
// failed attempt's response carries a Retry-After header, that delay takes
// precedence over the jittered backoff, capped at the maximum backoff. A
// final non-2xx response is returned as a *StatusError. When resuming is
// enabled and wr is a seeker (for example a *FileWriter) with partial data,
// each attempt - including retries after a mid-body failure - is sent as a
// range request continuing from the writer's current position.
func (s *Session) Fetch(ctx context.Context, method, rawURL string, body io.Reader, wr io.Writer, opts ...RequestOption) error {
	if ctx == nil {
		return errors.New("fetch: nil context")
	}
	if !isValidHeaderKey(method) {
		if strings.TrimSpace(method) == "" {
			return errors.New("fetch: empty http method")
		}
		return fmt.Errorf("fetch: invalid http method %q", method)
	}
	rc, err := s.newRequestConfig(opts, body)
	if err != nil {
		return err
	}
	target, err := s.resolveURL(rawURL)
	if err != nil {
		return err
	}

	// Capture the writer's initial state so a retry that cannot resume can
	// rewind to it instead of appending a fresh copy onto partial bytes.
	var initPos int64
	var initSeekOK bool
	var initBufLen int
	var initBufOK bool
	if wr != nil {
		initPos, initSeekOK = seekerPos(wr)
		initBufLen, initBufOK = bufferLen(wr)
	}
	userRange := rc.headers.Get("Range") != ""

	// A single ProgressWriter across attempts preserves Elapsed and lets
	// Written stay monotonic; per-attempt bases are repositioned inside
	// doAttempt after resume negotiation.
	var pw *ProgressWriter
	writerForIO := wr
	if rc.progress != nil {
		underlying := wr
		if underlying == nil {
			underlying = io.Discard
		}
		pw = NewProgressWriter(underlying, rc.progressInterval, rc.progress)
		writerForIO = pw
	} else if existingPW, ok := wr.(*ProgressWriter); ok && existingPW != nil {
		pw = existingPW
	}

	var attempt int
	for {
		ri, attemptWrote, aerr := s.doAttempt(ctx, method, target, rc, writerForIO, pw, attempt)
		if aerr == nil {
			if pw != nil {
				pw.Flush()
			}
			return nil
		}
		// A non-replayable body cannot be retried, and neither can a
		// canceled context.
		if errors.Is(aerr, ErrBodyNotReplayable) || ctx.Err() != nil {
			return s.wrapFinal(method, target, attempt, aerr)
		}
		// A retry after partial bytes were committed to a non-rewindable
		// stream would duplicate data: fail fast instead of corrupting.
		// The caller's explicit Range takes over writer positioning, so it
		// is exempt from this guard.
		if attemptWrote > 0 && !userRange && !canRestoreWriter(writerForIO) {
			return s.wrapFinal(method, target, attempt,
				fmt.Errorf("fetch: cannot retry after %d bytes were written to non-rewindable writer: %w", attemptWrote, aerr))
		}
		if !rc.retryPolicy(ctx, attempt, ri, aerr) || attempt >= rc.maxRetries {
			return s.wrapFinal(method, target, attempt, aerr)
		}
		if werr := sleepCtx(ctx, retryWait(rc, ri, attempt+1)); werr != nil {
			return s.wrapFinal(method, target, attempt, werr)
		}
		// Rewind to the initial state when the next attempt will not
		// resume from the current position. A resumed seeker continues via
		// Range; everything else must start clean.
		if attemptWrote > 0 && !userRange {
			willResume := false
			if rc.resume {
				if pos, ok := seekerPos(writerForIO); ok && pos > 0 {
					willResume = true
				}
			}
			if !willResume {
				if rerr := restoreWriterToInitial(writerForIO, initPos, initSeekOK, initBufLen, initBufOK); rerr != nil {
					return s.wrapFinal(method, target, attempt,
						fmt.Errorf("fetch: restore writer for retry: %w (%w)", rerr, aerr))
				}
				if pw != nil {
					pw.SetWritten(0)
				}
			}
		}
		attempt++
	}
}

// bodyHeadersOnRewrite are dropped when a 301/302/303 rewrites the request
// to GET: they describe a body that is no longer sent.
var bodyHeadersOnRewrite = []string{
	"Content-Length",
	"Content-Type",
	"Content-Encoding",
	"Content-Language",
	"Content-Location",
	"Transfer-Encoding",
}

// doAttempt performs exactly one request attempt: the redirect chain ending
// in the final response, whose body is streamed into wr (which is pw when
// progress is enabled, otherwise the caller's writer). It returns the
// response info (nil on transport-level failure), the bytes written during
// this attempt, and an error of type *StatusError for non-2xx responses.
// attempt is the zero-based Fetch attempt number, used to populate
// StatusError.Attempts so retry policies see the accurate count.
func (s *Session) doAttempt(ctx context.Context, method, target string, rc *requestConfig, wr io.Writer, pw *ProgressWriter, attempt int) (*ResponseInfo, int64, error) {
	if rc.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, rc.timeout)
		defer cancel()
	}

	current := target
	dropBody := false
	crossOrigin := false
	userRange := rc.headers.Get("Range") != ""
	for hop := 0; ; hop++ {
		var bodyReader io.Reader
		if !dropBody {
			var err error
			bodyReader, err = rc.bodyFn()
			if err != nil {
				return nil, 0, err
			}
		}
		var getBody func() (io.Reader, error)
		if !dropBody {
			getBody = rc.bodyFn
		}

		hdr := rc.headerSnapshot()
		if dropBody {
			for _, k := range bodyHeadersOnRewrite {
				hdr.Del(k)
			}
		}
		if method != http.MethodHead && rc.resume && wr != nil && !userRange {
			// The writer position is re-read on every hop: a redirect after
			// a rewind must not carry a stale range.
			if pos, ok := seekerPos(wr); ok && pos > 0 {
				hdr.Set("Range", fmt.Sprintf("bytes=%d-", pos))
			}
		}
		if crossOrigin {
			hdr = filterRedirectHeaders(hdr)
		}

		resp, err := s.cloak.DoStream(ctx, &httpcloak.Request{
			Method:  method,
			URL:     current,
			Headers: map[string][]string(hdr),
			Body:    bodyReader,
			GetBody: getBody,
			Timeout: rc.timeout,
		})
		if err != nil {
			if rc.bodyFnCustom {
				if c, ok := bodyReader.(io.Closer); ok {
					_ = c.Close()
				}
			}
			return nil, 0, err
		}

		// Redirects are followed here rather than by the client, because the
		// streaming path does not follow them itself.
		if loc := redirectTarget(resp.StatusCode, firstHeader(resp, "Location")); loc != "" {
			ri := newResponseInfo(resp, current, method)
			if rc.onResponse != nil {
				rc.onResponse(ri)
			}
			drainAndClose(resp)
			if hop >= rc.maxRedirects {
				// Hand back the last 3xx alongside the error the way
				// net/http does when it gives up on a chain; the default
				// retry policy declines to retry a redirect loop.
				return ri, 0, fmt.Errorf("%w (%d hops)", ErrTooManyRedirects, rc.maxRedirects)
			}
			next, err := resolveReference(current, loc)
			if err != nil {
				return ri, 0, fmt.Errorf("fetch: bad redirect location %q: %w", loc, err)
			}
			if !sameOrigin(current, next) {
				crossOrigin = true
			}
			switch resp.StatusCode {
			case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther:
				dropBody = true
				if method != http.MethodGet && method != http.MethodHead {
					method = http.MethodGet
				}
			case http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
				// Method and body are preserved; bodyFn replays it.
			}
			current = next
			continue
		}

		ri := newResponseInfo(resp, current, method)
		if rc.onResponse != nil {
			rc.onResponse(ri)
		}

		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			drainAndClose(resp)
			return ri, 0, &StatusError{
				URL:        current,
				StatusCode: resp.StatusCode,
				Status:     statusText(resp.StatusCode),
				Headers:    ri.Headers,
				Attempts:   attempt + 1,
			}
		}

		if method == http.MethodHead {
			resp.Close()
			return ri, 0, nil
		}

		// Resume negotiation: the writer already holds some bytes. A
		// user-supplied Range header takes over range handling entirely,
		// so the automatic logic stays out of the way.
		var resumePos int64
		var expectedLen = ri.ContentLength
		var total = ri.ContentLength
		if !userRange && rc.resume && wr != nil {
			if pos, ok := seekerPos(wr); ok && pos > 0 {
				resumePos = pos
				switch resp.StatusCode {
				case http.StatusPartialContent:
					start, end, crTotal, ok := parseContentRange(ri.Headers.Get("Content-Range"))
					if !ok {
						drainAndClose(resp)
						return ri, 0, fmt.Errorf("fetch: resume mismatch: 206 response missing valid Content-Range for bytes from %d", pos)
					}
					if start != pos {
						drainAndClose(resp)
						return ri, 0, fmt.Errorf("fetch: resume mismatch: requested bytes from %d but server sent Content-Range starting at %d", pos, start)
					}
					rangeLen := end - start + 1
					if ri.ContentLength >= 0 && ri.ContentLength != rangeLen {
						drainAndClose(resp)
						return ri, 0, fmt.Errorf("fetch: resume mismatch: Content-Length %d disagrees with Content-Range bytes %d-%d/%d", ri.ContentLength, start, end, crTotal)
					}
					expectedLen = rangeLen
					if crTotal >= 0 {
						total = crTotal
					} else if ri.ContentLength >= 0 {
						total = pos + ri.ContentLength
					} else {
						total = -1
					}
				default:
					// The server ignored the Range header and returned the
					// whole body (200 or any other 2xx); rewind the writer
					// and start over. Truncating writers discard the stale
					// prefix; plain seekers rewind (a shorter re-sent body
					// may leave a stale tail, which is documented in
					// resetWriter).
					if err := resetWriter(wr); err != nil {
						drainAndClose(resp)
						return ri, 0, fmt.Errorf("fetch: reset writer after ignored range request: %w", err)
					}
					resumePos = 0
					expectedLen = ri.ContentLength
					total = ri.ContentLength
				}
			}
		}
		// No Range was sent (fresh writer, non-seeker, resume disabled, or
		// user Range): an unsolicited 206 must cover the whole resource
		// from 0 or it cannot be completed safely. This applies even when
		// resume is disabled.
		if !userRange && resp.StatusCode == http.StatusPartialContent && resumePos == 0 {
			start, end, crTotal, ok := parseContentRange(ri.Headers.Get("Content-Range"))
			rangeLen := int64(0)
			if ok {
				rangeLen = end - start + 1
			}
			if !ok || start != 0 || crTotal < 0 || end+1 != crTotal {
				drainAndClose(resp)
				return ri, 0, fmt.Errorf("fetch: unexpected 206 response without range request (Content-Range %q)", ri.Headers.Get("Content-Range"))
			}
			if ri.ContentLength >= 0 && ri.ContentLength != rangeLen {
				drainAndClose(resp)
				return ri, 0, fmt.Errorf("fetch: 206 Content-Length %d disagrees with Content-Range bytes %d-%d/%d", ri.ContentLength, start, end, crTotal)
			}
			expectedLen = rangeLen
			total = crTotal
		}

		if pw != nil {
			pw.SetTotal(total)
			if resumePos > 0 {
				pw.SetWritten(resumePos)
			} else {
				pw.SetWritten(0)
			}
		}

		attemptWrote, cerr := copyBody(resp, expectedLen, wr, pw)
		resp.Close()
		if cerr != nil {
			// Write-side failures (disk full, ...) are not retryable by the
			// default policy; read-side failures carry
			// ErrIncompleteResponse and are.
			return ri, attemptWrote, cerr
		}

		// A server that closes the connection early must not count as
		// success: a truncated file that looks complete is the worst kind
		// of failure, and it would never be resumed. HEAD responses have
		// no body at all but still carry the Content-Length a GET would
		// produce, so the check does not apply to them.
		if method != http.MethodHead && expectedLen >= 0 {
			if attemptWrote < expectedLen {
				return ri, attemptWrote, fmt.Errorf("fetch: incomplete response body: got %d of %d bytes: %w", attemptWrote, expectedLen, ErrIncompleteResponse)
			}
			// Overshoot is already truncated in copyBody; reaching here
			// with more bytes is impossible, but keep the guard explicit.
			if attemptWrote > expectedLen {
				return ri, attemptWrote, fmt.Errorf("fetch: response body longer than advertised: got %d of %d bytes: %w", attemptWrote, expectedLen, ErrIncompleteResponse)
			}
		}
		return ri, attemptWrote, nil
	}
}

// redirectAllowlist is the set of headers forwarded on cross-origin or
// downgrade redirects. Everything else - Authorization, Cookie and any
// custom headers - is stripped to avoid credential leaks.
var redirectAllowlist = map[string]struct{}{
	"Accept":            {},
	"Accept-Language":   {},
	"Accept-Encoding":   {},
	"Cache-Control":     {},
	"If-Modified-Since": {},
	"If-None-Match":     {},
	"If-Range":          {},
	"Range":             {},
	"Content-Type":      {},
	"User-Agent":        {},
}

// filterRedirectHeaders returns a copy of h containing only allowlisted
// headers for a cross-origin redirect hop.
func filterRedirectHeaders(h http.Header) http.Header {
	out := make(http.Header, len(h))
	for k, vs := range h {
		if _, ok := redirectAllowlist[http.CanonicalHeaderKey(k)]; ok {
			out[http.CanonicalHeaderKey(k)] = append([]string(nil), vs...)
		}
	}
	// Preserve the canonical "Range" form even if the input used another case.
	return out
}

// sameOrigin reports whether a and b share scheme, host and port. A scheme
// change (including https to http), a host change, or a port change counts
// as cross-origin. Default ports are normalized so https://example.com:443
// and https://example.com compare equal (likewise :80 on http).
func sameOrigin(a, b string) bool {
	ua, err := url.Parse(a)
	if err != nil {
		return false
	}
	ub, err := url.Parse(b)
	if err != nil {
		return false
	}
	if !strings.EqualFold(ua.Scheme, ub.Scheme) {
		return false
	}
	if !strings.EqualFold(ua.Hostname(), ub.Hostname()) {
		return false
	}
	pa := ua.Port()
	if pa == "" {
		if strings.EqualFold(ua.Scheme, "https") {
			pa = "443"
		} else if strings.EqualFold(ua.Scheme, "http") {
			pa = "80"
		}
	}
	pb := ub.Port()
	if pb == "" {
		if strings.EqualFold(ub.Scheme, "https") {
			pb = "443"
		} else if strings.EqualFold(ub.Scheme, "http") {
			pb = "80"
		}
	}
	return pa == pb
}

// copyBody streams the response body into dst (wr, or pw when progress is
// enabled). expectedLen is the number of bytes this attempt must deliver
// (-1 when unknown, e.g. transparently decompressed or chunked responses
// without a usable Content-Range); bytes past expectedLen are never written,
// so an overshooting server cannot corrupt the file. It returns the bytes
// written during this attempt. Read-side failures are wrapped with
// ErrIncompleteResponse (retried by default); write-side failures are
// returned as-is. A short write without an error is reported as
// io.ErrShortWrite.
func copyBody(resp *httpcloak.StreamResponse, expectedLen int64, wr io.Writer, pw *ProgressWriter) (int64, error) {
	dst := wr
	if dst == nil {
		dst = io.Discard
	}
	// When progress is enabled wr already is pw (created once per Fetch so
	// Elapsed spans retries); dst writes through it and advances its
	// counters.
	buf := make([]byte, DefaultCopyBufferSize)
	var written int64
	for {
		n, rerr := resp.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			if expectedLen >= 0 && written+int64(len(chunk)) > expectedLen {
				// Truncate the overshoot: keep exactly the advertised
				// bytes on disk so a retry can resume cleanly.
				allow := expectedLen - written
				if allow > 0 {
					wn, werr := dst.Write(chunk[:allow])
					written += int64(wn)
					if werr != nil {
						return written, fmt.Errorf("fetch: write response body: %w", werr)
					}
					if int64(wn) != allow {
						return written, fmt.Errorf("fetch: short write writing response body: wrote %d of %d bytes: %w", wn, allow, io.ErrShortWrite)
					}
				}
				return written, fmt.Errorf("fetch: response body longer than advertised: got at least %d of %d bytes: %w", written+int64(len(chunk))-allow, expectedLen, ErrIncompleteResponse)
			}
			wn, werr := dst.Write(chunk)
			written += int64(wn)
			if werr != nil {
				return written, fmt.Errorf("fetch: write response body: %w", werr)
			}
			if wn != len(chunk) {
				return written, fmt.Errorf("fetch: short write writing response body: wrote %d of %d bytes: %w", wn, len(chunk), io.ErrShortWrite)
			}
		}
		if rerr == io.EOF {
			return written, nil
		}
		if rerr != nil {
			return written, fmt.Errorf("fetch: read response body after %d bytes: %w (%w)", written, ErrIncompleteResponse, rerr)
		}
	}
}

// wrapFinal turns the last attempt error into the value Fetch returns.
func (s *Session) wrapFinal(method, target string, attempt int, err error) error {
	var se *StatusError
	if errors.As(err, &se) {
		se.Attempts = attempt + 1
		return se
	}
	if attempt > 0 {
		return fmt.Errorf("fetch %s %s after %d attempt(s): %w", method, target, attempt+1, err)
	}
	return fmt.Errorf("fetch %s %s: %w", method, target, err)
}

// resolveURL validates rawURL and resolves it against the session base URL
// when it is relative. Only http and https URLs are accepted.
func (s *Session) resolveURL(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("fetch: invalid url %q: %w", rawURL, err)
	}
	if s.cfg.baseURL != nil && !u.IsAbs() {
		u = s.cfg.baseURL.ResolveReference(u)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		if u.Scheme == "" || u.Host == "" {
			return "", fmt.Errorf("fetch: invalid url %q: missing scheme or host", rawURL)
		}
		return "", fmt.Errorf("fetch: invalid url %q: scheme must be http or https", rawURL)
	}
	if u.Host == "" {
		return "", fmt.Errorf("fetch: invalid url %q: missing host", rawURL)
	}
	if u.User != nil {
		return "", fmt.Errorf("fetch: invalid url %q: userinfo not allowed", rawURL)
	}
	return u.String(), nil
}

// resolveReference resolves a Location header value against the URL it was
// received from and validates the result. Only http(s) URLs with a host and
// no userinfo are accepted; anything else (foreign schemes, empty hosts,
// credentials) fails instead of being followed.
func resolveReference(base, ref string) (string, error) {
	r, err := url.Parse(ref)
	if err != nil {
		return "", err
	}
	b, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	var resolved *url.URL
	if r.IsAbs() {
		resolved = r
	} else {
		resolved = b.ResolveReference(r)
	}
	if resolved.Scheme != "http" && resolved.Scheme != "https" {
		if resolved.Scheme == "" || resolved.Host == "" {
			return "", fmt.Errorf("fetch: invalid redirect location %q: missing scheme or host", ref)
		}
		return "", fmt.Errorf("fetch: invalid redirect location %q: scheme must be http or https", ref)
	}
	if resolved.Host == "" {
		return "", fmt.Errorf("fetch: invalid redirect location %q: missing host", ref)
	}
	if resolved.User != nil {
		return "", fmt.Errorf("fetch: invalid redirect location %q: userinfo not allowed", ref)
	}
	return resolved.String(), nil
}

// redirectTarget returns the Location to follow when the status is a
// redirect carrying one, and "" otherwise.
func redirectTarget(status int, location string) string {
	if location == "" {
		return ""
	}
	switch status {
	case http.StatusMovedPermanently,
		http.StatusFound,
		http.StatusSeeOther,
		http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect:
		return location
	}
	return ""
}

// ResponseInfo is a read-only summary of an HTTP response. It is handed to
// the WithOnResponse callback and to retry policies so they can make
// decisions without owning the response itself. For a redirect chain it
// describes the final response.
type ResponseInfo struct {
	StatusCode    int
	Status        string // e.g. "200 OK"
	Headers       http.Header
	FinalURL      string // URL of the response after redirects
	Protocol      string // e.g. "HTTP/1.1"
	ContentLength int64  // body size announced by the response; -1 if unknown
}

func newResponseInfo(resp *httpcloak.StreamResponse, requestURL, method string) *ResponseInfo {
	finalURL := resp.FinalURL
	if finalURL == "" {
		finalURL = requestURL
	}
	contentLength := resp.ContentLength
	// httpcloak transparently decompresses gzip/br/deflate/zstd streams, so
	// the wire Content-Length describes compressed bytes while Read yields
	// decompressed ones. Treat the uncompressed length as unknown instead
	// of comparing decompressed bytes against the compressed length.
	// HEAD has no body to decompress, so its length stays meaningful.
	if method != http.MethodHead && contentLength > 0 {
		if enc := firstHeader(resp, "Content-Encoding"); isDecompressedByClient(enc) {
			contentLength = -1
		}
	}
	return &ResponseInfo{
		StatusCode:    resp.StatusCode,
		Status:        statusText(resp.StatusCode),
		Headers:       canonicalHeaders(resp.Headers),
		FinalURL:      finalURL,
		Protocol:      resp.Protocol,
		ContentLength: contentLength,
	}
}

// canonicalHeaders converts httpcloak's lowercase-keyed header map into a
// canonical http.Header whose keys work with http.Header.Get.
func canonicalHeaders(m map[string][]string) http.Header {
	if m == nil {
		return nil
	}
	h := make(http.Header, len(m))
	for k, vs := range m {
		key := textproto.CanonicalMIMEHeaderKey(k)
		h[key] = append(h[key], vs...)
	}
	return h
}

// StatusError is returned when the server responds with a non-2xx status and
// the retry policy does not want another attempt.
type StatusError struct {
	URL        string
	StatusCode int
	Status     string      // e.g. "404 Not Found"
	Headers    http.Header // headers of the failing response
	Attempts   int         // number of attempts made, including the failing one
}

// Error implements the error interface.
func (e *StatusError) Error() string {
	return fmt.Sprintf("fetch: unexpected status %s for %s (after %d attempt(s))", e.Status, e.URL, e.Attempts)
}

// ErrBodyNotReplayable is returned when a request with a body needs to be
// retried (or replayed for a redirect) but the body is a one-shot stream and
// no body provider was configured with WithBodyProvider. Replayable bodies
// are *bytes.Buffer and any io.ReadSeeker (*bytes.Reader, *strings.Reader,
// *os.File, ...); anything else needs WithBodyProvider.
var ErrBodyNotReplayable = errors.New("fetch: request body is not replayable; use WithBodyProvider or a replayable reader type (*bytes.Buffer or io.ReadSeeker)")

func statusText(code int) string {
	txt := http.StatusText(code)
	if txt == "" {
		return strconv.Itoa(code)
	}
	return strconv.Itoa(code) + " " + txt
}

func firstHeader(resp *httpcloak.StreamResponse, key string) string {
	if resp == nil || len(resp.Headers) == 0 {
		return ""
	}
	if vs := resp.Headers[strings.ToLower(key)]; len(vs) > 0 {
		return vs[0]
	}
	// Tolerate non-lowercase keys from the underlying client.
	want := textproto.CanonicalMIMEHeaderKey(key)
	for k, vs := range resp.Headers {
		if textproto.CanonicalMIMEHeaderKey(k) == want && len(vs) > 0 {
			return vs[0]
		}
	}
	return ""
}

func drainAndClose(resp *httpcloak.StreamResponse) {
	if resp == nil {
		return
	}
	// Drain a little so the connection can go back to the pool.
	io.Copy(io.Discard, io.LimitReader(resp, 4096))
	resp.Close()
}
