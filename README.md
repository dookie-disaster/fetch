# fetch

High-level HTTP fetch tools for Go, built on
[github.com/sardanioss/httpcloak](https://github.com/sardanioss/httpcloak).

The `fetch` package wraps an httpcloak session (browser-grade TLS/HTTP
fingerprinting) and adds the plumbing an HTTP client or download worker
usually ends up hand-rolling:

- **High-level fetch interface** — `Session.Get/Post/Head/...` that stream the
  response body into any `io.Writer`.
- **Automatic retries** — exponential backoff with random jitter, with a
  user-provided policy function for full control over the retry decision
  (e.g. don't retry 404s).
- **Progress reporting** — an `io.Writer` wrapper that calls your callback at
  a configurable interval (default: every second) with throughput statistics.
- **Cookies** — paste a `Cookie` header straight out of browser developer
  tools (e.g. after completing an anti-bot challenge) and send it verbatim.
- **File writer** — a writer that opens its target file on demand, only when
  the download actually starts, so queued workers keep open files to a minimum.
- **Automatic resume** — range requests that continue a partially downloaded
  body, both across sessions and on every retry after a mid-body failure.

Bodies are streamed end to end (never buffered whole), redirects are followed
with browser-like method rewriting, and a response that ends before delivering
its advertised number of bytes is treated as a failure so that retries and
resume can kick in.

## Installation

```
go get github.com/dookie-disaster/fetch
```

Requires Go 1.26+.

## Quick start

```go
package main

import (
	"bytes"
	"context"
	"log"

	"github.com/dookie-disaster/fetch"
)

func main() {
	s, err := fetch.NewSession()
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	var buf bytes.Buffer
	if err := s.Get(context.Background(), "https://example.com", &buf); err != nil {
		log.Fatal(err)
	}
	log.Printf("got %d bytes", buf.Len())
}
```

A non-2xx response is returned as a `*fetch.StatusError` once the retry policy
gives up:

```go
err := s.Get(ctx, url, &buf)
var se *fetch.StatusError
if errors.As(err, &se) {
	log.Printf("server said %s after %d attempt(s)", se.Status, se.Attempts)
}
```

## Downloads with progress and automatic resume

```go
// File is NOT opened until the first write.
fw := fetch.NewFileWriter("/tmp/big.iso", fetch.WithFileHash(sha256.New()))
defer fw.Close()

err := s.Get(ctx, "https://example.com/big.iso", fw,
	fetch.WithResume(true), // continue from fw's current position
	fetch.WithProgress(func(st fetch.ProgressStats) {
		log.Printf("%d/%d bytes (%.1f MB/s)",
			st.Written, st.Total, st.BytesPerSec/1e6)
	}),
)
if err == nil {
	sum, err := fw.Sum()
	if err != nil {
		log.Printf("checksum failed: %v", err)
	} else {
		log.Printf("sha256: %x", sum)
	}
}
```

- `NewFileWriter` stats the path up front: an existing partial file makes the
  first request a `Range: bytes=<size>-` request, but the file itself is only
  opened when the first byte is written.
- If the server ignores the range and replies `200` with the full body, the
  writer is rewound (and truncated) and the download starts over.
- If the transfer dies mid-body, the retry continues from the bytes already
  on disk — `Range: bytes=<pos>-` is recomputed for every attempt.
- `WithFileHash` maintains a digest of the complete file as bytes arrive, and
  `fw.Sum()` returns it. Existing bytes in a resumed file are included: they
  are read once from disk to seed the hash before the first new write. Fresh
  downloads add no extra read. The supplied hash is reset and owned by the
  writer. `Sum` fails when the file is shorter than the writer's position (an
  unwritten sparse gap after `Seek` past EOF has no bytes to digest yet).
- To abort and discard the partial file, call `fw.Cancel()`.

## Retries

Defaults: 3 retries after the initial attempt, delays growing exponentially
from 500ms to a 30s cap, with random jitter (each delay is drawn uniformly
between the minimum and the current cap).

The built-in policy retries transport-level failures, responses whose body
ended prematurely (`fetch.ErrIncompleteResponse`), and statuses 408, 425, 429
and all 5xx. Everything else — 404 Not Found in particular — fails
immediately.

When the failed response carries a `Retry-After` header (delta-seconds or an
HTTP date), that delay takes precedence over the jittered backoff for the
next attempt, capped at the configured maximum backoff (huge values saturate
instead of overflowing).

Supply your own decision function for full control:

```go
err := s.Get(ctx, url, &buf,
	fetch.WithRetryPolicy(func(ctx context.Context, attempt int,
		ri *fetch.ResponseInfo, err error) bool {

		// Never retry 404, let the default rules handle the rest.
		if ri != nil && ri.StatusCode == http.StatusNotFound {
			return false
		}
		return fetch.DefaultRetryPolicy(ctx, attempt, ri, err)
	}),
	fetch.WithRetries(5),
	fetch.WithRetryBackoff(100*time.Millisecond, 5*time.Second),
)
```

Requests with a body are replayed automatically when the body is a
`*bytes.Buffer` (copied) or any `io.ReadSeeker` (`*bytes.Reader`,
`*strings.Reader`, `*os.File`, ... — rewound to the offset it held when the
request started). For anything else, provide a body factory (and pass nil as
the body):

```go
err := s.Post(ctx, url, nil, &buf,
	fetch.WithBodyProvider(func() (io.Reader, error) {
		return os.Open("upload.bin")
	}),
)
```

Without a provider, a one-shot body fails with `fetch.ErrBodyNotReplayable`
rather than being re-sent corrupted or empty. Supplying both a body and a
provider fails the request before anything is sent.

Invalid option combinations (`min > max` backoff, non-positive progress
interval, negative timeout/redirects, negative session retries, bad header
keys/values, non-http(s) URLs, userinfo in URLs, unsupported or hostless proxy
URLs, `WithProxy` combined with `WithClient`) fail fast — `NewSession`
for session defaults, `Fetch` for per-request options — instead of being
silently ignored. A negative per-request `WithRetries(n)` is the one
exception: it is clamped to 0 to disable retries.

## Cookies

The typical anti-bot workflow: complete the challenge in a real browser, copy
the `Cookie` request header from developer tools, and send it unchanged —
flags, exotic values and all:

```go
err := s.Get(ctx, url, &buf,
	fetch.WithCookieHeader("cf_clearance=AbCdEf0123; __cf_bm=XyZ.123~; consent"),
)
```

`fetch.ParseCookieHeader(raw)` parses such a string into `[]*http.Cookie` if
you need to inspect or filter the individual cookies first. Multiple cookie
options merge (`a=1` + `b=2` → `a=1; b=2`) instead of last-wins.

## Sessions

Options configure the underlying httpcloak client and set the defaults every
request inherits:

```go
s, err := fetch.NewSession(
	fetch.WithPreset("firefox-latest"),        // browser fingerprint preset
	fetch.WithProxy("socks5://user:pass@host:1080"), // HTTP/SOCKS5 proxy
	fetch.WithBaseURL("https://api.example.com/v1/"),
	fetch.WithDefaultHeaders(http.Header{"X-Client": {"my-app"}}),
	fetch.WithDefaultResume(true),             // resume every download by default
	fetch.WithDefaultRetries(2),
	fetch.WithDefaultBackoff(250*time.Millisecond, 10*time.Second),
	fetch.WithDefaultProgressInterval(500*time.Millisecond),
	fetch.WithDefaultTimeout(30*time.Second),  // per-attempt timeout
	fetch.WithDefaultMaxRedirects(10),         // redirect hops per attempt
)
```

Bring your own httpcloak session with `fetch.WithClient(cloak)` — you keep
ownership and `s.Close()` leaves it untouched. `s.Client()` exposes the
underlying session for advanced configuration (custom fingerprints, the cookie
jar). Proxies are configured with `fetch.WithProxy` (schemes `http`, `socks5`,
`socks5h`, with optional `user:pass@` credentials). An `https://` proxy URL is
rejected: the underlying client sends the CONNECT request in plaintext, so it
would leak proxy credentials instead of providing the TLS the scheme suggests.
Combining `WithProxy` with `WithClient` fails, since the supplied client owns
its own transport.

## API overview

| Method | Description |
|---|---|
| `Get(ctx, url, wr, opts...)` | GET, body copied to `wr` |
| `Head(ctx, url, opts...)` | HEAD, no body to copy |
| `Post/Put/Patch(ctx, url, body, wr, opts...)` | body-carrying requests |
| `Delete/Options(ctx, url, wr, opts...)` | other methods |
| `Fetch(ctx, method, url, body, wr, opts...)` | arbitrary method — the core |
| `Client()` | the underlying `*httpcloak.Session` |
| `Close()` | release resources (unless the client was user-supplied) |

Notable request options: `WithHeader(s)`, `WithCookieHeader`, `WithCookies`,
`WithRetries`, `WithRetryPolicy`, `WithRetryBackoff`, `WithProgress`,
`WithProgressInterval`, `WithResume`, `WithTimeout`, `WithMaxRedirects`,
`WithBodyProvider`, `WithOnResponse`.

## Behavior notes

- **Streaming** — response bodies are streamed into the writer; nothing is
  buffered whole. `Total` in progress stats is the complete resource size
  (`-1` only if unknown, e.g. transparently decompressed responses whose wire
  `Content-Length` describes compressed bytes); for a `206` resume it is the
  full file size, and `Written` includes pre-existing bytes so a completed
  resume ends with `Written == Total` (e.g. `100/100`) while `BytesPerSec`
  measures only newly transferred bytes. `Elapsed` spans retries in the same
  `Fetch` so progress never regresses.
- **Redirects** — followed per attempt (up to the configured max, default 10;
  `WithMaxRedirects(0)` disables): 301/302/303 rewrite non-GET/HEAD methods to
  GET and drop the body (including `Content-Length`/`Content-Type` and other
  body headers) while HEAD stays HEAD (303 does not rewrite HEAD to GET),
  307/308 replay the body, mirroring `net/http`. Same-origin hops re-send all
  headers; once a chain
  leaves the origin (including `https→http` downgrades) only a safelist
  (`Accept`, `Accept-Language`, `Accept-Encoding`, `Cache-Control`,
  `If-Modified-Since`, `If-None-Match`, `If-Range`, `Range`, `Content-Type`,
  `User-Agent`) is forwarded, so `Authorization`/`Cookie` never leak — even if
  a later hop bounces back. Redirect targets must be `http(s)` URLs with a
  host and no userinfo. Exceeding the cap returns
  `fetch.ErrTooManyRedirects` alongside the last 3xx response info.
- **Truncation** — a body shorter *or longer* than its announced length is an
  error (`fetch.ErrIncompleteResponse`), never a silent success, so partial
  files are always resumable; excess bytes are never written. A `206` without
  a valid `Content-Range` starting at the requested offset fails instead of
  appending corruptly, as does an unsolicited `206` that does not cover the
  whole resource from byte 0 or whose `Content-Length` disagrees with its
  `Content-Range`.
- **Resume** — `WithResume` sends `Range: bytes=<pos>-` from the writer's
  position; a `200` fallback truncates (`FileWriter.Reset`, `Truncate(0)` for
  `*os.File`, `Seek(0)` otherwise — plain seekers may retain a stale tail if
  the re-sent body is shorter). If a resuming `206` covers only part of the
  remaining range, the client keeps issuing range requests within the same
  attempt until the advertised total from `Content-Range` is reached (bounded,
  and only for automatic resume, not user-supplied `Range` headers). A `416`
  surfaces as `*StatusError`; check the local size against the expected total
  before re-fetching a complete file.
- **FileWriter** — opens lazily, appends to partial files, truncates on
  `Reset`, removes on `Cancel` (idempotent; `Cancel` after `Close` fails so
  `Close`'s "file left on disk" promise holds). `Seek` after `Close` fails.
  One writer per concurrent download; methods are individually thread-safe but
  check-then-act sequences are not atomic. `WithFileSync(true)` fsyncs on
  `Close`/`Reset`; `WithFileHash` streams a full-file digest returned by
  `Sum` (available after `Close` too).
- **Conditional caching** — the underlying httpcloak session's
  ETag/If-Modified-Since handling is disabled at construction, because this
  layer must always receive full bodies. Re-enable it via `Client()` if you
  prefer raw session semantics.
- **Errors** — non-2xx → `*fetch.StatusError` (with `StatusCode`, `Status`,
  `Headers`, `Attempts`); non-replayable bodies → `fetch.ErrBodyNotReplayable`;
  exhausted redirect chains → `fetch.ErrTooManyRedirects`. All wrapping uses
  `%w`, so `errors.Is`/`errors.As` work throughout. Once a fetch has returned,
  `fetch.IsRetryable` classifies the final error for a higher-level retry and
  `fetch.RetryAfter` extracts its `Retry-After` delay.

### Higher-level retries

A worker that wants to pause for longer than the built-in backoff can classify
the final error before deciding to reschedule it:

```go
if err := s.Get(ctx, url, fw, fetch.WithResume(true)); err != nil {
	if !fetch.IsRetryable(err) {
		return err // 4xx, redirect loop, writer error, ...
	}
	wait, ok := fetch.RetryAfter(err)
	if !ok {
		wait = 5 * time.Minute
	}
	time.Sleep(wait)
	// Retry with a fresh context. fw resumes from its current position.
}
```

`IsRetryable` is stricter than the built-in policy for transport failures: it
uses httpcloak's retryability verdict, so permanent TLS, proxy and protocol
errors are not retried. It classifies the cause carried by the error only;
callers reusing a writer must ensure it can resume (a `*FileWriter` with
`WithResume` does), and a canceled or expired context must be replaced.

## Testing

```
go test ./...
```

The suite runs against local `httptest` servers (no network access needed).
Add `-race` for concurrency checks and `-cover` for coverage (~95%).
