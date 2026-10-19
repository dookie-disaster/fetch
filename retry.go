package fetch

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// DefaultRetryPolicy is the built-in retry policy. It retries transport-level
// failures (no HTTP response at all), responses whose body ended prematurely
// (errors wrapping ErrIncompleteResponse), and responses with status 408
// (Request Timeout), 425 (Too Early), 429 (Too Many Requests) or any 5xx.
// Everything else - 404 Not Found, other 4xx, 3xx - fails immediately. Use
// WithRetryPolicy to replace it.
func DefaultRetryPolicy(ctx context.Context, attempt int, ri *ResponseInfo, err error) bool {
	if ri == nil {
		// No response was received; only transport-level failures land here.
		return err != nil
	}
	if errors.Is(err, ErrIncompleteResponse) {
		// The transfer itself failed; the status says nothing about whether
		// a retry will succeed.
		return true
	}
	switch sc := ri.StatusCode; {
	case sc == http.StatusRequestTimeout,
		sc == http.StatusTooEarly,
		sc == http.StatusTooManyRequests,
		sc >= 500:
		return true
	}
	return false
}

// backoffDelay computes the wait before retry number `retry` (1-based: the
// wait before the first retry is backoffDelay(minWait, maxWait, 1)).
//
// The cap grows exponentially - minWait, 2*minWait, 4*minWait, ... capped at
// maxWait - and the actual delay is drawn uniformly from [minWait, cap], so
// retries carry random jitter while never waiting less than minWait or more
// than maxWait.
func backoffDelay(minWait, maxWait time.Duration, retry int) time.Duration {
	if maxWait <= 0 {
		return 0
	}
	if minWait < 0 {
		minWait = 0
	}
	if minWait > maxWait {
		minWait = maxWait
	}
	if retry < 1 {
		retry = 1
	}

	limit := minWait
	for i := 1; i < retry; i++ {
		if limit >= maxWait {
			break
		}
		// Guard against overflow before doubling.
		if limit > maxWait/2 {
			limit = maxWait
			break
		}
		limit *= 2
	}
	if limit < minWait {
		limit = minWait
	}

	span := limit - minWait
	if span <= 0 {
		return limit
	}
	if int64(span) == math.MaxInt64 {
		return minWait + time.Duration(rand.Int64N(int64(span)))
	}
	return minWait + time.Duration(rand.Int64N(int64(span)+1))
}

// retryWait computes the delay before a retry: the jittered backoff, or the
// failed attempt's Retry-After when the response supplies one. Server
// guidance takes precedence over the backoff schedule, but is capped at
// maxWait so an absurd Retry-After cannot stall a download indefinitely.
// Transport-level failures (ri == nil) and responses without a usable
// Retry-After fall back to backoffDelay.
func retryWait(rc *requestConfig, ri *ResponseInfo, retry int) time.Duration {
	delay := backoffDelay(rc.retryMinWait, rc.retryMaxWait, retry)
	if rc.retryMaxWait > 0 {
		if d, ok := retryAfterDelay(ri); ok {
			if d < 0 {
				d = 0
			}
			if d > rc.retryMaxWait {
				return rc.retryMaxWait
			}
			return d
		}
	}
	return delay
}

// retryAfterDelay parses the Retry-After header of a failed attempt's
// response. The value may be delta-seconds or an HTTP date. ok is false when
// ri is nil, the header is absent, or its value is unusable; an HTTP date in
// the past yields zero. Absurdly large delta-seconds saturate to the maximum
// representable duration so the caller can cap it, instead of overflowing
// to a negative duration that would retry immediately.
func retryAfterDelay(ri *ResponseInfo) (d time.Duration, ok bool) {
	if ri == nil {
		return 0, false
	}
	v := strings.TrimSpace(ri.Headers.Get("Retry-After"))
	if v == "" {
		return 0, false
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
		const maxSeconds = int64(1<<63-1) / int64(time.Second)
		if n > maxSeconds {
			return time.Duration(1<<63 - 1), true
		}
		return time.Duration(n) * time.Second, true
	} else if err != nil && errors.Is(err, strconv.ErrRange) && !strings.HasPrefix(v, "-") {
		return time.Duration(1<<63 - 1), true
	}
	if t, err := http.ParseTime(v); err == nil {
		d = time.Until(t)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}

// sleepCtx sleeps for d, returning early with the context's error when ctx is
// canceled.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
