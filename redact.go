package fetch

import (
	"errors"
	"net/url"
	"strings"
)

// redactedURLString renders raw as a credential-safe string for error
// messages. When raw parses, (*url.URL).Redacted is used so the username,
// scheme and host stay visible while any password becomes "xxxxx". The
// scrubber then catches malformed and opaque forms that url.Parse does not
// treat as userinfo (for example "http:/user:pass@host"), so the credential
// never reaches the error text.
func redactedURLString(raw string) string {
	if u, err := url.Parse(raw); err == nil && u != nil {
		return scrubURLString(u.Redacted())
	}
	return scrubURLString(raw)
}

// scrubURLString hides passwords in userinfo-looking segments without relying
// on url.Parse (which may have failed or parsed the input as an opaque path).
// It scans the part before any query or fragment, segment by segment, and
// replaces the password half of any "user:password@host" sequence with
// "xxxxx". A segment without a colon before its last '@' is left unchanged,
// so path segments that merely contain '@' are preserved.
func scrubURLString(s string) string {
	limit := len(s)
	if i := strings.IndexAny(s, "?#"); i >= 0 {
		limit = i
	}
	var b strings.Builder
	b.Grow(len(s))
	start := 0
	for i := 0; i < limit; i++ {
		if s[i] != '/' && s[i] != '\\' {
			continue
		}
		b.WriteString(scrubURLSegment(s[start:i]))
		b.WriteByte(s[i])
		start = i + 1
	}
	b.WriteString(scrubURLSegment(s[start:limit]))
	b.WriteString(s[limit:])
	return b.String()
}

// scrubURLSegment redacts the password in one path or authority segment.
func scrubURLSegment(seg string) string {
	at := strings.LastIndexByte(seg, '@')
	if at < 0 {
		return seg
	}
	colon := strings.IndexByte(seg[:at], ':')
	if colon < 0 {
		return seg
	}
	return seg[:colon+1] + "xxxxx" + seg[at:]
}

// urlCause strips the raw URL from a *url.Error, returning the underlying
// cause. url.Error.Error() formats as `op "url": cause`, which would echo
// any userinfo password in the URL; wrapping the cause instead keeps the
// reason while dropping the credential.
func urlCause(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) && ue.Err != nil {
		return ue.Err
	}
	return err
}
