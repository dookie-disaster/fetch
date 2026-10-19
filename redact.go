package fetch

import (
	"errors"
	"net/url"
	"strings"
)

// redactedURLString renders raw as a credential-safe string for error
// messages. When raw parses, (*url.URL).Redacted is used so the username,
// scheme and host stay visible while any password becomes "xxxxx". When raw
// does not parse there is no URL to render, so a best-effort scrub hides a
// userinfo password without echoing the raw input verbatim when it carries
// credentials.
func redactedURLString(raw string) string {
	if u, err := url.Parse(raw); err == nil && u != nil && u.Opaque == "" {
		return u.Redacted()
	}
	return scrubURLString(raw)
}

// scrubURLString hides a userinfo password in raw without relying on
// url.Parse (which may have failed). If no userinfo is present the input has
// no password to leak via userinfo; it is returned unchanged.
func scrubURLString(s string) string {
	schemeEnd := strings.Index(s, "://")
	start := 0
	if schemeEnd >= 0 {
		start = schemeEnd + len("://")
	}
	end := len(s)
	for i := start; i < len(s); i++ {
		if s[i] == '/' || s[i] == '?' || s[i] == '#' {
			end = i
			break
		}
	}
	if start > len(s) || end < start {
		return "[invalid url]"
	}
	authority := s[start:end]
	at := strings.LastIndex(authority, "@")
	if at < 0 {
		return s
	}
	userinfo := authority[:at]
	hostport := authority[at+1:]
	if idx := strings.Index(userinfo, ":"); idx >= 0 {
		userinfo = userinfo[:idx] + ":xxxxx"
	}
	return s[:start] + userinfo + "@" + hostport + s[end:]
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
