package fetch

import (
	"net/http"
	"strings"
)

// ParseCookieHeader parses the value of a Cookie request header - the kind of
// string copied from the network panel or the request headers in browser
// developer tools after completing an anti-bot challenge - into individual
// cookies.
//
// The parse is deliberately forgiving: pairs are split on semicolons, names
// and values are trimmed of surrounding whitespace, and cookie flags without
// a value (e.g. "consent") are kept as cookies with an empty value. Malformed
// pairs without a name are dropped.
//
// Note that Session sends the raw string unchanged when you use
// WithCookieHeader; this function is for inspection and filtering.
func ParseCookieHeader(raw string) []*http.Cookie {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ";")
	cookies := make([]*http.Cookie, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, value := part, ""
		if eq := strings.Index(part, "="); eq >= 0 {
			name, value = part[:eq], part[eq+1:]
		}
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		cookies = append(cookies, &http.Cookie{Name: name, Value: strings.TrimSpace(value)})
	}
	if len(cookies) == 0 {
		return nil
	}
	return cookies
}

// serializeCookies renders cookies into a Cookie header value. Cookies with
// empty values are serialized as bare names, matching how browsers send
// cookie flags.
func serializeCookies(cookies []*http.Cookie) string {
	if len(cookies) == 0 {
		return ""
	}
	parts := make([]string, 0, len(cookies))
	for _, c := range cookies {
		if c == nil || c.Name == "" {
			continue
		}
		if c.Value == "" {
			parts = append(parts, c.Name)
			continue
		}
		parts = append(parts, c.Name+"="+c.Value)
	}
	return strings.Join(parts, "; ")
}
