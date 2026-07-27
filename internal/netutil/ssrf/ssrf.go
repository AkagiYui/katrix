// Package ssrf provides SSRF protection for outbound fetches (remote media,
// URL preview). It resolves a URL's host to its IP addresses and rejects any
// that fall into a private/loopback/link-local/reserved range, including IPv6
// ULA and link-local. It also enforces redirect-count, body-size and timeout
// limits.
//
// The check is performed at request time against the freshly resolved IPs, so
// DNS-rebinding (public A record that resolves to 169.254.169.254) is caught.
package ssrf

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Limits configures a guarded fetch.
type Limits struct {
	// MaxBodyBytes caps the response body read.
	MaxBodyBytes int64
	// Timeout is the overall fetch deadline.
	Timeout time.Duration
	// MaxRedirects caps redirect hops (0 = no redirects).
	MaxRedirects int
}

// DefaultLimits are the production-safe defaults for URL preview / remote media.
var DefaultLimits = Limits{
	MaxBodyBytes: 10 << 20, // 10 MB
	Timeout:      10 * time.Second,
	MaxRedirects: 5,
}

// IsBlocked reports whether an IP is in a private/loopback/link-local/reserved
// range that must never be fetched.
func IsBlocked(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() {
		return true
	}
	// IPv4-mapped IPv6 (::ffff:a.b.c.d) reduces to the v4 check above.
	if v4 := ip.To4(); v4 != nil {
		// 0.0.0.0/8 (this-network), 100.64.0.0/10 (CGNAT), 192.0.2.0/24,
		// 198.51.100.0/24, 203.0.113.0/24 (TEST-NET), 240.0.0.0/4 (reserved).
		if v4[0] == 0 || (v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127) ||
			(v4[0] == 192 && v4[1] == 0 && v4[2] == 2) ||
			(v4[0] == 198 && v4[1] == 51 && v4[2] == 100) ||
			(v4[0] == 203 && v4[1] == 0 && v4[2] == 113) ||
			v4[0] >= 240 {
			return true
		}
	}
	return false
}

// CheckHost resolves host (via the system resolver) and reports whether every
// resolved IP is blocked. Returns an error if resolution fails or any IP is
// blocked. When all IPs are blocked the URL must not be fetched.
func CheckHost(ctx context.Context, host string) error {
	// Strip a port if present.
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	// Literal IP?
	if ip := net.ParseIP(host); ip != nil {
		if IsBlocked(ip) {
			return fmt.Errorf("ssrf: ip %s is blocked", ip)
		}
		return nil
	}
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
	if err != nil {
		return fmt.Errorf("ssrf: resolve %s: %w", host, err)
	}
	for _, ip := range ips {
		if IsBlocked(ip) {
			return fmt.Errorf("ssrf: host %s resolves to blocked ip %s", host, ip)
		}
	}
	return nil
}

// CheckURL parses rawURL and runs CheckHost on its hostname.
func CheckURL(ctx context.Context, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("ssrf: parse: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("ssrf: scheme %q not allowed", u.Scheme)
	}
	if u.Host == "" {
		return fmt.Errorf("ssrf: empty host")
	}
	return CheckHost(ctx, u.Hostname())
}

// SafeTransport is an http.Transport whose DialContext checks every dialled IP
// against IsBlocked. It catches DNS-rebinding because the check runs at connect
// time on the actual address being dialled.
func SafeTransport() *http.Transport {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	return &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, _ := net.SplitHostPort(addr)
			ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
			if err != nil {
				return nil, fmt.Errorf("ssrf: resolve %s: %w", host, err)
			}
			for _, ip := range ips {
				if IsBlocked(ip.IP) {
					return nil, fmt.Errorf("ssrf: dial %s blocked (%s)", host, ip.IP)
				}
			}
			return dialer.DialContext(ctx, network, net.JoinHostPort(host, port))
		},
	}
}

// Fetch performs an SSRF-guarded GET. It validates the URL before dialling,
// caps redirects to l.MaxRedirects (re-checking each Location), limits the
// response body to l.MaxBodyBytes, and bounds the whole operation to l.Timeout.
// Returns the final response (body already truncated) for the caller to read.
func Fetch(ctx context.Context, rawURL string, l Limits) (*http.Response, error) {
	if l.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, l.Timeout)
		defer cancel()
	}
	if err := CheckURL(ctx, rawURL); err != nil {
		return nil, err
	}
	client := &http.Client{
		Timeout:       l.Timeout,
		CheckRedirect: redirectGuard(ctx, l.MaxRedirects),
		Transport:     SafeTransport(),
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	resp.Body = &limitedReadCloser{rc: io.LimitReader(resp.Body, l.MaxBodyBytes), underlying: resp.Body}
	return resp, nil
}

// redirectGuard returns a CheckRedirect that re-runs the SSRF check on each
// redirect target and caps the hop count.
func redirectGuard(ctx context.Context, max int) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if max > 0 && len(via) > max {
			return fmt.Errorf("ssrf: too many redirects")
		}
		return CheckURL(ctx, req.URL.String())
	}
}

type limitedReadCloser struct {
	rc         io.Reader
	underlying io.Closer
}

func (l *limitedReadCloser) Read(p []byte) (int, error) { return l.rc.Read(p) }
func (l *limitedReadCloser) Close() error               { return l.underlying.Close() }

// EnsurePrefixes is a helper that reports whether s starts with http:// or
// https://. Used by handlers that validate the scheme before delegating to Fetch.
func EnsurePrefixes(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}
