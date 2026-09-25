package httpclient

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// Shared http.Client with optimized transport for high-concurrency scenarios.
// This client is designed to maximize connection reuse and minimize overhead
// for applications making many concurrent HTTP requests.
var (
	// sharedClient is the singleton HTTP client instance.
	sharedClient     *http.Client
	sharedClientOnce sync.Once
)

// GetClient returns the shared HTTP client instance with optimized transport settings.
// This client should be reused across your application rather than creating new clients,
// as it maintains a connection pool that benefits from long-lived connections.
func GetClient() *http.Client {
	sharedClientOnce.Do(func() {
		sharedClient = &http.Client{
			Transport: createOptimizedTransport(),
			// Timeout should be set per-request for better control over different operations
			Timeout: 0,
		}
	})
	return sharedClient
}

// createOptimizedTransport creates an http.Transport configured for high-concurrency.
// The transport uses aggressive connection pooling and keep-alive settings to
// maximize socket reuse and minimize TCP/TLS handshake overhead.
func createOptimizedTransport() *http.Transport {
	return &http.Transport{
		// Proxy: Use environment variables (HTTP_PROXY, HTTPS_PROXY) by default

		// DialContext: Custom dialer with optimized keep-alive settings
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second, // Maximum time to establish connection
			KeepAlive: 30 * time.Second, // Send keep-alive probes to maintain connections
			// DualStack is enabled by default for IPv6 support
		}).DialContext,

		// MaxIdleConns: Maximum number of idle connections to keep across all hosts.
		// Set to 200 to handle high concurrency without connection starvation.
		// Idle connections are kept open for reuse, avoiding the overhead of
		// establishing new TCP connections and TLS handshakes.
		MaxIdleConns: 200,

		// MaxIdleConnsPerHost: Maximum idle connections to keep per host.
		// Set to 100 to allow many concurrent requests to the same host.
		// This is critical for services that make many requests to few endpoints
		// (e.g., API gateways, microservices).
		MaxIdleConnsPerHost: 100,

		// MaxConnsPerHost: Maximum concurrent connections to a single host.
		// Set to 0 (unlimited) to allow the connection pool to grow as needed.
		// Combined with MaxIdleConnsPerHost, this allows unlimited concurrent
		// connections while maintaining a large pool of reusable idle connections.
		MaxConnsPerHost: 0,

		// IdleConnTimeout: Maximum time an idle connection can remain unused.
		// Set to 90 seconds to balance connection reuse vs resource cleanup.
		// Connections idle longer than this are closed to free resources,
		// but 90s is long enough for typical request patterns to benefit from reuse.
		IdleConnTimeout: 90 * time.Second,

		// TLSHandshakeTimeout: Maximum time to wait for TLS handshake.
		// Set to 10 seconds to fail fast on TLS issues while allowing
		// enough time for successful handshakes under load.
		TLSHandshakeTimeout: 10 * time.Second,

		// ResponseHeaderTimeout: Maximum time to wait for response headers.
		// Set to 10 seconds to detect slow/unresponsive servers early.
		// This prevents connections from being tied up by slow responses.
		ResponseHeaderTimeout: 10 * time.Second,

		// ExpectContinueTimeout: Maximum time to wait for 100 Continue response.
		// Set to 1 second for fast expect-continue handling.
		// This optimization is used when sending request bodies with
		// Expect: 100-continue header.
		ExpectContinueTimeout: 1 * time.Second,

		// DisableCompression: Set to false to allow gzip compression.
		// Compression reduces bandwidth usage at the cost of CPU.
		// For high-concurrency scenarios, the bandwidth savings typically
		// outweigh the CPU cost.
		DisableCompression: false,

		// ForceAttemptHTTP2: Attempt HTTP/2 even if not advertised by server.
		// HTTP/2 provides multiplexing (multiple requests over one connection),
		// header compression (HPACK), and server push, all of which improve
		// performance for concurrent requests to the same host.
		ForceAttemptHTTP2: true,

		// TLSClientConfig: Uses default TLS configuration with modern cipher suites.
		// The default Go TLS config is secure and performant for most use cases.
		// TLSClientConfig: &tls.Config{...} // Custom config if needed
	}
}
