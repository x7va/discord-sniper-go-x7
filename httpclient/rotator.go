package httpclient

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/proxy"
)

// RateLimitInfo holds information about an active rate limit for timer display.
type RateLimitInfo struct {
	ID      string
	Target  string
	Proxy   string
	EndTime time.Time
}

// Rotator manages round-robin distribution of proxies and tokens.
// It provides thread-safe access to proxy and token pools for high-concurrency scenarios.
type Rotator struct {
	proxies          []string                 // List of proxy URLs loaded from proxies.txt
	tokens           []string                 // List of authorization tokens loaded from tokens.txt
	proxyMu          sync.Mutex               // Mutex for thread-safe proxy access
	tokenMu          sync.Mutex               // Mutex for thread-safe token access
	proxyIdx         int                      // Current index for round-robin proxy selection
	tokenIdx         int                      // Current index for round-robin token selection
	clientCache      map[string]*http.Client  // Cached HTTP clients per proxy URL
	cacheMu          sync.RWMutex             // Mutex for thread-safe cache access
	proxyFailures    map[string]int           // Track failure count per proxy
	proxyMuFail      sync.Mutex               // Mutex for proxy failure tracking
	failureThreshold int                      // Max failures before disabling proxy
	proxyCooldowns   map[string]time.Time     // Track cooldown end times for rate-limited proxies
	cooldownMu       sync.Mutex               // Mutex for cooldown tracking
	activeRateLimits map[string]RateLimitInfo // Track active rate limits for timer display
	rateLimitMu      sync.Mutex               // Mutex for rate limit tracking
	timerRunning     bool                     // Track if timer manager is running
	timerStopChan    chan struct{}            // Channel to stop timer manager
}

// NewRotator creates a new Rotator instance and loads proxies and tokens from files.
// It reads from the specified proxy and token file paths.
// Returns an error if both files are missing or empty.
func NewRotator(proxyFile, tokenFile string) (*Rotator, error) {
	r := &Rotator{
		proxyIdx:         0,
		tokenIdx:         0,
		clientCache:      make(map[string]*http.Client),
		proxyFailures:    make(map[string]int),
		failureThreshold: 3, // Disable proxy after 3 failures
		proxyCooldowns:   make(map[string]time.Time),
		activeRateLimits: make(map[string]RateLimitInfo),
		timerRunning:     false,
		timerStopChan:    make(chan struct{}),
	}

	// Load proxies from file if path is provided
	if proxyFile != "" {
		proxies, err := loadProxies(proxyFile)
		if err == nil && len(proxies) > 0 {
			r.proxies = proxies
		}
	}

	// Load tokens from file if path is provided
	if tokenFile != "" {
		tokens, err := loadTokens(tokenFile)
		if err == nil && len(tokens) > 0 {
			r.tokens = tokens
		}
	}

	// Return error if both are empty/missing
	if len(r.proxies) == 0 && len(r.tokens) == 0 {
		return r, nil // Return rotator anyway for flexibility, but log warnings
	}

	return r, nil
}

// loadProxies reads proxy URLs from a file, supporting http, https, and socks5 formats.
// Each line should contain a single proxy URL in format: protocol://host:port
// Empty lines and lines starting with # are ignored.
func loadProxies(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var proxies []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Validate proxy URL format
		proxyURL, err := url.Parse(line)
		if err != nil {
			continue
		}

		// Check for supported proxy schemes
		scheme := strings.ToLower(proxyURL.Scheme)
		if scheme != "http" && scheme != "https" && scheme != "socks5" && scheme != "socks4" {
			continue
		}

		// For SOCKS4, try to use it as SOCKS5 (many proxies support both)
		if scheme == "socks4" {
			// Convert socks4:// to socks5:// - many proxies support both protocols
			converted := strings.Replace(line, "socks4://", "socks5://", 1)
			proxies = append(proxies, converted)
		} else {
			proxies = append(proxies, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return proxies, nil
}

// loadTokens reads authorization tokens from a file.
// Each line should contain a single token (e.g., Bearer token, API key).
// Empty lines and lines starting with # are ignored.
func loadTokens(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var tokens []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		tokens = append(tokens, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return tokens, nil
}

// NextProxy returns the next proxy URL in round-robin fashion, skipping failed proxies.
// Thread-safe using sync.Mutex to prevent race conditions in concurrent scenarios.
// Returns empty string if no proxies are available.
func (r *Rotator) NextProxy() string {
	r.proxyMu.Lock()
	defer r.proxyMu.Unlock()

	if len(r.proxies) == 0 {
		return ""
	}

	// Try to find a working proxy (skip failed ones and cooldowns)
	attempts := 0
	maxAttempts := len(r.proxies) * 2 // Prevent infinite loop

	for attempts < maxAttempts {
		proxy := r.proxies[r.proxyIdx]
		r.proxyIdx = (r.proxyIdx + 1) % len(r.proxies)

		// Check if proxy has failed too many times
		r.proxyMuFail.Lock()
		failures := r.proxyFailures[proxy]
		r.proxyMuFail.Unlock()

		// Check if proxy is in cooldown (avoid nested locks)
		r.cooldownMu.Lock()
		inCooldown := r.isProxyInCooldownNoLock(proxy)
		r.cooldownMu.Unlock()

		if inCooldown {
			attempts++
			continue
		}

		if failures < r.failureThreshold {
			return proxy
		}

		attempts++
	}

	return "" // All proxies failed
}

// GetCurrentProxy returns the current proxy URL without advancing the index.
// Thread-safe using sync.Mutex to prevent race conditions in concurrent scenarios.
// Returns empty string if no proxies are available.
func (r *Rotator) GetCurrentProxy() string {
	r.proxyMu.Lock()
	defer r.proxyMu.Unlock()

	if len(r.proxies) == 0 {
		return ""
	}

	return r.proxies[r.proxyIdx]
}

// NextToken returns the next token in round-robin fashion.
// Thread-safe using sync.Mutex to prevent race conditions in concurrent scenarios.
// Returns empty string if no tokens are available.
func (r *Rotator) NextToken() string {
	r.tokenMu.Lock()
	defer r.tokenMu.Unlock()

	if len(r.tokens) == 0 {
		return ""
	}

	token := r.tokens[r.tokenIdx]
	r.tokenIdx = (r.tokenIdx + 1) % len(r.tokens)
	return token
}

// GetClientWithProxy returns an http.Client configured with the next proxy from the pool.
// This uses a cached client for each proxy to maintain connection pooling benefits.
// If no proxy is available, returns the base client.
func (r *Rotator) GetClientWithProxy() (*http.Client, error) {
	proxyURL := r.NextProxy()
	if proxyURL == "" {
		// No proxy available, return the base client
		return GetClient(), nil
	}

	// Check cache first
	r.cacheMu.RLock()
	if client, exists := r.clientCache[proxyURL]; exists {
		r.cacheMu.RUnlock()
		return client, nil
	}
	r.cacheMu.RUnlock()

	// Parse the proxy URL
	proxyURLParsed, err := url.Parse(proxyURL)
	if err != nil {
		return nil, err
	}

	// Create a new transport with proxy configuration
	transport := createOptimizedTransport()

	// Handle SOCKS5 proxies with the golang.org/x/net/proxy package
	if proxyURLParsed.Scheme == "socks5" {
		socksDialer, err := proxy.SOCKS5("tcp", proxyURLParsed.Host, nil, &net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		})
		if err != nil {
			return nil, err
		}
		transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return socksDialer.Dial(network, addr)
		}
	} else {
		// For HTTP/HTTPS proxies, use standard proxy
		transport.Proxy = http.ProxyURL(proxyURLParsed)
	}

	// Create a new client with the proxied transport
	client := &http.Client{
		Transport: transport,
		Timeout:   0, // Timeout should be set per-request
	}

	// Cache the client
	r.cacheMu.Lock()
	r.clientCache[proxyURL] = client
	r.cacheMu.Unlock()

	return client, nil
}

// GetClientWithToken returns the base http.Client along with the next token.
// The token should be added to the request's Authorization header.
// This method returns both the client and token for convenience in request building.
func (r *Rotator) GetClientWithToken() (*http.Client, string) {
	token := r.NextToken()
	return GetClient(), token
}

// GetClientWithProxyAndToken returns an http.Client configured with the next proxy
// and the next token from their respective pools.
// This combines both rotation mechanisms for requests that need both proxy and auth.
func (r *Rotator) GetClientWithProxyAndToken() (*http.Client, string, string, error) {
	// Get the current proxy before advancing (for logging purposes)
	currentProxy := r.GetCurrentProxy()
	if currentProxy == "" {
		currentProxy = "direct"
	}

	client, err := r.GetClientWithProxy()
	if err != nil {
		return nil, "", currentProxy, err
	}
	token := r.NextToken()
	return client, token, currentProxy, nil
}

// ProxyCount returns the number of loaded proxies.
func (r *Rotator) ProxyCount() int {
	return len(r.proxies)
}

// TokenCount returns the number of loaded tokens.
func (r *Rotator) TokenCount() int {
	return len(r.tokens)
}

// MarkProxyFailed marks a proxy as failed and increments its failure count.
// If a proxy fails too many times, it will be skipped in NextProxy.
func (r *Rotator) MarkProxyFailed(proxy string) {
	r.proxyMuFail.Lock()
	defer r.proxyMuFail.Unlock()

	r.proxyFailures[proxy]++
	if r.proxyFailures[proxy] >= r.failureThreshold {
		// Remove proxy from list to prevent retries
		r.proxyMu.Lock()
		for i, p := range r.proxies {
			if p == proxy {
				r.proxies = append(r.proxies[:i], r.proxies[i+1:]...)
				break
			}
		}
		r.proxyMu.Unlock()
	}
}

// MarkProxyRateLimited marks a proxy as rate-limited with a cooldown duration.
func (r *Rotator) MarkProxyRateLimited(proxy string, delay time.Duration) {
	r.cooldownMu.Lock()
	defer r.cooldownMu.Unlock()

	r.proxyCooldowns[proxy] = time.Now().Add(delay)
}

// GetProxyCooldownTime returns the cooldown end time for a proxy.
func (r *Rotator) GetProxyCooldownTime(proxy string) time.Time {
	r.cooldownMu.Lock()
	defer r.cooldownMu.Unlock()

	return r.proxyCooldowns[proxy]
}

// isProxyInCooldownNoLock checks if a proxy is currently in cooldown (must hold cooldownMu).
func (r *Rotator) isProxyInCooldownNoLock(proxy string) bool {
	if r.proxyCooldowns == nil {
		return false
	}

	endTime, exists := r.proxyCooldowns[proxy]
	if !exists {
		return false
	}

	// Clean up expired cooldowns
	if time.Now().After(endTime) {
		delete(r.proxyCooldowns, proxy)
		return false
	}

	return true
}

// IsProxyInCooldown checks if a proxy is currently in cooldown.
func (r *Rotator) IsProxyInCooldown(proxy string) bool {
	r.cooldownMu.Lock()
	defer r.cooldownMu.Unlock()
	return r.isProxyInCooldownNoLock(proxy)
}

// CleanExpiredCooldowns removes expired cooldown entries.
func (r *Rotator) CleanExpiredCooldowns() {
	r.cooldownMu.Lock()
	defer r.cooldownMu.Unlock()

	if r.proxyCooldowns == nil {
		return
	}

	now := time.Now()
	for proxy, endTime := range r.proxyCooldowns {
		if now.After(endTime) {
			delete(r.proxyCooldowns, proxy)
		}
	}
}

// TrackRateLimit registers an active rate limit for timer display.
func (r *Rotator) TrackRateLimit(target string, proxy string, duration time.Duration, linePos int) string {
	r.rateLimitMu.Lock()
	defer r.rateLimitMu.Unlock()

	// Generate unique ID to avoid conflicts with same target
	id := fmt.Sprintf("%s-%d", target, time.Now().UnixNano())

	r.activeRateLimits[id] = RateLimitInfo{
		ID:      id,
		Target:  target,
		Proxy:   proxy,
		EndTime: time.Now().Add(duration),
	}

	return id
}

// GetRateLimitRemaining returns the remaining time for a rate limit.
func (r *Rotator) GetRateLimitRemaining(id string) time.Duration {
	r.rateLimitMu.Lock()
	defer r.rateLimitMu.Unlock()

	if r.activeRateLimits == nil {
		return 0
	}

	info, exists := r.activeRateLimits[id]
	if !exists {
		return 0
	}

	remaining := time.Until(info.EndTime)
	if remaining <= 0 {
		delete(r.activeRateLimits, id)
		return 0
	}

	return remaining
}

// RemoveRateLimit removes a completed rate limit.
func (r *Rotator) RemoveRateLimit(id string) {
	r.rateLimitMu.Lock()
	defer r.rateLimitMu.Unlock()

	if r.activeRateLimits == nil {
		return
	}

	delete(r.activeRateLimits, id)
}

// StopTimerManager stops the background timer manager (idempotent).
func (r *Rotator) StopTimerManager() {
	r.rateLimitMu.Lock()
	defer r.rateLimitMu.Unlock()

	if r.timerRunning {
		r.timerRunning = false
		select {
		case <-r.timerStopChan:
			// Already closed
		default:
			close(r.timerStopChan)
		}
	}
}

// StartTimerManager starts the background timer manager for rate limit updates.
func (r *Rotator) StartTimerManager() {
	r.rateLimitMu.Lock()
	if r.timerRunning {
		r.rateLimitMu.Unlock()
		return
	}
	r.timerRunning = true
	r.rateLimitMu.Unlock()

	// Initialize maps if nil
	if r.activeRateLimits == nil {
		r.rateLimitMu.Lock()
		if r.activeRateLimits == nil {
			r.activeRateLimits = make(map[string]RateLimitInfo)
		}
		r.rateLimitMu.Unlock()
	}

	go func() {
		ticker := time.NewTicker(1 * time.Second) // Update every 1 second
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				r.rateLimitMu.Lock()

				if r.activeRateLimits == nil {
					r.rateLimitMu.Unlock()
					continue
				}

				// Make a copy of the map to avoid long lock holding
				rateLimitsCopy := make(map[string]RateLimitInfo)
				for id, info := range r.activeRateLimits {
					rateLimitsCopy[id] = info
				}
				r.rateLimitMu.Unlock()

				for id, info := range rateLimitsCopy {
					remaining := time.Until(info.EndTime)
					if remaining <= 0 {
						r.rateLimitMu.Lock()
						delete(r.activeRateLimits, id)
						r.rateLimitMu.Unlock()
						// Print completion message and newline
						fmt.Printf("%s[RATELIMIT] @%s cooldown complete%s\n", Yellow, info.Target, Reset)
					} else {
						// Don't print updates - just track the cooldown
						// The initial message already shows the time
					}
				}
			case <-r.timerStopChan:
				return
			}
		}
	}()
}
