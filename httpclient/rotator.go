package httpclient

import (
	"bufio"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
)

// Rotator manages round-robin distribution of proxies and tokens.
// It provides thread-safe access to proxy and token pools for high-concurrency scenarios.
type Rotator struct {
	proxies     []string                // List of proxy URLs loaded from proxies.txt
	tokens      []string                // List of authorization tokens loaded from tokens.txt
	proxyMu     sync.Mutex              // Mutex for thread-safe proxy access
	tokenMu     sync.Mutex              // Mutex for thread-safe token access
	proxyIdx    int                     // Current index for round-robin proxy selection
	tokenIdx    int                     // Current index for round-robin token selection
	clientCache map[string]*http.Client // Cached HTTP clients per proxy URL
	cacheMu     sync.RWMutex            // Mutex for thread-safe cache access
}

// NewRotator creates a new Rotator instance and loads proxies and tokens from files.
// It reads from the specified proxy and token file paths.
// Returns an error if both files are missing or empty.
func NewRotator(proxyFile, tokenFile string) (*Rotator, error) {
	r := &Rotator{
		proxyIdx:    0,
		tokenIdx:    0,
		clientCache: make(map[string]*http.Client),
	}

	// Load proxies from file if path is provided
	if proxyFile != "" {
		proxies, err := loadProxies(proxyFile)
		if err != nil {
			log.Printf("Warning: Failed to load proxies from %s: %v", proxyFile, err)
		} else if len(proxies) == 0 {
			log.Printf("Warning: %s is empty, no proxies will be used", proxyFile)
		} else {
			r.proxies = proxies
			log.Printf("Loaded %d proxies from %s", len(proxies), proxyFile)
		}
	}

	// Load tokens from file if path is provided
	if tokenFile != "" {
		tokens, err := loadTokens(tokenFile)
		if err != nil {
			log.Printf("Warning: Failed to load tokens from %s: %v", tokenFile, err)
		} else if len(tokens) == 0 {
			log.Printf("Warning: %s is empty, no tokens will be used", tokenFile)
		} else {
			r.tokens = tokens
			log.Printf("Loaded %d tokens from %s", len(tokens), tokenFile)
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
			log.Printf("Warning: Invalid proxy URL '%s': %v", line, err)
			continue
		}

		// Check for supported proxy schemes
		scheme := strings.ToLower(proxyURL.Scheme)
		if scheme != "http" && scheme != "https" && scheme != "socks5" {
			log.Printf("Warning: Unsupported proxy scheme '%s' in '%s'", scheme, line)
			continue
		}

		proxies = append(proxies, line)
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

// NextProxy returns the next proxy URL in round-robin fashion.
// Thread-safe using sync.Mutex to prevent race conditions in concurrent scenarios.
// Returns empty string if no proxies are available.
func (r *Rotator) NextProxy() string {
	r.proxyMu.Lock()
	defer r.proxyMu.Unlock()

	if len(r.proxies) == 0 {
		return ""
	}

	proxy := r.proxies[r.proxyIdx]
	r.proxyIdx = (r.proxyIdx + 1) % len(r.proxies)
	return proxy
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
	proxy, err := url.Parse(proxyURL)
	if err != nil {
		return nil, err
	}

	// Create a new transport with proxy configuration
	transport := createOptimizedTransport()
	transport.Proxy = http.ProxyURL(proxy)

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
