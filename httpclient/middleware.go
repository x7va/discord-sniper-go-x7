package httpclient

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MiddlewareConfig holds configuration for response handling and rate limiting.
type MiddlewareConfig struct {
	SuccessStatusCodes   []int         // HTTP status codes considered successful (default: 200)
	MaxErrorRate         int           // Maximum errors per minute before throttling
	RateLimitBackoff     time.Duration // Default backoff when no Retry-After is provided
	EnableAutoRotation   bool          // Automatically rotate proxy/token on rate limits
	StopOnSuccess        bool          // Stop execution on first success
	SuccessIdentifierKey string        // JSON key to extract identifier from response (optional)
}

// ResponseHandler defines the interface for handling HTTP responses.
type ResponseHandler interface {
	HandleSuccess(resp *http.Response, body []byte) (identifier string, shouldStop bool)
	HandleRateLimit(resp *http.Response) (delay time.Duration, shouldRotate bool)
	HandleError(statusCode int, err error) (shouldContinue bool)
}

// ErrorTracker tracks error frequency to prevent permanent blocks.
type ErrorTracker struct {
	errorCount    uint64         // Total error count
	errorCounts   map[int]uint64 // Error counts by status code
	lastResetTime time.Time
	mu            sync.RWMutex
	windowSize    time.Duration // Time window for error rate calculation
}

// NewErrorTracker creates a new error tracker with the specified window size.
func NewErrorTracker(windowSize time.Duration) *ErrorTracker {
	return &ErrorTracker{
		errorCounts:   make(map[int]uint64),
		lastResetTime: time.Now(),
		windowSize:    windowSize,
	}
}

// RecordError records an error with the given status code.
func (et *ErrorTracker) RecordError(statusCode int) {
	et.mu.Lock()
	defer et.mu.Unlock()

	atomic.AddUint64(&et.errorCount, 1)
	et.errorCounts[statusCode]++

	// Reset if window has expired
	if time.Since(et.lastResetTime) > et.windowSize {
		et.reset()
	}
}

// reset clears error counts and resets the timer.
func (et *ErrorTracker) reset() {
	et.errorCount = 0
	et.errorCounts = make(map[int]uint64)
	et.lastResetTime = time.Now()
}

// GetErrorRate returns the current error rate (errors per minute).
func (et *ErrorTracker) GetErrorRate() float64 {
	et.mu.RLock()
	defer et.mu.RUnlock()

	if time.Since(et.lastResetTime) == 0 {
		return 0
	}

	elapsed := time.Since(et.lastResetTime).Minutes()
	if elapsed == 0 {
		return 0
	}

	return float64(atomic.LoadUint64(&et.errorCount)) / elapsed
}

// GetErrorCounts returns a copy of the error counts by status code.
func (et *ErrorTracker) GetErrorCounts() map[int]uint64 {
	et.mu.RLock()
	defer et.mu.RUnlock()

	counts := make(map[int]uint64)
	for code, count := range et.errorCounts {
		counts[code] = count
	}
	return counts
}

// IsRateLimitExceeded checks if the error rate exceeds the threshold.
func (et *ErrorTracker) IsRateLimitExceeded(threshold int) bool {
	return et.GetErrorRate() > float64(threshold)
}

// Middleware implements response handling and rate limiting logic.
type Middleware struct {
	config       MiddlewareConfig
	errorTracker *ErrorTracker
	rotator      *Rotator
	mu           sync.Mutex
}

// NewMiddleware creates a new middleware instance with the given configuration.
func NewMiddleware(config MiddlewareConfig, rotator *Rotator) *Middleware {
	if len(config.SuccessStatusCodes) == 0 {
		config.SuccessStatusCodes = []int{200} // Default to 200 OK
	}
	if config.RateLimitBackoff == 0 {
		config.RateLimitBackoff = 5 * time.Second // Default 5s backoff
	}

	return &Middleware{
		config:       config,
		errorTracker: NewErrorTracker(1 * time.Minute), // 1-minute window
		rotator:      rotator,
	}
}

// HandleSuccess processes successful responses and determines if execution should stop.
// It logs the claimed identifier if configured and returns whether to stop execution.
func (m *Middleware) HandleSuccess(resp *http.Response, body []byte) (identifier string, shouldStop bool) {
	statusCode := resp.StatusCode

	// For Discord username checking, parse the JSON response to check "taken" field
	// Python code doesn't check status codes for this - it processes JSON regardless
	if len(body) > 0 {
		var discordResponse map[string]interface{}
		if err := json.Unmarshal(body, &discordResponse); err == nil {
			// Try to extract username from response for logging
			username := "unknown"
			if uname, exists := discordResponse["username"]; exists {
				if unameStr, ok := uname.(string); ok {
					username = unameStr
				}
			}
			log.Printf("DEBUG: Discord response for username %s: %s", username, string(body))

			// EXACT logic from working Python checker:
			// Only process if "taken" field exists
			if taken, exists := discordResponse["taken"]; exists {
				if takenBool, ok := taken.(bool); ok {
					if !takenBool {
						// Username is available (taken = false)
						log.Printf("SUCCESS: Username is available (taken=false)")
						return "available", m.config.StopOnSuccess
					} else {
						// Username is taken (taken = true)
						log.Printf("INFO: Username is taken (taken=true)")
						return "taken", false
					}
				}
			} else {
				// If "taken" field doesn't exist, it's an error (per Python logic)
				if message, exists := discordResponse["message"]; exists {
					log.Printf("ERROR: Error validating username: %v", message)
				} else {
					log.Printf("ERROR: Unknown response format - no 'taken' field")
				}
				return "error", false
			}
		} else {
			log.Printf("ERROR: Failed to parse Discord response: %v, raw body: %s", err, string(body))
			return "error", false
		}
	}

	// Fallback to original behavior for non-Discord responses
	// Check if this is a success status code
	for _, code := range m.config.SuccessStatusCodes {
		if statusCode == code {
			if m.config.SuccessIdentifierKey != "" && len(body) > 0 {
				identifier = m.extractIdentifier(body, m.config.SuccessIdentifierKey)
			} else {
				identifier = fmt.Sprintf("status_%d", statusCode)
			}

			log.Printf("SUCCESS: Status %d - Identifier: %s", statusCode, identifier)
			return identifier, m.config.StopOnSuccess
		}
	}

	// Return empty identifier for non-success status codes
	return "", false
}

// HandleRateLimit processes rate limit responses (429) and Retry-After headers.
// It parses the delay value and determines if proxy/token rotation should occur.
func (m *Middleware) HandleRateLimit(resp *http.Response) (delay time.Duration, shouldRotate bool) {
	if resp.StatusCode != http.StatusTooManyRequests {
		return 0, false
	}

	// Parse Retry-After header
	retryAfter := resp.Header.Get("Retry-After")
	if retryAfter != "" {
		// Retry-After can be either seconds (number) or HTTP date
		if seconds, err := strconv.Atoi(retryAfter); err == nil {
			delay = time.Duration(seconds) * time.Second
			log.Printf("RATE LIMIT: Retry-After header indicates %d second delay", seconds)
		} else {
			// Try parsing as HTTP date
			if retryTime, err := http.ParseTime(retryAfter); err == nil {
				delay = time.Until(retryTime)
				if delay < 0 {
					delay = 0
				}
				log.Printf("RATE LIMIT: Retry-After header indicates delay until %v", retryTime)
			} else {
				log.Printf("RATE LIMIT: Invalid Retry-After header '%s', using default backoff", retryAfter)
				delay = m.config.RateLimitBackoff
			}
		}
	} else {
		// No Retry-After header, use default backoff
		delay = m.config.RateLimitBackoff
		log.Printf("RATE LIMIT: No Retry-After header, using default %v backoff", delay)
	}

	// Record the rate limit error
	m.errorTracker.RecordError(http.StatusTooManyRequests)

	// Determine if rotation should occur
	shouldRotate = m.config.EnableAutoRotation
	if shouldRotate {
		log.Printf("RATE LIMIT: Triggering proxy/token rotation")
	}

	return delay, shouldRotate
}

// HandleError processes error responses and determines if execution should continue.
// It tracks error frequency and enforces safe thresholds.
func (m *Middleware) HandleError(statusCode int, err error) (shouldContinue bool) {
	// Record the error
	m.errorTracker.RecordError(statusCode)

	// Check if error rate exceeds threshold (but don't stop execution, just warn)
	if m.config.MaxErrorRate > 0 && m.errorTracker.IsRateLimitExceeded(m.config.MaxErrorRate) {
		log.Printf("WARNING: Error rate %.2f/min exceeds max %d/min (continuing)",
			m.errorTracker.GetErrorRate(), m.config.MaxErrorRate)
		// Don't stop execution - continue despite high error rate
	}

	// Log the error
	if err != nil {
		log.Printf("ERROR: Status %d - %v", statusCode, err)
	} else {
		log.Printf("ERROR: Status %d", statusCode)
	}

	// Always continue execution - let user decide when to stop
	return true
}

// extractIdentifier attempts to extract a value from JSON body using a key path.
// This is a simple implementation; for complex JSON, consider using a JSON parser.
func (m *Middleware) extractIdentifier(body []byte, key string) string {
	bodyStr := string(body)

	// This is a basic implementation - for production use, use encoding/json
	// Find the pattern in the body
	if idx := strings.Index(bodyStr, `"`+key+`"`); idx != -1 {
		// Look for the value after the key
		start := idx + len(key) + 3 // Skip past "key":
		if start < len(bodyStr) {
			// Find the closing quote
			if end := strings.Index(bodyStr[start:], `"`); end != -1 {
				return bodyStr[start : start+end]
			}
		}
	}

	return fmt.Sprintf("response_%d", len(body))
}

// ApplyDelay pauses the current goroutine for the specified duration.
// This is called when rate limiting is detected to respect server limits.
func (m *Middleware) ApplyDelay(delay time.Duration) {
	if delay > 0 {
		log.Printf("Applying rate limit delay: %v", delay)
		time.Sleep(delay)
	}
}

// RotateCredentials forces a rotation of proxy and token.
// This is called when rate limiting is detected to use fresh credentials.
func (m *Middleware) RotateCredentials() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.rotator != nil {
		// Rotate to next proxy and token
		proxy := m.rotator.NextProxy()
		token := m.rotator.NextToken()
		log.Printf("Credentials rotated - Proxy count: %d, Token count: %d",
			m.rotator.ProxyCount(), m.rotator.TokenCount())

		// Print proxy switch notification
		if proxy != "" {
			truncatedProxy := proxy
			if len(proxy) > 20 {
				truncatedProxy = proxy[:20] + "..."
			}
			PrintProxySwitch("Rotated to proxy: %s", truncatedProxy)
		}
		if token != "" {
			truncatedToken := token
			if len(token) > 10 {
				truncatedToken = token[:10] + "..."
			}
			PrintProxySwitch("Rotated to token: %s", truncatedToken)
		}
	}
}

// ProcessResponse is the main entry point for middleware response processing.
// It handles success, rate limits, and errors in a single call.
func (m *Middleware) ProcessResponse(resp *http.Response, body []byte, err error) (identifier string, shouldStop bool, shouldRotate bool, delay time.Duration) {
	if err != nil {
		// Handle request error
		shouldContinue := m.HandleError(0, err)
		return "", !shouldContinue, false, 0
	}

	statusCode := resp.StatusCode

	// Check for rate limit FIRST (like Python code)
	if statusCode == http.StatusTooManyRequests {
		delay, shouldRotate := m.HandleRateLimit(resp)
		return "", false, shouldRotate, delay
	}

	// Process Discord username response regardless of status code
	// Python code processes JSON response for username checking regardless of status
	identifier, shouldStopSuccess := m.HandleSuccess(resp, body)
	if identifier != "" {
		// If we got an identifier (available, taken, or error), use it
		return identifier, shouldStopSuccess, false, 0
	}

	// Handle other error status codes (if not already handled above)
	if statusCode >= 400 {
		shouldContinue := m.HandleError(statusCode, nil)
		return "", !shouldContinue, false, 0
	}

	// Non-success, non-error response (e.g., 3xx redirects)
	return "", false, false, 0
}

// GetErrorStats returns current error tracking statistics.
func (m *Middleware) GetErrorStats() map[string]interface{} {
	return map[string]interface{}{
		"total_errors":       atomic.LoadUint64(&m.errorTracker.errorCount),
		"error_rate_per_min": m.errorTracker.GetErrorRate(),
		"error_counts":       m.errorTracker.GetErrorCounts(),
		"window_size":        m.errorTracker.windowSize,
	}
}

// ResetErrorTracking clears all error tracking data.
func (m *Middleware) ResetErrorTracking() {
	m.errorTracker.reset()
	log.Println("Error tracking reset")
}
