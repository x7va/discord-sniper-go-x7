package httpclient

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Result represents the outcome of a single HTTP request execution.
// It contains the target identifier, HTTP status code, latency in milliseconds,
// and any error that occurred during the request.
type Result struct {
	Target     string        // Target identifier (URL or endpoint)
	Status     int           // HTTP status code (0 if request failed)
	Latency    time.Duration // Request latency in milliseconds
	Error      error         // Error if request failed, nil otherwise
	Proxy      string        // Proxy used for this request (if any)
	Token      string        // Token used for this request (truncated for logging)
	Identifier string        // Extracted identifier from successful response (if available)
}

// SniperConfig holds configuration for the Sniper execution engine.
type SniperConfig struct {
	WorkerCount    int               // Number of concurrent worker goroutines
	HTTPMethod     string            // HTTP method to use (GET, POST, PATCH, PUT, DELETE)
	JSONPayload    interface{}       // JSON payload to send with request (for POST/PATCH/PUT)
	RequestTimeout time.Duration     // Per-request timeout
	Headers        map[string]string // Additional headers to include
	Middleware     *Middleware       // Response handler and rate limiting middleware
	BaseURL        string            // Base URL for constructing full target URLs
	RequestDelay   int               // Delay between requests in seconds (when no proxies)
}

// Sniper is the core execution engine that manages concurrent HTTP request processing.
// It uses a worker pool pattern with goroutines and channels for high-throughput execution.
type Sniper struct {
	config      SniperConfig
	rotator     *Rotator
	targets     []string
	workers     int
	resultsChan chan Result
	wg          sync.WaitGroup
	middleware  *Middleware
	dashboard   *Dashboard
	metrics     *DashboardMetrics
	baseURL     string // Base URL for constructing full target URLs
	useProxies  bool   // Whether proxies are being used
}

// NewSniper creates a new Sniper instance with the given configuration and rotator.
// It initializes the worker pool and results channel based on the configuration.
func NewSniper(config SniperConfig, rotator *Rotator) *Sniper {
	if config.WorkerCount <= 0 {
		config.WorkerCount = 10 // Default to 10 workers
	}
	if config.HTTPMethod == "" {
		config.HTTPMethod = "GET" // Default to GET
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = 30 * time.Second // Default 30s timeout
	}
	if config.RequestDelay == 0 {
		config.RequestDelay = 3 // Default 3 second delay when no proxies
	}

	// Initialize middleware if not provided
	var middleware *Middleware
	if config.Middleware == nil {
		middleware = NewMiddleware(MiddlewareConfig{
			SuccessStatusCodes: []int{200},
			MaxErrorRate:       60, // 60 errors per minute
			RateLimitBackoff:   5 * time.Second,
			EnableAutoRotation: true,
			StopOnSuccess:      false,
		}, rotator)
	} else {
		middleware = config.Middleware
	}

	// Initialize metrics and dashboard
	metrics := &DashboardMetrics{}
	dashboard := NewDashboard(metrics)

	return &Sniper{
		config:      config,
		rotator:     rotator,
		workers:     config.WorkerCount,
		resultsChan: make(chan Result, config.WorkerCount*2), // Buffered channel
		middleware:  middleware,
		dashboard:   dashboard,
		metrics:     metrics,
		baseURL:     config.BaseURL,
		useProxies:  true, // Will be set based on actual proxy availability
	}
}

// LoadTargets reads target URLs from a file (targets.txt).
// Each line should contain a single target URL or endpoint.
// Empty lines and lines starting with # are ignored.
func (s *Sniper) LoadTargets(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open targets file: %w", err)
	}
	defer file.Close()

	var targets []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		targets = append(targets, line)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading targets file: %w", err)
	}

	if len(targets) == 0 {
		return fmt.Errorf("no targets found in %s", filename)
	}

	s.targets = targets
	log.Printf("Loaded %d targets from %s", len(targets), filename)
	return nil
}

// SetTargets directly sets the targets from a slice (for random username generation)
func (s *Sniper) SetTargets(targets []string) {
	s.targets = targets
	log.Printf("Set %d targets for execution", len(targets))
}

// SetProxyUsage sets whether proxies are being used
func (s *Sniper) SetProxyUsage(useProxies bool) {
	s.useProxies = useProxies
	if !useProxies {
		log.Printf("Proxy usage disabled - using %d second delay between requests", s.config.RequestDelay)
	} else {
		log.Printf("Proxy usage enabled - concurrent execution")
	}
}

// Results returns the read-only results channel.
// Consumers can read from this channel to receive request results as they complete.
func (s *Sniper) Results() <-chan Result {
	return s.resultsChan
}

// Execute starts the concurrent execution engine.
// It launches worker goroutines that process targets from the loaded list.
// The method blocks until all targets are processed, then closes the results channel.
func (s *Sniper) Execute() {
	if len(s.targets) == 0 {
		log.Println("Warning: No targets loaded, nothing to execute")
		close(s.resultsChan)
		return
	}

	PrintStatus("Starting execution with %d workers for %d targets", s.workers, len(s.targets))

	// Start the dashboard
	s.dashboard.Start(100 * time.Millisecond)
	defer s.dashboard.Stop()

	// Create a buffered channel for targets
	targetChan := make(chan string, s.workers)

	// Launch worker goroutines
	for i := 0; i < s.workers; i++ {
		s.wg.Add(1)
		go s.worker(targetChan, i)
	}

	// Feed targets to workers
	go func() {
		for _, target := range s.targets {
			targetChan <- target
		}
		close(targetChan)
	}()

	// Wait for all workers to complete
	go func() {
		s.wg.Wait()
		close(s.resultsChan)
		PrintStatus("Execution completed")
	}()
}

// worker is a goroutine that processes targets from the channel.
// Each worker pulls a unique proxy and token pair for each request,
// executes the HTTP request, and sends results down the results channel.
func (s *Sniper) worker(targetChan <-chan string, workerID int) {
	defer s.wg.Done()

	for target := range targetChan {
		// Update dashboard with current target
		s.dashboard.UpdateTarget(target)

		// Add delay if not using proxies (rate limiting)
		if !s.useProxies && s.config.RequestDelay > 0 {
			time.Sleep(time.Duration(s.config.RequestDelay) * time.Second)
		}

		result := s.executeRequest(target, workerID)

		// Update metrics based on result
		s.metrics.IncrementTotalChecks()

		if result.Error == nil {
			if result.Status >= 200 && result.Status < 300 {
				// Check the identifier to determine if username is available or taken
				if result.Identifier == "available" {
					s.metrics.IncrementAvailableStatus()
					s.metrics.IncrementSuccessfulClaims()
					// Detailed success message like the example
					PrintSuccess("CLAIMED @%s in %.4fms via %s", result.Target, float64(result.Latency.Microseconds())/1000, TruncateToken(result.Token, 8))
					fmt.Printf("%s[!] @%s is available!%s\n", Green, result.Target, Reset)
				} else if result.Identifier == "taken" {
					// Username is taken, just log it
					PrintInfo("@%s is taken (%.4fms)", result.Target, float64(result.Latency.Microseconds())/1000)
				} else if result.Identifier == "error" {
					// Discord returned an error response
					s.metrics.IncrementErrors()
					PrintError("Discord API error for @%s", result.Target)
				} else {
					// Generic success response
					s.metrics.IncrementAvailableStatus()
					if result.Identifier != "" {
						s.metrics.IncrementSuccessfulClaims()
						PrintSuccess("CLAIMED @%s in %.4fms via %s", result.Target, float64(result.Latency.Microseconds())/1000, TruncateToken(result.Token, 8))
					} else {
						PrintAvailability("@%s is FREE! Dispatching claim (%.4fms)", result.Target, float64(result.Latency.Microseconds())/1000)
					}
				}
			} else if result.Status == 429 {
				s.metrics.IncrementRateLimits()
				PrintRateLimit("Rate limited on @%s", result.Target)
			} else {
				s.metrics.IncrementErrors()
				PrintError("HTTP %d on @%s", result.Status, result.Target)
			}
		} else {
			s.metrics.IncrementErrors()
			PrintError("Request failed: %v", result.Error)
		}

		s.resultsChan <- result
	}
}

// executeRequest performs a single HTTP request with the given target.
// It measures latency, handles errors, and returns a Result struct.
func (s *Sniper) executeRequest(target string, workerID int) Result {
	startTime := time.Now()

	// Get client with proxy and token
	client, token, proxy, err := s.rotator.GetClientWithProxyAndToken()
	if err != nil {
		return Result{
			Target:  target,
			Status:  0,
			Latency: time.Since(startTime),
			Error:   fmt.Errorf("failed to get client with proxy: %w", err),
			Proxy:   proxy,
		}
	}

	// Use base URL (Discord endpoint is fixed: https://discord.com/api/v9/users/@me)
	fullURL := s.baseURL
	if fullURL == "" {
		fullURL = target // Fallback to target if no base URL
	}

	// Prepare request body with username for Discord
	var body io.Reader
	if s.config.JSONPayload != nil && (s.config.HTTPMethod == "POST" || s.config.HTTPMethod == "PATCH" || s.config.HTTPMethod == "PUT") {
		// Deep copy payload and inject target username
		payloadCopy := make(map[string]interface{})
		if pv, ok := s.config.JSONPayload.(map[string]interface{}); ok {
			for k, v := range pv {
				if strVal, isStr := v.(string); isStr && strVal == "TARGET_PLACEHOLDER" {
					payloadCopy[k] = target // Inject username
				} else {
					payloadCopy[k] = v
				}
			}
		} else {
			// If payload is not a map, use target as username
			payloadCopy["username"] = target
		}

		jsonData, err := json.Marshal(payloadCopy)
		if err != nil {
			return Result{
				Target:  target,
				Status:  0,
				Latency: time.Since(startTime),
				Error:   fmt.Errorf("failed to marshal JSON payload: %w", err),
			}
		}
		body = bytes.NewReader(jsonData)
	}

	// Create HTTP request
	req, err := http.NewRequest(s.config.HTTPMethod, fullURL, body)
	if err != nil {
		return Result{
			Target:  target,
			Status:  0,
			Latency: time.Since(startTime),
			Error:   fmt.Errorf("failed to create request: %w", err),
		}
	}

	// Set headers
	if token != "" {
		req.Header.Set("Authorization", token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range s.config.Headers {
		req.Header.Set(key, value)
	}

	// Set timeout for this specific request
	client.Timeout = s.config.RequestTimeout

	// Execute request
	resp, err := client.Do(req)
	latency := time.Since(startTime)

	if err != nil {
		// Process error through middleware
		if s.middleware != nil {
			_, shouldStop, _, _ := s.middleware.ProcessResponse(nil, nil, err)
			if shouldStop {
				log.Printf("Middleware requested stop due to error threshold")
			}
		}
		return Result{
			Target:  target,
			Status:  0,
			Latency: latency,
			Error:   fmt.Errorf("request failed: %w", err),
		}
	}
	defer resp.Body.Close()

	// Read response body for middleware processing
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Warning: Failed to read response body: %v", err)
		responseBody = []byte{}
	}

	// Process response through middleware
	var identifier string
	if s.middleware != nil {
		identifier, shouldStop, shouldRotate, delay := s.middleware.ProcessResponse(resp, responseBody, nil)

		// Apply rate limiting delay if needed
		if delay > 0 {
			s.middleware.ApplyDelay(delay)
		}

		// Rotate credentials if requested
		if shouldRotate {
			s.middleware.RotateCredentials()
			s.metrics.IncrementProxySwitches()
		}

		// Return early if middleware requests stop
		if shouldStop {
			return Result{
				Target:     target,
				Status:     resp.StatusCode,
				Latency:    latency,
				Error:      nil,
				Proxy:      proxy,
				Token:      TruncateToken(token, 10),
				Identifier: identifier,
			}
		}
	}

	// Return result
	return Result{
		Target:     target,
		Status:     resp.StatusCode,
		Latency:    latency,
		Error:      nil,
		Proxy:      proxy,
		Token:      TruncateToken(token, 10),
		Identifier: identifier,
	}
}

// TruncateToken returns a truncated version of the token for logging purposes.
// This prevents sensitive credentials from appearing in logs.
// Exported to allow use by other packages.
func TruncateToken(token string, maxLen int) string {
	if token == "" {
		return ""
	}
	if len(token) <= maxLen {
		return token
	}
	return token[:maxLen] + "..."
}

// ExecuteWithCallback executes the sniper and calls a callback function for each result.
// This provides a convenient way to handle results without managing the channel directly.
func (s *Sniper) ExecuteWithCallback(callback func(Result)) {
	// Start execution in a goroutine
	go s.Execute()

	// Process results as they come in
	for result := range s.Results() {
		callback(result)
	}
}

// ExecuteAndWait executes the sniper and collects all results into a slice.
// This blocks until all requests are complete and returns all results.
func (s *Sniper) ExecuteAndWait() []Result {
	var results []Result
	var mu sync.Mutex

	// Start execution in a goroutine
	go s.Execute()

	// Collect results with mutex protection
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for result := range s.Results() {
			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}
	}()

	// Wait for all results to be collected
	wg.Wait()
	return results
}

// GetStats returns statistics about the execution results.
// This should be called after ExecuteAndWait() completes.
func GetStats(results []Result) map[string]interface{} {
	if len(results) == 0 {
		return map[string]interface{}{
			"total":       0,
			"success":     0,
			"failed":      0,
			"avg_latency": 0,
		}
	}

	successCount := 0
	failedCount := 0
	totalLatency := time.Duration(0)
	statusCodes := make(map[int]int)

	for _, result := range results {
		if result.Error == nil && result.Status >= 200 && result.Status < 300 {
			successCount++
		} else {
			failedCount++
		}
		totalLatency += result.Latency
		statusCodes[result.Status]++
	}

	avgLatency := totalLatency / time.Duration(len(results))

	return map[string]interface{}{
		"total":        len(results),
		"success":      successCount,
		"failed":       failedCount,
		"avg_latency":  avgLatency.Milliseconds(),
		"status_codes": statusCodes,
	}
}

// GetMetrics returns the dashboard metrics snapshot for the sniper.
func (s *Sniper) GetMetrics() map[string]interface{} {
	if s.metrics == nil {
		return nil
	}
	return s.metrics.GetSnapshot()
}
