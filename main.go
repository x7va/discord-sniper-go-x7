package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"sniper/httpclient"
)

// Config holds the application configuration
type Config struct {
	Workers           int                    `json:"workers"`
	Method            string                 `json:"method"`
	Timeout           int                    `json:"timeout"`
	UseProxies        bool                   `json:"use_proxies"`
	ProxyFile         string                 `json:"proxy_file"`
	UseTokens         bool                   `json:"use_tokens"`
	TokenFile         string                 `json:"token_file"`
	GenerateUsernames bool                   `json:"generate_usernames"`
	UsernameCount     int                    `json:"username_count"`
	UsernameLength    int                    `json:"username_length"`
	TargetFile        string                 `json:"target_file"`
	BaseURL           string                 `json:"base_url"`      // Base URL for constructing full target URLs
	RequestDelay      int                    `json:"request_delay"` // Delay between requests in seconds (when no proxies)
	Payload           map[string]interface{} `json:"payload"`
	Headers           map[string]string      `json:"headers"`
	SuccessCodes      []int                  `json:"success_codes"`
	MaxErrorRate      int                    `json:"max_error_rate"`
	RateLimitBackoff  int                    `json:"rate_limit_backoff"`
	AutoRotation      bool                   `json:"auto_rotation"`
	StopOnSuccess     bool                   `json:"stop_on_success"`
	IdentifierKey     string                 `json:"identifier_key"`
}

func main() {
	// Interactive configuration menu (no logo)
	config := interactiveConfig()

	// Validate configuration
	if err := validateConfig(&config); err != nil {
		httpclient.PrintCritical("Configuration error: %v", err)
		os.Exit(1)
	}

	// Step 1: Load configuration files with graceful error handling
	httpclient.PrintStatus("Loading configuration files...")

	// Check if required files exist based on user selection
	if config.UseProxies {
		if err := checkFileExists(config.ProxyFile); err != nil {
			httpclient.PrintStatus("Warning: %v (continuing without proxies)", err)
			config.UseProxies = false
		}
	}
	if config.UseTokens {
		if err := checkFileExists(config.TokenFile); err != nil {
			httpclient.PrintStatus("Warning: %v (continuing without tokens)", err)
			config.UseTokens = false
		}
	}

	// Step 2: Initialize shared HTTP transport, thread-safe rotator, and rate-limit middleware
	httpclient.PrintStatus("Initializing rotator...")

	// Determine file paths based on config
	proxyFile := ""
	tokenFile := ""
	if config.UseProxies {
		proxyFile = config.ProxyFile
	}
	if config.UseTokens {
		tokenFile = config.TokenFile
	}

	rotator, err := httpclient.NewRotator(proxyFile, tokenFile)
	if err != nil {
		httpclient.PrintCritical("Failed to initialize rotator: %v", err)
		os.Exit(1)
	}

	if config.UseProxies {
		httpclient.PrintInfo("Loaded %d proxies", rotator.ProxyCount())
	} else {
		httpclient.PrintInfo("Proxy rotation disabled")
	}

	if config.UseTokens {
		httpclient.PrintInfo("Loaded %d tokens", rotator.TokenCount())
	} else {
		httpclient.PrintInfo("Token rotation disabled")
	}

	// Create middleware configuration for rate-limit defense
	middlewareConfig := httpclient.MiddlewareConfig{
		SuccessStatusCodes:   config.SuccessCodes,
		MaxErrorRate:         config.MaxErrorRate,
		RateLimitBackoff:     time.Duration(config.RateLimitBackoff) * time.Second,
		EnableAutoRotation:   config.AutoRotation && config.UseProxies,
		StopOnSuccess:        config.StopOnSuccess,
		SuccessIdentifierKey: config.IdentifierKey,
	}

	middleware := httpclient.NewMiddleware(middlewareConfig, rotator)

	// Step 3: Define target API endpoint URL, HTTP method, and JSON payload structure
	httpclient.PrintStatus("Configuring HTTP client...")
	httpclient.PrintInfo("HTTP Method: %s", config.Method)
	httpclient.PrintInfo("Request Timeout: %d seconds", config.Timeout)
	httpclient.PrintInfo("Base URL: %s", config.BaseURL)
	if len(config.Payload) > 0 {
		httpclient.PrintInfo("Payload: %+v", config.Payload)
	}
	if len(config.Headers) > 0 {
		httpclient.PrintInfo("Custom Headers: %d", len(config.Headers))
	}

	// Create sniper configuration
	sniperConfig := httpclient.SniperConfig{
		WorkerCount:    config.Workers,
		HTTPMethod:     config.Method,
		JSONPayload:    config.Payload,
		RequestTimeout: time.Duration(config.Timeout) * time.Second,
		Headers:        config.Headers,
		Middleware:     middleware,
		BaseURL:        config.BaseURL,
		RequestDelay:   config.RequestDelay,
	}

	// Step 4: Spin up concurrent worker pool via sniper.go using buffered channels and goroutines
	httpclient.PrintStatus("Initializing sniper with %d workers...", config.Workers)
	sniper := httpclient.NewSniper(sniperConfig, rotator)

	// Set proxy usage based on actual availability
	sniper.SetProxyUsage(config.UseProxies && rotator.ProxyCount() > 0)

	// Generate or load targets
	if config.GenerateUsernames {
		httpclient.PrintStatus("Generating %d random usernames...", config.UsernameCount)
		usernames := generateRandomUsernames(config.UsernameCount, config.UsernameLength)
		sniper.SetTargets(usernames)
		httpclient.PrintInfo("Generated %d usernames", len(usernames))
	} else {
		// Load targets from file
		httpclient.PrintStatus("Loading targets from %s...", config.TargetFile)
		if err := sniper.LoadTargets(config.TargetFile); err != nil {
			httpclient.PrintCritical("Failed to load targets: %v", err)
			os.Exit(1)
		}
	}

	// Step 5: Launch real-time terminal UI dashboard in concurrent ticker routine
	// The dashboard is automatically started by sniper.Execute() and runs in background
	httpclient.PrintStatus("Starting real-time dashboard...")
	httpclient.PrintInfo("Dashboard will display: checks, availability, claims, errors, rate limits, req/s")

	// Step 6: Handle graceful shutdown (SIGINT/Ctrl+C) to print summary stats before exiting
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start execution (this runs in background with dashboard)
	sniper.Execute()

	// Process results in real-time (non-blocking)
	resultCount := 0
	done := make(chan bool)

	go func() {
		for range sniper.Results() {
			resultCount++

			// Results are logged by the worker goroutines via dashboard
			// This channel just tracks completion
		}
		done <- true
	}()

	// Wait for completion or interrupt
	select {
	case <-done:
		httpclient.PrintSuccess("Execution completed successfully")
	case <-sigChan:
		httpclient.PrintStatus("Received interrupt signal, shutting down gracefully...")
		httpclient.PrintInfo("Processed %d results before shutdown", resultCount)
		time.Sleep(500 * time.Millisecond) // Allow dashboard to final update
	}

	// Print final summary statistics
	printFinalStats(sniper)
}

// interactiveConfig provides an interactive menu for configuration
func interactiveConfig() Config {
	config := Config{
		Workers:          10,
		Method:           "POST",
		Timeout:          30, // Increased timeout for slow proxies
		BaseURL:          "https://discord.com/api/v9/users/@me/pomelo-attempt",
		ProxyFile:        "proxies.txt",
		TokenFile:        "tokens.txt",
		TargetFile:       "targets.txt",
		UsernameCount:    100,
		UsernameLength:   6, // Minimum 2 for Discord validation
		RequestDelay:     3, // Default 3 second delay when no proxies
		SuccessCodes:     []int{200},
		MaxErrorRate:     200, // Increased threshold to be more lenient
		RateLimitBackoff: 5,
		AutoRotation:     true,
		StopOnSuccess:    false, // Don't stop on first success for username checking
		IdentifierKey:    "",
		Payload: map[string]interface{}{
			"username": "TARGET_PLACEHOLDER",
		},
		Headers: map[string]string{
			"Orgin": "https://discord.com/",
		},
	}

	fmt.Println("\n=== Interactive Configuration ===")

	// Proxy selection
	fmt.Print("Use proxies? (y/n): ")
	var useProxies string
	fmt.Scanln(&useProxies)
	config.UseProxies = (useProxies == "y" || useProxies == "Y")

	// Token selection
	fmt.Print("Use tokens? (y/n): ")
	var useTokens string
	fmt.Scanln(&useTokens)
	config.UseTokens = (useTokens == "y" || useTokens == "Y")

	// Username generation vs file
	fmt.Print("Generate random usernames? (y/n): ")
	var generateUsernames string
	fmt.Scanln(&generateUsernames)
	config.GenerateUsernames = (generateUsernames == "y" || generateUsernames == "Y")

	if config.GenerateUsernames {
		fmt.Print("Number of usernames to generate: ")
		fmt.Scanln(&config.UsernameCount)
		fmt.Print("Username length (characters, min 2 for Discord): ")
		fmt.Scanln(&config.UsernameLength)
		if config.UsernameLength < 2 {
			config.UsernameLength = 2 // Minimum for Discord
			fmt.Println("Username length set to minimum of 2 for Discord compatibility")
		}
	}

	// Worker count
	fmt.Printf("Worker count (default %d): ", config.Workers)
	var workers int
	fmt.Scanln(&workers)
	if workers > 0 {
		config.Workers = workers
	}

	// Stop on success
	fmt.Print("Stop on first success? (y/n): ")
	var stopOnSuccess string
	fmt.Scanln(&stopOnSuccess)
	config.StopOnSuccess = (stopOnSuccess == "y" || stopOnSuccess == "Y")

	fmt.Println("\n=== Configuration Summary ===")
	fmt.Printf("Proxies: %v\n", config.UseProxies)
	fmt.Printf("Tokens: %v\n", config.UseTokens)
	if config.GenerateUsernames {
		fmt.Printf("Generate %d usernames of length %d\n", config.UsernameCount, config.UsernameLength)
	} else {
		fmt.Printf("Load targets from: %s\n", config.TargetFile)
	}
	fmt.Printf("Workers: %d\n", config.Workers)
	fmt.Printf("Stop on success: %v\n", config.StopOnSuccess)
	if !config.UseProxies {
		fmt.Printf("Request delay: %d seconds (no proxy mode)\n", config.RequestDelay)
	}
	fmt.Println()

	return config
}

// generateRandomUsernames generates random usernames of specified length
func generateRandomUsernames(count, length int) []string {
	charset := "abcdefghijklmnopqrstuvwxyz0123456789_"

	usernames := make([]string, 0, count)
	attempts := 0
	maxAttempts := count * 10 // Prevent infinite loop

	for len(usernames) < count && attempts < maxAttempts {
		attempts++

		username := make([]byte, length)
		for j := 0; j < length; j++ {
			username[j] = charset[rand.Intn(len(charset))]
		}

		usernameStr := string(username)

		// Validate Discord username requirements
		if isValidDiscordUsername(usernameStr) {
			usernames = append(usernames, usernameStr)
		}
	}

	if len(usernames) < count {
		httpclient.PrintStatus("Warning: Only generated %d valid usernames out of %d requested", len(usernames), count)
	}

	return usernames
}

// isValidDiscordUsername validates a username against Discord's requirements:
// - 2-32 characters
// - Alphanumeric (a-z, A-Z, 0-9) and underscores only
// - Cannot start or end with underscore
// - No consecutive underscores
func isValidDiscordUsername(username string) bool {
	if len(username) < 2 || len(username) > 32 {
		return false
	}

	// Check first and last character
	if username[0] == '_' || username[len(username)-1] == '_' {
		return false
	}

	prevWasUnderscore := false
	for _, char := range username {
		// Check if character is valid
		if !((char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '_') {
			return false
		}

		// Check for consecutive underscores
		if char == '_' {
			if prevWasUnderscore {
				return false
			}
			prevWasUnderscore = true
		} else {
			prevWasUnderscore = false
		}
	}

	return true
}

// loadConfigFromFile loads configuration from a JSON file
func loadConfigFromFile(filename string) Config {
	file, err := os.Open(filename)
	if err != nil {
		httpclient.PrintCritical("Failed to open config file: %v", err)
		os.Exit(1)
	}
	defer file.Close()

	var config Config
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		httpclient.PrintCritical("Failed to parse config file: %v", err)
		os.Exit(1)
	}

	return config
}

// validateConfig validates the configuration parameters
func validateConfig(config *Config) error {
	if config.Workers <= 0 {
		return fmt.Errorf("workers must be greater than 0")
	}
	if config.Workers > 1000 {
		httpclient.PrintStatus("Warning: High worker count (%d) may impact system performance", config.Workers)
	}

	validMethods := map[string]bool{
		"GET": true, "POST": true, "PATCH": true, "PUT": true, "DELETE": true,
	}
	if !validMethods[config.Method] {
		return fmt.Errorf("invalid HTTP method: %s", config.Method)
	}

	if config.Timeout <= 0 {
		return fmt.Errorf("timeout must be greater than 0")
	}

	if config.UseProxies && config.ProxyFile == "" {
		return fmt.Errorf("proxy file path cannot be empty when using proxies")
	}

	if config.UseTokens && config.TokenFile == "" {
		return fmt.Errorf("token file path cannot be empty when using tokens")
	}

	if !config.GenerateUsernames && config.TargetFile == "" {
		return fmt.Errorf("target file path cannot be empty when not generating usernames")
	}

	if config.GenerateUsernames && config.UsernameCount <= 0 {
		return fmt.Errorf("username count must be greater than 0")
	}

	if config.GenerateUsernames && config.UsernameLength < 2 {
		return fmt.Errorf("username length must be at least 2 for Discord compatibility")
	}

	// BaseURL is required for Discord
	if config.BaseURL == "" {
		return fmt.Errorf("base URL is required for Discord API")
	}

	return nil
}

// checkFileExists checks if a file exists and returns an error if it doesn't
func checkFileExists(filename string) error {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return fmt.Errorf("file '%s' does not exist", filename)
	}
	return nil
}

// printFinalStats prints final execution statistics
func printFinalStats(sniper *httpclient.Sniper) {
	httpclient.PrintSection("Final Statistics")

	// Get metrics snapshot
	snapshot := sniper.GetMetrics()
	if snapshot != nil {
		httpclient.PrintInfo("Total Checks: %d", snapshot["total_checks"])
		httpclient.PrintInfo("Available Status: %d", snapshot["available_status"])
		httpclient.PrintInfo("Successful Claims: %d", snapshot["successful_claims"])
		httpclient.PrintInfo("Errors: %d", snapshot["errors"])
		httpclient.PrintInfo("Rate Limits: %d", snapshot["rate_limits"])
		httpclient.PrintInfo("Proxy Switches: %d", snapshot["proxy_switches"])
		httpclient.PrintInfo("Requests/Second: %.2f", snapshot["requests_per_second"])
		httpclient.PrintInfo("Elapsed Time: %.2f seconds", snapshot["elapsed_seconds"])
	} else {
		httpclient.PrintInfo("Metrics not available")
	}

	httpclient.PrintInfo("Execution completed. Check logs above for detailed results.")
	httpclient.PrintInfo("Press Ctrl+C to exit if dashboard is still running")
}
