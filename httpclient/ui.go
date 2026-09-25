package httpclient

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// ANSI color codes for terminal output
const (
	Reset   = "\033[0m"
	Red     = "\033[31m"
	Green   = "\033[32m"
	Yellow  = "\033[33m"
	Blue    = "\033[34m"
	Magenta = "\033[35m"
	Cyan    = "\033[36m"
	White   = "\033[37m"
	Bold    = "\033[1m"
)

// DashboardMetrics holds real-time metrics for the terminal dashboard.
type DashboardMetrics struct {
	TotalChecks      atomic.Uint64 // Total requests attempted
	AvailableStatus  atomic.Uint64 // Available targets (200 OK)
	SuccessfulClaims atomic.Uint64 // Successful claims with identifiers
	Errors           atomic.Uint64 // Total errors encountered
	RateLimits       atomic.Uint64 // Rate limit responses (429)
	ProxySwitches    atomic.Uint64 // Proxy rotation events
	CurrentTarget    atomic.Value  // Current target being processed (string)
	StartTime        time.Time     // When execution started
}

// Dashboard manages the real-time terminal UI with metrics and status display.
type Dashboard struct {
	metrics    *DashboardMetrics
	enabled    bool
	ticker     *time.Ticker
	done       chan bool
	statusLine string
}

// NewDashboard creates a new dashboard instance with the given metrics.
func NewDashboard(metrics *DashboardMetrics) *Dashboard {
	return &Dashboard{
		metrics: metrics,
		enabled: true,
		done:    make(chan bool),
	}
}

// Start begins the real-time dashboard updates with the specified refresh interval.
func (d *Dashboard) Start(refreshInterval time.Duration) {
	if !d.enabled {
		return
	}

	d.ticker = time.NewTicker(refreshInterval)
	d.metrics.StartTime = time.Now()

	go d.updateLoop()
}

// Stop gracefully stops the dashboard updates and clears the status line.
func (d *Dashboard) Stop() {
	if !d.enabled {
		return
	}

	if d.ticker != nil {
		d.ticker.Stop()
	}

	d.done <- true
	d.clearStatusLine()
	d.enabled = false
}

// updateLoop runs the main dashboard update loop.
func (d *Dashboard) updateLoop() {
	for {
		select {
		case <-d.ticker.C:
			d.updateStatusLine()
		case <-d.done:
			return
		}
	}
}

// updateStatusLine updates the persistent status line at the bottom of the terminal.
func (d *Dashboard) updateStatusLine() {
	// Calculate metrics
	totalChecks := d.metrics.TotalChecks.Load()
	claims := d.metrics.SuccessfulClaims.Load()
	errors := d.metrics.Errors.Load()
	rateLimits := d.metrics.RateLimits.Load()

	// Calculate requests per second
	elapsed := time.Since(d.metrics.StartTime).Seconds()
	var reqPerSec float64
	if elapsed > 0 {
		reqPerSec = float64(totalChecks) / elapsed
	}

	// Get current target
	currentTarget := "Idle"
	if ct := d.metrics.CurrentTarget.Load(); ct != nil {
		if target, ok := ct.(string); ok && target != "" {
			// Truncate target if too long
			if len(target) > 20 {
				currentTarget = target[:17] + "..."
			} else {
				currentTarget = target
			}
		}
	}

	// Build detailed status line matching the example UI
	statusLine := fmt.Sprintf(
		"\r\033[K%s[LIVE] %d (%.1f/s) | Taken: %d | 429: %d | Errors: %d | %s (0ms)%s",
		Cyan, totalChecks, reqPerSec, claims, rateLimits, errors, "@"+currentTarget, Reset,
	)

	// Move to bottom of terminal and print status line
	d.moveToBottom()
	fmt.Print(statusLine)
	d.statusLine = statusLine
}

// getTerminalHeight returns the current terminal height.
// Uses environment variables for portability across platforms.
func getTerminalHeight() int {
	// Try LINES environment variable (common on Unix)
	if lines := os.Getenv("LINES"); lines != "" {
		var height int
		if _, err := fmt.Sscanf(lines, "%d", &height); err == nil && height > 0 {
			return height
		}
	}

	// Try to parse from stty or similar commands as fallback
	// For now, use a sensible default
	return 24
}

// moveToBottom moves the cursor to the bottom of the terminal.
func (d *Dashboard) moveToBottom() {
	height := getTerminalHeight()
	// Move cursor to last line of terminal
	fmt.Printf("\033[%d;0H", height)
}

// clearStatusLine clears the status line and resets cursor position.
func (d *Dashboard) clearStatusLine() {
	d.moveToBottom()
	fmt.Print("\r\033[K")
	fmt.Print("\033[0m") // Reset colors
}

// UpdateTarget updates the current target being processed.
func (d *Dashboard) UpdateTarget(target string) {
	d.metrics.CurrentTarget.Store(target)
}

// Color-coded print functions for different log levels

// PrintSuccess prints a success message in green with timestamp.
func PrintSuccess(format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("%s%s [SUCCESS] %s%s\n", White, timestamp, Green+msg, Reset)
}

// PrintAvailability prints an availability message in green with timestamp.
func PrintAvailability(format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("%s%s [AVAILABLE] %s%s\n", White, timestamp, Green+msg, Reset)
}

// PrintRateLimit prints a rate limit message in yellow with timestamp.
func PrintRateLimit(format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("%s%s [RATELIMIT] %s%s\n", White, timestamp, Yellow+msg, Reset)
}

// PrintProxySwitch prints a proxy switch message in yellow with timestamp.
func PrintProxySwitch(format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("%s%s [PROXY] %s%s\n", White, timestamp, Yellow+msg, Reset)
}

// PrintError prints an error message in red with timestamp.
func PrintError(format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("%s%s [ERROR] %s%s\n", White, timestamp, Red+msg, Reset)
}

// PrintCritical prints a critical error message in red with bold and timestamp.
func PrintCritical(format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("%s%s [CRITICAL] %s%s%s\n", White, timestamp, Red+Bold, msg, Reset)
}

// PrintStatus prints a status message in cyan with timestamp.
func PrintStatus(format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("%s%s [INFO] %s%s\n", White, timestamp, Cyan+msg, Reset)
}

// PrintInfo prints an informational message in white with timestamp.
func PrintInfo(format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("%s%s [INFO] %s%s\n", White, timestamp, White+msg, Reset)
}

// PrintProgress prints a progress indicator.
func PrintProgress(current, total int, message string) {
	percentage := float64(current) / float64(total) * 100
	barWidth := 30
	filled := int(percentage / 100 * float64(barWidth))

	bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
	fmt.Printf("%s[%s] %s%d/%d (%.1f%%)%s %s\n",
		Cyan, bar, White, current, total, percentage, Reset, message)
}

// ClearLine clears the current line and moves cursor to start.
func ClearLine() {
	fmt.Print("\r\033[K")
}

// MoveUp moves the cursor up by the specified number of lines.
func MoveUp(lines int) {
	fmt.Printf("\033[%dA", lines)
}

// MoveDown moves the cursor down by the specified number of lines.
func MoveDown(lines int) {
	fmt.Printf("\033[%dB", lines)
}

// SaveCursor saves the current cursor position.
func SaveCursor() {
	fmt.Print("\033[s")
}

// RestoreCursor restores the saved cursor position.
func RestoreCursor() {
	fmt.Print("\033[u")
}

// HideCursor hides the cursor (useful for animations).
func HideCursor() {
	fmt.Print("\033[?25l")
}

// ShowCursor shows the cursor again.
func ShowCursor() {
	fmt.Print("\033[?25h")
}

// ClearScreen clears the entire terminal screen.
func ClearScreen() {
	fmt.Print("\033[2J")
	fmt.Print("\033[H")
}

// PrintHeader prints a formatted header section.
func PrintHeader(title string) {
	width := 60
	border := strings.Repeat("═", width)
	padding := (width - len(title)) / 2
	if padding < 0 {
		padding = 0
	}

	fmt.Printf("%s%s\n", Cyan, border)
	fmt.Printf("%s%s%s%s%s\n", Cyan, strings.Repeat(" ", padding), Bold, title, Reset+Cyan)
	fmt.Printf("%s%s\n\n", Cyan, border)
}

// PrintSection prints a section divider.
func PrintSection(title string) {
	fmt.Printf("\n%s%s%s\n", Bold, title, Reset)
	fmt.Printf("%s\n", strings.Repeat("─", len(title)))
}

// PrintTable prints a simple formatted table.
func PrintTable(headers []string, rows [][]string) {
	if len(headers) == 0 || len(rows) == 0 {
		return
	}

	// Calculate column widths
	colWidths := make([]int, len(headers))
	for i, header := range headers {
		colWidths[i] = len(header)
	}
	for _, row := range rows {
		for i, cell := range row {
			if len(cell) > colWidths[i] {
				colWidths[i] = len(cell)
			}
		}
	}

	// Print header
	headerLine := ""
	for i, header := range headers {
		headerLine += fmt.Sprintf("%-*s", colWidths[i]+2, header)
	}
	fmt.Printf("%s%s%s\n", Bold+Cyan, headerLine, Reset)

	// Print separator
	separator := ""
	for _, width := range colWidths {
		separator += strings.Repeat("─", width+2) + " "
	}
	fmt.Printf("%s%s\n", Cyan, separator)

	// Print rows
	for _, row := range rows {
		rowLine := ""
		for i, cell := range row {
			rowLine += fmt.Sprintf("%-*s", colWidths[i]+2, cell)
		}
		fmt.Printf("%s\n", rowLine)
	}
}

// LogWithTimestamp prints a message with timestamp.
func LogWithTimestamp(level, format string, args ...interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	msg := fmt.Sprintf(format, args...)

	var color string
	switch strings.ToUpper(level) {
	case "SUCCESS", "AVAIL":
		color = Green
	case "RATELIMIT", "PROXY":
		color = Yellow
	case "ERROR", "CRITICAL":
		color = Red
	case "STATUS", "INFO":
		color = Cyan
	default:
		color = White
	}

	fmt.Printf("%s[%s]%s %s%s\n", White, timestamp, color, msg, Reset)
}

// FormatNumber formats a number with thousands separators.
func FormatNumber(n uint64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1000000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	return fmt.Sprintf("%.1fM", float64(n)/1000000)
}

// FormatDuration formats a duration in a human-readable way.
func FormatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.1fm", d.Minutes())
	}
	return fmt.Sprintf("%.1fh", d.Hours())
}

// PrintBanner prints a startup banner.
func PrintBanner() {
	fmt.Printf("%s=== SNIPER - High-Performance Username Checker ===%s\n\n", Bold+Cyan, Reset)
}

// Metrics helper methods for atomic operations

// IncrementTotalChecks increments the total checks counter.
func (m *DashboardMetrics) IncrementTotalChecks() {
	m.TotalChecks.Add(1)
}

// IncrementAvailableStatus increments the available status counter.
func (m *DashboardMetrics) IncrementAvailableStatus() {
	m.AvailableStatus.Add(1)
}

// IncrementSuccessfulClaims increments the successful claims counter.
func (m *DashboardMetrics) IncrementSuccessfulClaims() {
	m.SuccessfulClaims.Add(1)
}

// IncrementErrors increments the errors counter.
func (m *DashboardMetrics) IncrementErrors() {
	m.Errors.Add(1)
}

// IncrementRateLimits increments the rate limits counter.
func (m *DashboardMetrics) IncrementRateLimits() {
	m.RateLimits.Add(1)
}

// IncrementProxySwitches increments the proxy switches counter.
func (m *DashboardMetrics) IncrementProxySwitches() {
	m.ProxySwitches.Add(1)
}

// GetSnapshot returns a snapshot of current metrics.
func (m *DashboardMetrics) GetSnapshot() map[string]interface{} {
	elapsed := time.Since(m.StartTime).Seconds()
	var reqPerSec float64
	if elapsed > 0 {
		reqPerSec = float64(m.TotalChecks.Load()) / elapsed
	}

	return map[string]interface{}{
		"total_checks":        m.TotalChecks.Load(),
		"available_status":    m.AvailableStatus.Load(),
		"successful_claims":   m.SuccessfulClaims.Load(),
		"errors":              m.Errors.Load(),
		"rate_limits":         m.RateLimits.Load(),
		"proxy_switches":      m.ProxySwitches.Load(),
		"requests_per_second": reqPerSec,
		"elapsed_seconds":     elapsed,
	}
}
