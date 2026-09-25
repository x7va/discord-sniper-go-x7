# High-Performance Concurrent Username Sniper

A lightning-fast, platform-agnostic username sniper engineered in **Go (Golang)**. Built for millisecond-precision execution, high-throughput concurrency via goroutines, and robust rate-limit defense, complete with a real-time ANSI terminal dashboard.

---

## 🚀 Key Features

* **Optimized HTTP Transport**: Utilizes custom transport connection pooling, aggressive keep-alives, and disabled idle timeout overhead to maintain pre-warmed sockets and eliminate handshake latency.
* **Thread-Safe Rotator**: Seamlessly cycles through proxies (HTTP/HTTPS/SOCKS5) and authorization tokens using round-robin distribution with built-in health tracking and client caching.
* **Concurrent Worker Pool**: Scales execution safely using buffered channels and goroutines for multi-threaded target evaluation.
* **Smart Rate-Limit Defense**: Instantly intercepts status codes and `Retry-After` headers to back off throttled workers and rotate dead nodes without crashing.
* **Live Terminal UI Dashboard**: Features a clean, non-blocking status ticker built with ANSI escape codes and carriage returns (`\r`) for real-time telemetry tracking (`checks`, `availability`, `claims`, `errors`, `rate-limits`, and `req/s`).
* **Discord Username Validation**: Built-in validation for Discord username requirements (2-32 chars, alphanumeric + underscores, no leading/trailing underscores).

---

## 📁 Project Architecture

The codebase is organized into a modular structure for maximum maintainability and performance:

```text
├── main.go              # Entry point & orchestration loop with interactive configuration
├── generator/
│   └── generator.go     # Username generation utility with Discord validation
├── cmd/
│   └── generator/
│       └── main.go     # Generator command-line interface
├── httpclient/
│   ├── transport.go    # Optimized HTTP transport & persistent connection pool
│   ├── rotator.go      # Thread-safe proxy and token rotation manager with client caching
│   ├── sniper.go       # Core worker pool and concurrent execution engine
│   ├── middleware.go   # Response error handling, backoff, and rate-limit defense
│   └── ui.go           # Real-time ANSI terminal dashboard and logging
├── config.json          # Example configuration file
├── proxies.txt         # Proxy list (http, https, socks5)
├── tokens.txt          # Authorization tokens
└── targets.txt         # Target usernames (generated or manual)
```

---

## 🛠️ Installation

### Prerequisites
- Go 1.21 or higher
- Basic terminal with ANSI support

### Setup
```bash
# Clone the repository
git clone <repository-url>
cd sniper

# No external dependencies required - uses only Go standard library
# The project is ready to use immediately
```

---

## 📖 Usage

### Basic Usage

```bash
# Run with interactive configuration
go run main.go

# The program will prompt you for:
# - Proxy usage (y/n)
# - Token usage (y/n)
# - Username generation vs file loading
# - Worker count
# - Stop on first success option
```

### Generate Usernames

```bash
# Generate random usernames for Discord (4-character combinations)
go run cmd/generator/main.go

# This creates targets.txt with valid Discord usernames
# Generated usernames follow Discord's validation rules:
# - 2-32 characters
# - Alphanumeric (a-z, A-Z, 0-9) and underscores only
# - Cannot start or end with underscore
# - No consecutive underscores
```

### Configuration File

Create a `config.json` file for advanced configuration:

```json
{
  "workers": 25,
  "method": "PATCH",
  "timeout": 10,
  "proxy_file": "proxies.txt",
  "token_file": "tokens.txt",
  "target_file": "targets.txt",
  "base_url": "https://discord.com/api/v9/users/@me",
  "payload": {
    "username": "TARGET_PLACEHOLDER"
  },
  "headers": {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
  },
  "success_codes": [200],
  "max_error_rate": 50,
  "rate_limit_backoff": 5,
  "auto_rotation": true,
  "stop_on_success": true,
  "identifier_key": "username"
}
```

### Input Files

**proxies.txt** - One proxy per line (supports http, https, socks5):
```
http://proxy1.example.com:8080
https://proxy2.example.com:8443
socks5://proxy3.example.com:1080
# Comments are ignored
```

**tokens.txt** - One authorization token per line:
```
Bearer token1_here
Bearer token2_here
# Comments are ignored
```

**targets.txt** - One username per line (for Discord username checking):
```
username1
username2
username3
# Comments are ignored
```

### Interactive Configuration Options

When running `go run main.go`, you'll be prompted for:

- **Proxy Usage**: Enable/disable proxy rotation
- **Token Usage**: Enable/disable token rotation  
- **Username Generation**: Generate random usernames or load from file
- **Username Count**: Number of usernames to generate (if generation enabled)
- **Username Length**: Length of generated usernames (min 2 for Discord)
- **Worker Count**: Number of concurrent workers (default: 10)
- **Stop on Success**: Stop execution after first successful claim

---

## 🎯 Configuration Options

### Interactive Configuration
The main application uses an interactive configuration menu that prompts for:
- Proxy and token usage
- Username generation vs file loading
- Worker count (1-1000 recommended)
- Request timeout
- Stop on success behavior

### Discord-Specific Settings
- **Base URL**: `https://discord.com/api/v9/users/@me` (configurable)
- **HTTP Method**: PATCH (for username changes)
- **Username Validation**: Built-in Discord username requirements
- **Request Delay**: 3 seconds when no proxies used (rate limiting)

### Default Configuration
```go
Workers:          10
Method:           "PATCH"
Timeout:          30 seconds
BaseURL:          "https://discord.com/api/v9/users/@me"
UsernameLength:   6 characters (minimum 2 for Discord)
RequestDelay:     3 seconds (no proxy mode)
MaxErrorRate:     200 errors/minute
RateLimitBackoff: 5 seconds
AutoRotation:     true
StopOnSuccess:    true
```

---

## 🎨 Terminal Dashboard

The real-time dashboard displays:
- **TARGET**: Current target being processed
- **CHECKS**: Total requests attempted
- **AVAIL**: Available targets (200 OK)
- **CLAIMS**: Successful claims with identifiers
- **ERRORS**: Total errors encountered
- **429s**: Rate limit responses
- **PROXY**: Proxy rotation events
- **REQ/S**: Requests per second

### Color Coding
- 🟢 **Green**: Successes and availability
- 🟡 **Yellow**: Rate limits and proxy switches
- 🔴 **Red**: Critical errors
- 🔵 **Cyan**: Status notes and information

---

## ⚙️ Configuration Options

### Transport Configuration
- **MaxIdleConns**: 200 (total idle connections)
- **MaxIdleConnsPerHost**: 100 (per-host idle connections)
- **IdleConnTimeout**: 90s (connection reuse window)
- **TLSHandshakeTimeout**: 10s (TLS handshake limit)
- **ResponseHeaderTimeout**: 10s (slow server detection)
- **ForceAttemptHTTP2**: true (HTTP/2 multiplexing)

### Middleware Configuration
- **Success Codes**: Configurable (default: 200)
- **Max Error Rate**: 200 errors/minute threshold
- **Rate Limit Backoff**: 5 seconds default
- **Auto Rotation**: Automatic proxy/token rotation on rate limits
- **Stop on Success**: Halt execution on first successful claim

### Performance Optimizations
- **Client Caching**: HTTP clients are cached per proxy URL to maintain connection pooling
- **Dynamic Terminal Height**: Dashboard automatically detects terminal size
- **Concurrent Execution**: Thread-safe operations throughout

---

## 🔧 Performance Tuning

### High Concurrency
```bash
# Generate many usernames and use high worker count
go run cmd/generator/main.go
go run main.go  # Use 100+ workers when prompted
```

### Conservative Settings
```bash
# Use fewer workers and longer timeout
go run main.go  # Use 5 workers, 60 second timeout when prompted
```

### Aggressive Settings
```bash
# Maximum speed with stop on success
go run main.go  # Use 200 workers, 10 second timeout, stop on success
```

---

## 🛡️ Rate Limit Handling

The system automatically handles rate limiting by:
1. Detecting 429 status codes
2. Parsing `Retry-After` headers (seconds or HTTP-date)
3. Applying appropriate backoff delays
4. Rotating to fresh proxy/token pairs
5. Tracking error frequency to prevent permanent blocks

---

## 📊 Error Tracking

Built-in error tracking includes:
- Sliding window error rate monitoring
- Status code distribution tracking
- Automatic threshold enforcement
- Detailed error statistics reporting

---

## 🚨 Safety Features

- Thread-safe operations throughout
- Graceful shutdown on interrupt signals
- Configurable error thresholds
- Automatic connection cleanup
- Secure token truncation in logs
- Discord username validation to prevent invalid requests

---

## 🤝 Contributing

Contributions are welcome! Please ensure:
- Code follows Go best practices
- All functions are documented
- Thread-safety is maintained
- Error handling is comprehensive

---

## 📝 License

This project is provided as-is for educational and research purposes.

---

## ⚠️ Disclaimer

This tool is designed for legitimate testing and research purposes only. Users are responsible for ensuring compliance with applicable laws, terms of service, and ethical guidelines. The authors are not responsible for misuse of this software.

---

## 🐛 Troubleshooting

### Common Issues

**"go: command not found"**
- Install Go from https://golang.org/dl/

**Connection errors**
- Check proxy file format
- Verify network connectivity
- Ensure proxies are operational

**Rate limiting**
- Increase timeout value
- Add more proxies to rotation
- Reduce worker count

**High memory usage**
- Reduce worker count
- Check for connection leaks
- Monitor system resources

**Username generation issues**
- Ensure username length is at least 2 for Discord
- Check that generated usernames meet validation requirements
- Try increasing the generation attempts limit

---

## 📞 Support

For issues, questions, or contributions, please refer to the project repository or contact the maintainers.
