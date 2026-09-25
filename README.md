# Discord Username Sniper

A high-performance Discord username availability checker written in Go. Built for speed with concurrent execution, proxy rotation, and intelligent rate-limit handling.

---

## Features

- **High-Concurrency**: Worker pool pattern with goroutines for maximum throughput
- **Proxy Rotation**: Supports HTTP/HTTPS/SOCKS5/SOCKS4 with automatic failover
- **Token Rotation**: Authorization token rotation for authenticated requests
- **Rate Limit Defense**: Automatic backoff on 429 responses with `Retry-After` header parsing
- **Smart Proxy Management**: Health tracking, cooldown tracking, and automatic proxy removal
- **Discord Validation**: Built-in username validation (2-32 chars, alphanumeric + underscores)
- **Clean Output**: Color-coded terminal output with real-time statistics
- **Username Generation**: Random username generation with Discord validation

---

## Installation

### Prerequisites
- Go 1.21 or higher

### Build from Source
```bash
git clone https://github.com/x7va/discord-sniper-go-x7
cd discord-sniper-go-x7
go build -o sniper main.go
```

### Run Directly
```bash
go run main.go
```

---

## Quick Start

### Interactive Mode
```bash
./sniper
# or
go run main.go
```

The program will prompt you for:
- Proxy usage (y/n)
- Token usage (y/n)
- Username generation vs file loading
- Worker count
- Stop on first success

### Generate Usernames
```bash
go run cmd/generator/main.go
```
This creates `targets.txt` with valid Discord usernames.

---

## Configuration

### Input Files

**proxies.txt** - One proxy per line:
```
http://proxy1.example.com:8080
https://proxy2.example.com:8443
socks5://proxy3.example.com:1080
socks4://proxy4.example.com:1080
```

**tokens.txt** - One token per line:
```
Bearer token1_here
Bearer token2_here
```

**targets.txt** - One username per line:
```
username1
username2
username3
```

### Config File (config.json)
```json
{
  "workers": 20,
  "method": "POST",
  "timeout": 60,
  "use_proxies": true,
  "proxy_file": "proxies.txt",
  "use_tokens": true,
  "token_file": "tokens.txt",
  "generate_usernames": false,
  "username_count": 100,
  "username_length": 4,
  "target_file": "targets.txt",
  "base_url": "https://discord.com/api/v9/unique-username/username-attempt-unauthed",
  "request_delay": 3,
  "success_codes": [200],
  "max_error_rate": 200,
  "rate_limit_backoff": 5,
  "auto_rotation": true,
  "stop_on_success": false
}
```

---

## Output Format

The tool displays results in real-time:

```
Available] username, RPS : 18 / s, resp : {'taken': False}, proxy : proxy.example.com:8080
Taken] username2, RPS : 18 / s, resp : {'taken': True}, proxy : proxy.example.com:8080
[RATELIMIT] Rate limited on @username3 - back in 5s (proxy: proxy.example.com:8080)
[ERROR] Proxy error: connection refused
```

**Color Coding:**
- 🟢 Green: Available usernames
- 🔴 Red: Taken usernames
- 🟡 Yellow: Rate limits
- 🔴 Red: Errors

---

## Architecture

```
├── main.go              # Entry point & interactive configuration
├── generator/
│   └── generator.go     # Username generation
├── cmd/
│   └── generator/
│       └── main.go     # Generator CLI
├── httpclient/
│   ├── transport.go    # Optimized HTTP transport
│   ├── rotator.go      # Proxy/token rotation
│   ├── sniper.go       # Core execution engine
│   ├── middleware.go   # Response processing
│   └── ui.go           # Terminal UI
├── config.json          # Configuration file
├── proxies.txt         # Proxy list
├── tokens.txt          # Authorization tokens
└── targets.txt         # Target usernames
```

---

## Performance Tuning

### High Speed
```bash
# Use 100+ workers with good proxy pool
./sniper
# Select: proxies=y, tokens=y, workers=100
```

### Conservative
```bash
# Use fewer workers for reliability
./sniper
# Select: proxies=y, tokens=y, workers=5, timeout=60
```

### Without Proxies
```bash
# Direct connection with rate limiting
./sniper
# Select: proxies=n, workers=10
```

---

## Rate Limit Handling

The system automatically handles rate limits by:
1. Detecting HTTP 429 responses
2. Parsing `Retry-After` headers
3. Applying backoff delays
4. Rotating to fresh proxy/token pairs
5. Tracking proxy cooldowns
6. Skipping rate-limited proxies during cooldown

---

## Proxy Management

### Proxy Health
- Proxies that fail 3+ times are automatically removed
- Rate-limited proxies are tracked in cooldown
- Cooldowned proxies are skipped until available

### Proxy Rotation
- Round-robin distribution
- Skips failed and cooldowned proxies
- HTTP clients cached per proxy for connection reuse

---

## Safety Features

- Thread-safe operations throughout
- Graceful shutdown on Ctrl+C
- Configurable error thresholds
- Automatic connection cleanup
- Secure token truncation in logs
- Discord username validation

---

## Troubleshooting

### "go: command not found"
Install Go from https://golang.org/dl/

### Connection errors
- Check proxy file format
- Verify network connectivity
- Ensure proxies are operational

### Rate limiting
- Increase timeout value
- Add more proxies
- Reduce worker count

### High memory usage
- Reduce worker count
- Check for connection leaks

---

## Contributing

Contributions welcome! Please:
- Follow Go best practices
- Maintain thread-safety
- Add tests for new features
- Update documentation

---

## License

This project is provided as-is for educational and research purposes.

---

## Disclaimer

This tool is designed for legitimate testing and research purposes only. Users are responsible for ensuring compliance with applicable laws, terms of service, and ethical guidelines.
