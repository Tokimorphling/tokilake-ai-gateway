# tokilake

The high-performance tunnel and gateway ecosystem for LLM API proxying.

[![Crates.io](https://img.shields.io/crates/v/tokilake)](https://crates.io/crates/tokilake)
[![License](https://img.shields.io/crates/l/tokilake)](LICENSE)

## Repository

**GitHub**: https://github.com/anomalyco/tokilake/tree/main/rust/tokilake

## Overview

`tokilake` is the main gateway binary that provides WebSocket-based tunnel routing for LLM API proxying. It accepts worker connections via WebSocket, registers their models, and routes chat completion requests through the tunnel to the appropriate worker.

### Key Features

- **WebSocket Worker Connections**: Accepts workers via WebSocket with authentication
- **In-Memory Channel Index**: High-performance routing via `ChannelIndex` (mirrors Go's `ChannelsChooser`)
- **Group-Based Routing**: Primary/backup group fallback support
- **Model Mapping**: Per-channel model name translation (e.g., `gpt-4o` → `gpt-4o-2024-08-06`)
- **Priority & Weighted Selection**: Priority tiers with weighted random channel selection
- **Cooldown Management**: Automatic cooldown for rate-limited channels

## Architecture

```
Client → tokilake (HTTP) → SessionManager → Worker (via tunnel) → Upstream API
         ↓
      AuthService → RouteService → UpstreamService
         ↓
      ChannelIndex (model → channel resolution)
```

### Service Stack

1. **AuthService**: Validates Bearer token against Toasty DB
2. **RouteService**: Resolves model → channel via `ChannelIndex`
3. **UpstreamService**: Forwards request to upstream with model mapping

### Channel Index

The `ChannelIndex` implements Go's `ChannelsChooser` routing algorithm:

- **Exact match** → **Wildcard fallback** (e.g., `gpt-4*` matches `gpt-4o`)
- **Priority tiers** (highest first)
- **Weighted random** within same priority
- **Group isolation** (default, vip, etc.)

## Usage

```bash
# Start the server
cargo run --release --bin tokilake -- -addr :18080 -token sk-your-token
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|--------------|
| `/health` | GET | Health check with session count |
| `/connect` | WebSocket | Worker connection endpoint |
| `/api/tokilake/connect` | WebSocket | Alternative worker endpoint |
| `/v1/chat/completions` | POST | Chat completion relay (requires `namespace` query param) |

## Dependencies

- `tokilake-core`
- `tokilake-smux`
- `toasty` (ORM)
- `axum` (HTTP server, WebSocket)
- `reqwest` (HTTP client)
- `service-async` (service trait)
- `thiserror`
- `rand`

## License

MIT License - see [LICENSE](LICENSE)