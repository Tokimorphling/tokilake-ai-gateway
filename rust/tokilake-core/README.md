# tokilake-core

The core tunnel and gateway abstraction library for the Tokilake ecosystem.

[![Crates.io](https://img.shields.io/crates/v/tokilake-core)](https://crates.io/crates/tokilake-core)
[![License](https://img.shields.io/crates/l/tokilake-core)](LICENSE)

## Repository

**GitHub**: https://github.com/anomalyco/tokilake/tree/main/rust/tokilake-core

## Overview

`tokilake-core` provides fundamental trait definitions, session management, routing logic, and protocol codecs required to build scalable, multiplexed gateways. It sits between the network transport layer and the application edge.

### Core Modules

- **`tunnel`**: Transport-agnostic tunnel traits (`TunnelSession`, `TunnelStream`) using zero-cost `impl Future` architectures
- **`session`**: Lock-free, concurrent worker registration and namespace claiming via `DashMap`
- **`roundtrip`**: Asynchronous HTTP-over-Tunnel request/response forwarding
- **`protocol`**: NDJSON protocol definitions for control planes
- **`gateway`**: Extensible HTTP/WebSocket handler traits
- **`codec`**: Tunnel data plane codec for request/response serialization
- **`error`**: Error types using `thiserror`

### Supported Transports

- **SMUX**: Backward-compatible with standard `tokilake` workers through `tokilake-smux`
- **QUIC**: Next-generation, zero-RTT capable high-throughput encrypted transport (via `quinn`)
- **Memory**: In-memory stream channels for unit testing

## Integration

```rust
use tokilake_core::session::{SessionManager, WorkerInfo};
use tokilake_core::tunnel::TunnelSession;

// Initialize the global session manager
let session_manager = SessionManager::<tokilake_smux::Session>::new();

// Handle incoming multiplexed streams through unified traits
```

## Key Traits

```rust
/// Tunnel session trait - multiplexed stream container
pub trait TunnelSession: Send + Sync + 'static {
    type Stream: TunnelStream;
    
    fn accept_stream(&mut self) -> impl Future<Output = Result<Option<Self::Stream>> + Send;
    fn open_stream(&mut self) -> impl Future<Output = Result<Self::Stream>> + Send;
    fn close(&self) -> impl Future<Output = Result<()>>;
    fn is_alive(&self) -> bool;
}

/// Authenticator trait - authenticates tunnel worker tokens
pub trait Authenticator: Send + Sync + 'static {
    fn authenticate_token_key(&self, token_key: &str) -> impl Future<Output = Result<(String, Token)>> + Send;
}

/// Worker registry trait - manages worker registration
pub trait WorkerRegistry: Send + Sync + 'static {
    fn register_worker(&self, session_id: u64, namespace: &str, node_name: &str, 
                     group: &str, models: &[String], backend_type: &str) 
        -> impl Future<Output = Result<RegisterResult>> + Send;
}
```

## Dependencies

- `tokilake-smux`
- `tokio` (full features)
- `serde` (derive)
- `serde_json`
- `thiserror`
- `dashmap`
- `parking_lot`
- `tracing`
- `quinn` (optional)
- `base64`

## License

MIT License - see [LICENSE](LICENSE)