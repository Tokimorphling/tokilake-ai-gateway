# tokilake-smux

A high-performance smux protocol implementation in Rust, optimized for zero overhead and concurrent multiplexing.

[![Crates.io](https://img.shields.io/crates/v/tokilake-smux)](https://crates.io/crates/tokilake-smux)
[![License](https://img.shields.io/crates/l/tokilake-smux)](LICENSE)

## Repository

**GitHub**: https://github.com/anomalyco/tokilake/tree/main/rust/tokilake-smux

## Overview

`tokilake-smux` provides a Rust implementation of the smux protocol, designed for seamless integration within the `tokilake` ecosystem and other high-concurrency Rust network applications.

### Key Features

- **Zero-overhead Design**: Generic over transport layers (`AsyncRead + AsyncWrite`) without dynamic dispatch (`Box<dyn>`)
- **No `async_trait`**: Uses modern Rust's `impl Future` in traits for maximum performance
- **Protocol Compatible**: Fully wire-compatible with Go implementations (v1)
- **Concurrent Multiplexing**: Channel-based internal stream multiplexing via `tokio`

## Usage

```rust
use tokilake_smux::{Session, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stream = tokio::net::TcpStream::connect("127.0.0.1:8080").await?;
    
    // Client-side multiplexed session
    let mut session = Session::client(stream, Config::default());
    
    // Open logical streams over single TCP connection
    let mut stream1 = session.open().await.unwrap();
    let mut stream2 = session.open().await.unwrap();
    
    // Use streams like standard tokio AsyncRead/AsyncWrite
    
    Ok(())
}
```

## Protocol Details

The protocol uses an 8-byte little-endian header format:
```
| VERSION (1B) | CMD (1B) | LENGTH (2B) | STREAM_ID (4B) |
```

Commands: `SYN(0)`, `FIN(1)`, `PSH(2)`, `NOP(3)`

## Dependencies

- `tokio` (full features)
- `futures-util`
- `bytes`
- `serde` (derive)
- `tracing`

## License

MIT License - see [LICENSE](LICENSE)