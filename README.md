# alchemy-solana-bot

A Solana real-time data bot written in Rust that connects to an
[Alchemy](https://www.alchemy.com/solana) node via the
[Yellowstone gRPC (Geyser)](https://docs.alchemy.com/reference/solana-yellowstone-grpc) streaming
interface and prints live account and transaction updates to the console.

## Features

- Streams **account** and **transaction** updates in real time over gRPC.
- Authenticates with Alchemy using an **X-Token** header.
- Automatically **reconnects** if the stream is interrupted.
- Configuration via a simple `.env` file (no flags required).

## Prerequisites

| Tool | Minimum version |
|------|-----------------|
| [Rust & Cargo](https://rustup.rs/) | 1.75 |

No other system dependencies are required – TLS support is bundled via
`rustls` native roots.

## Quick start

### 1. Clone the repository

```bash
git clone https://github.com/Disseveru/alchemy-solana-bot.git
cd alchemy-solana-bot
```

### 2. Configure credentials

Copy the example environment file and fill in your Alchemy API details:

```bash
cp .env.example .env
```

Edit `.env`:

```dotenv
# Your Alchemy Yellowstone gRPC endpoint
GRPC_URL=https://solana-mainnet.g.alchemy.com/v2/YOUR_ALCHEMY_API_KEY

# The x-token value for authentication
X_TOKEN=YOUR_X_TOKEN_HERE
```

> **Note:** `.env` is listed in `.gitignore` and will never be committed.

### 3. Compile and run

```bash
cargo run
```

For a release (optimised) build:

```bash
cargo run --release
```

You should see output similar to:

```
[2024-01-01T00:00:00Z INFO  alchemy_solana_bot] Alchemy Solana gRPC Bot starting…
[2024-01-01T00:00:00Z INFO  alchemy_solana_bot] Connecting to gRPC endpoint: https://solana-mainnet.g.alchemy.com/...
[2024-01-01T00:00:00Z INFO  alchemy_solana_bot] Connected. Subscribing to account and transaction updates…
[2024-01-01T00:00:00Z INFO  alchemy_solana_bot] Subscription active. Waiting for updates…
[2024-01-01T00:00:00Z INFO  alchemy_solana_bot] [Transaction] signature=5Xm... slot=123456789
[2024-01-01T00:00:00Z INFO  alchemy_solana_bot] [Account] pubkey=EPjF... lamports=1000000 slot=123456789
```

### Adjusting log verbosity

The bot uses the standard `RUST_LOG` environment variable:

```bash
# Show only warnings and errors
RUST_LOG=warn cargo run

# Show all debug output
RUST_LOG=debug cargo run
```

## Project structure

```
alchemy-solana-bot/
├── Cargo.toml        # Workspace manifest and dependency list
├── .env.example      # Template for required environment variables
├── src/
│   └── main.rs       # Bot entry point: connection, subscription, reconnect loop
└── README.md
```

## Key dependencies

| Crate | Purpose |
|-------|---------|
| `yellowstone-grpc-client` | Yellowstone Geyser gRPC client |
| `yellowstone-grpc-proto` | Protobuf definitions for the Geyser API |
| `tokio` | Async runtime |
| `futures` | Stream combinators |
| `serde` / `serde_json` | Data serialisation helpers |
| `dotenvy` | `.env` file loading |
| `anyhow` | Ergonomic error handling |
| `log` / `env_logger` | Structured logging |

## How it works

1. On startup the bot reads `GRPC_URL` and `X_TOKEN` from the environment (or
   `.env`).
2. It establishes a TLS-enabled gRPC connection to the Alchemy endpoint,
   injecting the X-Token as a metadata header.
3. A `SubscribeRequest` is sent that selects **all account updates** and **all
   non-vote transactions** at the `Confirmed` commitment level.
4. Incoming `SubscribeUpdate` messages are decoded and printed to stdout.
5. If the stream closes or returns an error, the bot waits five seconds and
   reconnects automatically.

