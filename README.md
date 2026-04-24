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
- Includes a ready-to-extend **Execution Logic** module (`src/executor.rs`)
  where you can add your own buy / sell rules.

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

> **Where to find these values:**
> - Log in to the [Alchemy dashboard](https://dashboard.alchemy.com/).
> - Create or open a **Solana** app.
> - Copy the **HTTPS** endpoint URL — this is your `GRPC_URL`.
> - In the app settings, navigate to **Yellowstone gRPC** and copy the
>   **X-Token** — this is your `X_TOKEN`.

> **Note:** `.env` is listed in `.gitignore` and will never be committed.

### 3. Compile and run

```bash
cargo run
```

For a release (optimised) build:

```bash
cargo run --release
```

---

## Testing the gRPC connection

Follow these steps to confirm your Alchemy keys are correct and the stream
is flowing before you add any trading logic.

### Step 1 – confirm the bot starts without errors

Run the bot and watch the first few log lines:

```bash
cargo run
```

**You should see (in order):**

```
INFO  alchemy_solana_bot] Alchemy Solana gRPC Bot starting…
INFO  alchemy_solana_bot] [Executor] wallet address: <some pubkey>
INFO  alchemy_solana_bot] Connecting to gRPC endpoint: https://solana-mainnet.g.alchemy.com/...
INFO  alchemy_solana_bot] Connected. Subscribing to account and transaction updates…
INFO  alchemy_solana_bot] Subscription active. Waiting for updates…
```

If you reach **"Subscription active"** the handshake succeeded — your
`GRPC_URL` and `X_TOKEN` are correct.

### Step 2 – confirm live data is arriving

Within a few seconds of "Subscription active" you should see a continuous
stream of lines like:

```
INFO  alchemy_solana_bot] [Transaction] signature=5Xm8vN… slot=287654321
INFO  alchemy_solana_bot] [Account]     pubkey=EPjFWdd… lamports=2039280 slot=287654321
INFO  alchemy_solana_bot] [Transaction] signature=3kZpQr… slot=287654322
```

Solana produces roughly **2 000–4 000 transactions per second** on mainnet,
so lines should appear almost immediately.  If nothing appears after
10 seconds, see the troubleshooting table below.

### Step 3 – press `Ctrl+C` to stop

The bot runs in an infinite reconnect loop by design.  Hit `Ctrl+C` to exit.

### Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `GRPC_URL environment variable not set` | `.env` file missing or not in the project root | Run `cp .env.example .env` and fill in values |
| `Failed to connect to gRPC endpoint` | Wrong URL or no internet | Double-check `GRPC_URL` in `.env`; try opening it in a browser |
| `Stream error: status: Unauthenticated` | Wrong or missing X-Token | Verify `X_TOKEN` matches the value in the Alchemy dashboard |
| `Stream error: status: PermissionDenied` | Yellowstone gRPC not enabled for your app | Enable it in the Alchemy app settings |
| Connected but no data after 10 s | Subscription filter too narrow | Check the filter in `main.rs::build_subscribe_request` |
| Bot keeps reconnecting every 5 s | Persistent server-side error | Check Alchemy status page; reduce subscription scope |

### Increasing log verbosity

If you need to see more detail (e.g. raw gRPC frames), set `RUST_LOG`:

```bash
# Info level – normal operation (default)
RUST_LOG=info cargo run

# Debug level – shows executor dispatch for every event
RUST_LOG=debug cargo run

# Warn/error only – quieter output once you know it's working
RUST_LOG=warn cargo run
```

---

## Project structure

```
alchemy-solana-bot/
├── Cargo.toml          # Dependency manifest
├── .env.example        # Template for GRPC_URL and X_TOKEN
├── src/
│   ├── main.rs         # gRPC connection, subscription stream, reconnect loop
│   └── executor.rs     # ← YOUR TRADING LOGIC GOES HERE (buy/sell hooks)
└── README.md
```

### Where to add your trading logic

Open `src/executor.rs`.  Everything is labelled with `TODO` comments:

| Location | What to add |
|----------|-------------|
| `ExecutorConfig` struct | Extra fields your strategy needs (thresholds, target addresses, …) |
| `Executor::new` | Load your real wallet keypair from disk or env |
| `Executor::on_account_update` | React to account data changes (price feeds, pool reserves, …) |
| `Executor::on_transaction` | React to on-chain transactions (DEX swaps, mints, …) |
| `Executor::send_transaction` | Already implemented – call it from your hooks with your instructions |

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
| `solana-sdk` | Core Solana types: `Keypair`, `Pubkey`, `Transaction`, `Instruction`, … |
| `solana-client` | Non-blocking JSON-RPC client for broadcasting signed transactions |

## How it works

1. On startup the bot reads `GRPC_URL` and `X_TOKEN` from the environment (or
   `.env`).
2. It establishes a TLS-enabled gRPC connection to the Alchemy endpoint,
   injecting the X-Token as a metadata header.
3. A `SubscribeRequest` is sent that selects **all account updates** and **all
   non-vote transactions** at the `Confirmed` commitment level.
4. Incoming `SubscribeUpdate` messages are decoded and printed to stdout.
5. Each update is also forwarded to `Executor::on_account_update` or
   `Executor::on_transaction` — this is where your strategy code runs.
6. If the stream closes or returns an error, the bot waits five seconds and
   reconnects automatically.

