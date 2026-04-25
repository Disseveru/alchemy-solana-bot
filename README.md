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
INFO  alchemy_solana_bot] [Executor] initialised | rpc=https://solana-mainnet.g.alchemy.com/... | wallet=<some pubkey>
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

## How to run real arbitrage

> ⚠️ **WARNING: Flash-loan arbitrage involves real financial risk.**
> Failed bundles still pay Jito tips.  Priority fees are burned even when a
> transaction reverts on-chain.  Competition from other MEV bots means profit
> is never guaranteed.  Start with the smallest viable loan amounts and verify
> each step before going live.

### Step-by-step setup

#### 1. Fund the bot wallet

The bot signs every arb transaction and pays Jito tips from this wallet.
You need at least **0.1–0.5 SOL** to cover:
- Jito tips per bundle (default 0.0001 SOL, configurable via `JITO_TIP_LAMPORTS`)
- Transaction priority fees (default 0.04 SOL/tx at 100k µL/CU × 400k CUs)
- Solana base transaction fee (~0.000005 SOL)

Generate a new keypair:
```bash
solana-keygen new --no-bip39-passphrase --outfile wallet.json
solana transfer --from ~/.config/solana/id.json $(solana-keygen pubkey wallet.json) 0.5
```

Export the keypair as base-58 for the `WALLET_PRIVATE_KEY` env var:
```bash
python3 -c "import json, base58; \
    print(base58.b58encode(bytes(json.load(open('wallet.json')))).decode())"
```

Or use the `WALLET_KEY_FILE` env var to point at the JSON file directly:
```env
WALLET_KEY_FILE=/path/to/wallet.json
```

#### 2. Create SPL token accounts

The bot needs pre-existing SPL token accounts to hold tokens during the flash loan.
Create them for each mint involved in your route:

```bash
# Example for the SOL–USDC route (WSOL + USDC mint addresses):
WSOL=So11111111111111111111111111111111111111112
USDC=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
WALLET=$(solana-keygen pubkey wallet.json)

spl-token create-account $WSOL  --owner $WALLET --fee-payer wallet.json
spl-token create-account $USDC  --owner $WALLET --fee-payer wallet.json
```

Set the resulting account addresses in `src/main.rs` in the `ArbRoute` block:
- `our_loan_token_account` – account for the flash-borrowed asset (e.g. WSOL)
- `our_raydium_source` / `our_raydium_dest` – accounts for the Raydium swap leg
- `our_orca_token_a` / `our_orca_token_b` – accounts for the Orca swap leg

#### 3. Verify route addresses

All `ArbRoute` addresses must be verified on-chain before going live.
Check each one with:
```bash
solana account <PUBKEY>
```

Key addresses for the SOL–USDC example:

| Field | Address | Source |
|-------|---------|--------|
| `solend_reserve` | `8PbodeaosQP19SjYFx855UMqWxH2HynZLdBXmsrbac36` | [Solend docs](https://docs.solend.fi/protocol/addresses) |
| `solend_lending_market` | `4UpD2fh7xH3VP9QQaXtsS1YY3bxzWhtfpks7FatyKvdY` | [Solend docs](https://docs.solend.fi/protocol/addresses) |
| `raydium_amm_id` | `58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWaS6E7gexGD6` | [Raydium API](https://api.raydium.io/v2/ammV3/ammPools) |
| `orca_whirlpool` | `HJPjoWUrhoZzkNfRpHuieeFk9WcZWjwy6PBjZ81ngndJ` | [Orca API](https://api.mainnet.orca.so/v1/whirlpool/list) |

For Raydium pool-specific accounts (open orders, coin/PC vaults, Serum market),
fetch the pool state account and decode the layout using the
[Raydium SDK](https://github.com/raydium-io/raydium-sdk-V2) or
[anchor-client](https://docs.rs/anchor-client).

For Orca tick arrays, use the
[Orca Whirlpools SDK](https://github.com/orca-so/whirlpools):
```bash
npx ts-node -e "
  const { WhirlpoolContext, buildWhirlpoolClient } = require('@orca-so/whirlpools-sdk');
  // Derive tick arrays for your pool and swap direction
"
```

#### 4. Configure the environment

Add the following to your `.env`:
```dotenv
WALLET_PRIVATE_KEY=<your-base58-key>   # or WALLET_KEY_FILE=/path/to/wallet.json
RPC_URL=https://solana-mainnet.g.alchemy.com/v2/YOUR_KEY
JITO_BLOCK_ENGINE_URL=https://mainnet.block-engine.jito.wtf:443
ARB_ENABLED=true

# Tune these for current network conditions:
COMPUTE_UNIT_PRICE=200000    # 200k µL/CU during congestion
JITO_TIP_LAMPORTS=200000     # 0.0002 SOL during competition
```

#### 5. Fill in the ArbRoute in main.rs

Open `src/main.rs` and find the `if env::var("ARB_ENABLED") == Ok("true")` block.
Replace every `Pubkey::default()` with the verified on-chain address.

#### 6. Run with verbose logging first

```bash
RUST_LOG=debug cargo run --release
```

Watch for:
- `[BackrunEngine] opportunity | loan=…` – opportunities are being detected
- `[Executor] simulation OK` – pre-flight simulation is passing
- `[BackrunEngine] bundle submitted | uuid=…` – bundles are landing

#### 7. Monitor and tune

- Check the Jito explorer at `https://explorer.jito.wtf/` for landed bundles.
- Increase `COMPUTE_UNIT_PRICE` and `JITO_TIP_LAMPORTS` if bundles aren't landing.
- Decrease `MIN_TARGET_SWAP_LAMPORTS` to capture smaller opportunities (at the
  cost of more compute spent on detections that don't profit).
- Monitor wallet balance regularly — failed arbs drain tips over time.

### Security checklist

- [ ] `WALLET_PRIVATE_KEY` / `WALLET_KEY_FILE` is in `.env`, not committed to git
- [ ] `.env` is in `.gitignore` (it is by default in this repo)
- [ ] Bot wallet is separate from your main Solana wallet
- [ ] All route pubkeys verified with `solana account <PUBKEY>`
- [ ] `ARB_ENABLED=false` in all non-production environments
- [ ] Pre-flight simulation (`executor.simulate_transaction`) is enabled (it is by default)

---



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

