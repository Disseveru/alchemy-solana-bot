//! # Execution Logic – High-Performance Flash-Loan Arbitrage Executor
//!
//! This module implements a latency-optimised backrunning executor for Solana.
//!
//! ## Key design decisions
//!
//! * **Cached blockhash** – a background task refreshes [`SharedState::latest_blockhash`]
//!   every few slots so that [`Executor::send_bundle`] never calls the RPC in the
//!   hot-path.
//! * **Persistent Jito gRPC channel** – established once in [`Executor::new`]
//!   using lazy connect so startup succeeds even when the block-engine is
//!   temporarily unreachable.
//! * **Fast-path detection** – [`Executor::on_transaction`] checks for known
//!   DEX program IDs in the message account list *before* any expensive parsing,
//!   and skips failed transactions with a single branch.
//! * **No format! in the hot-path** – debug logging uses `debug!` which is
//!   compiled away in release builds; error paths use static strings.
//! * **Compute budget** – every outgoing transaction is prefixed with a
//!   `SetComputeUnitPrice` instruction so it competes effectively under
//!   network congestion.

use {
    anyhow::{Context, Result},
    log::{debug, info, warn},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        compute_budget::ComputeBudgetInstruction,
        hash::Hash,
        instruction::Instruction,
        pubkey::Pubkey,
        signature::{Keypair, Signature},
        signer::Signer,
        transaction::Transaction,
    },
    std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    tokio::sync::RwLock,
    tonic::transport::Channel,
    yellowstone_grpc_proto::prelude::{
        SubscribeUpdateAccountInfo, SubscribeUpdateTransactionInfo,
    },
};

use crate::jito::{
    bundle::Bundle,
    packet::{Meta, Packet},
    searcher::{
        searcher_service_client::SearcherServiceClient, GetTipAccountsRequest, SendBundleRequest,
    },
};

// ─────────────────────────────────────────────────────────────────────────────
// Well-known program IDs used for fast-path detection
// ─────────────────────────────────────────────────────────────────────────────

/// Jupiter v6 aggregator.
const JUPITER_V6_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4");

/// Raydium Liquidity Pool v4.
const RAYDIUM_LIQUIDITY_POOL_V4: Pubkey =
    Pubkey::from_str_const("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8");

/// Orca Whirlpool program.
const ORCA_WHIRLPOOL_PROGRAM: Pubkey =
    Pubkey::from_str_const("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc");

/// Solend lending program.
const SOLEND_PROGRAM: Pubkey =
    Pubkey::from_str_const("So1endD9AkS2mttmbeS8S96ySZps9SML7RnZBjWdLLV");

/// Programs whose accounts we monitor for pool-reserve changes.
///
/// Only account updates whose `owner` matches one of these program IDs will
/// trigger [`Executor::on_account_update`] logic.  This narrows the Geyser
/// fan-out to just the pool/vault accounts we care about.
const WATCHED_PROGRAMS: [Pubkey; 4] = [
    RAYDIUM_LIQUIDITY_POOL_V4,
    ORCA_WHIRLPOOL_PROGRAM,
    SOLEND_PROGRAM,
    JUPITER_V6_PROGRAM_ID,
];

/// Default minimum SOL balance delta (lamports) required to trigger a backrun.
/// 0.1 SOL – tune this based on gas costs and expected profit.
const DEFAULT_MIN_LAMPORTS_DELTA: u64 = 100_000_000;

// ─────────────────────────────────────────────────────────────────────────────
// Shared network state
// ─────────────────────────────────────────────────────────────────────────────

/// Thread-safe network state updated by a background task.
///
/// `on_transaction` reads this state without any network I/O.
pub struct SharedState {
    /// Most recently confirmed blockhash.  Starts as [`Hash::default`] until
    /// the background refresher completes its first call.
    pub latest_blockhash: RwLock<Hash>,
    /// Slot of the most recently observed block.
    pub current_slot: AtomicU64,
}

impl SharedState {
    fn new() -> Self {
        Self {
            latest_blockhash: RwLock::new(Hash::default()),
            current_slot: AtomicU64::new(0),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Strategy-level configuration.
#[derive(Debug)]
pub struct ExecutorConfig {
    /// Solana JSON-RPC endpoint used for blockhash refresh, simulation, and
    /// fallback transaction broadcast.
    ///
    /// **Recommendation:** Use your private Alchemy endpoint for lower latency:
    /// `https://solana-mainnet.g.alchemy.com/v2/YOUR_API_KEY`
    pub rpc_url: String,

    /// Jito Block Engine gRPC endpoint.
    pub jito_url: String,

    /// Minimum SOL balance delta (lamports) to trigger a backrun attempt.
    pub min_lamports: u64,

    /// Compute unit price in microlamports per compute unit (priority fee).
    ///
    /// Higher values improve inclusion priority under network congestion.
    /// At 100 000 µ-lamports/CU and 400 000 CUs total priority fee ≈ 0.04 SOL.
    /// Override via the `COMPUTE_UNIT_PRICE` env var.
    pub compute_unit_price: u64,

    /// Hard cap on compute units consumed per arb transaction.
    ///
    /// Flash loan + 2 DEX swaps typically consume 150 000–250 000 CUs.
    /// 400 000 gives headroom for complex pools.
    pub compute_unit_limit: u32,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            rpc_url: std::env::var("RPC_URL")
                .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string()),
            jito_url: std::env::var("JITO_BLOCK_ENGINE_URL")
                .unwrap_or_else(|_| "https://mainnet.block-engine.jito.wtf:443".to_string()),
            min_lamports: DEFAULT_MIN_LAMPORTS_DELTA,
            compute_unit_price: std::env::var("COMPUTE_UNIT_PRICE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100_000),
            compute_unit_limit: 400_000,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Executor
// ─────────────────────────────────────────────────────────────────────────────

/// High-performance flash-loan arbitrage executor.
///
/// `main.rs` creates one instance wrapped in `Arc<Executor>` and passes every
/// incoming Geyser event to [`on_account_update`] / [`on_transaction`].
///
/// [`on_account_update`]: Executor::on_account_update
/// [`on_transaction`]: Executor::on_transaction
pub struct Executor {
    config: ExecutorConfig,
    /// Non-blocking RPC client – used by the background blockhash refresher,
    /// pre-flight simulation, and the fallback [`send_transaction`].
    ///
    /// [`send_transaction`]: Executor::send_transaction
    rpc: Arc<RpcClient>,
    /// Signing keypair for outgoing transactions.
    wallet: Arc<Keypair>,
    /// Cached network state updated by the background task.
    pub state: Arc<SharedState>,
    /// Pre-initialised Jito Block Engine searcher client.
    /// Tonic clients are cheaply cloneable – the underlying channel is shared.
    jito_client: SearcherServiceClient<Channel>,
    /// Minimum lamport delta to consider an opportunity worth backrunning.
    min_lamports: u64,
}

impl Executor {
    /// Construct a new `Executor`, establishing the Jito gRPC channel.
    ///
    /// The channel uses lazy connect so startup succeeds even when the block
    /// engine endpoint is temporarily unreachable.
    ///
    /// ## Wallet loading (in priority order)
    ///
    /// 1. **`WALLET_PRIVATE_KEY`** env var – base-58 encoded 64-byte secret key.
    ///
    ///    Generate with `solana-keygen` and export as base-58:
    ///    ```bash
    ///    solana-keygen new --no-bip39-passphrase --outfile wallet.json
    ///    # Export as base-58 (requires `pip install base58`):
    ///    python3 -c "import json,base58; \
    ///        print(base58.b58encode(bytes(json.load(open('wallet.json')))).decode())"
    ///    ```
    ///
    /// 2. **`WALLET_KEY_FILE`** env var – path to a Solana JSON keypair file.
    ///    ```env
    ///    WALLET_KEY_FILE=/home/user/.config/solana/id.json
    ///    ```
    ///
    /// 3. **Ephemeral keypair** – generated at startup with a loud warning.
    ///    DO NOT use in production; the keypair is lost when the process exits.
    ///
    /// > **Security**: Never log, print, or commit private keys.
    /// > The env var value is read once at startup and never echoed.
    pub async fn new(config: ExecutorConfig) -> Result<Self> {
        let rpc = Arc::new(RpcClient::new(config.rpc_url.clone()));

        // ── Wallet loading ────────────────────────────────────────────────────
        let wallet: Arc<Keypair> = if let Ok(secret) = std::env::var("WALLET_PRIVATE_KEY") {
            let key_bytes = bs58::decode(&secret)
                .into_vec()
                .context("WALLET_PRIVATE_KEY contains invalid base-58")?;
            let kp = Keypair::try_from(key_bytes.as_slice()).map_err(|e| {
                anyhow::anyhow!("Invalid keypair bytes in WALLET_PRIVATE_KEY: {:?}", e)
            })?;
            // Log only the public key — never the private key bytes.
            info!(
                "[Executor] wallet loaded from WALLET_PRIVATE_KEY | pubkey={}",
                kp.pubkey()
            );
            Arc::new(kp)
        } else if let Ok(path) = std::env::var("WALLET_KEY_FILE") {
            let kp = solana_sdk::signature::read_keypair_file(&path)
                .map_err(|e| anyhow::anyhow!("Failed to read keypair file '{}': {}", path, e))?;
            info!(
                "[Executor] wallet loaded from {} | pubkey={}",
                path,
                kp.pubkey()
            );
            Arc::new(kp)
        } else {
            warn!(
                "[Executor] WALLET_PRIVATE_KEY and WALLET_KEY_FILE not set – \
                 using ephemeral keypair. This wallet has NO funds and CANNOT \
                 submit live transactions. Set one of these env vars before going live."
            );
            Arc::new(Keypair::new())
        };

        // Lazy Jito gRPC channel: connects on the first RPC call, not at startup.
        let channel = Channel::from_shared(config.jito_url.clone())
            .context("Invalid JITO_BLOCK_ENGINE_URL")?
            .connect_lazy();
        let jito_client = SearcherServiceClient::new(channel);

        info!(
            "[Executor] initialised | rpc={} | jito={} | wallet={} | \
             compute_price={} µL/CU | compute_limit={} CU",
            config.rpc_url,
            config.jito_url,
            wallet.pubkey(),
            config.compute_unit_price,
            config.compute_unit_limit,
        );

        let min_lamports = config.min_lamports;
        Ok(Self {
            config,
            rpc,
            wallet,
            state: Arc::new(SharedState::new()),
            jito_client,
            min_lamports,
        })
    }

    // ── Wallet accessor ───────────────────────────────────────────────────────

    /// Return a clone of the bot's signing keypair handle.
    ///
    /// The returned `Arc<Keypair>` shares the same heap allocation – there is
    /// no key copy.  Pass this to [`crate::arb::BackrunEngine::new`] so that
    /// both the executor and the arb engine sign with the same wallet.
    pub fn wallet(&self) -> Arc<Keypair> {
        Arc::clone(&self.wallet)
    }

    // ── Cached state helpers ──────────────────────────────────────────────────

    /// Update the cached slot counter.  Called by `main.rs` on every slot event.
    pub fn update_slot(&self, slot: u64) {
        self.state.current_slot.store(slot, Ordering::Relaxed);
    }

    /// Fetch the latest blockhash from the RPC endpoint and cache it.
    ///
    /// This should be called from a **background task**, not inside the
    /// transaction hot-path.
    pub async fn refresh_blockhash(&self) -> Result<()> {
        let bh = self.rpc.get_latest_blockhash().await?;
        *self.state.latest_blockhash.write().await = bh;
        debug!("[Executor] blockhash refreshed: {}", bh);
        Ok(())
    }

    /// Return a cheap clone of the pre-initialised Jito gRPC client.
    ///
    /// Tonic clients clone by reference-counting the underlying channel, so
    /// this does not create a new connection.  Use the returned client to open
    /// additional streams (e.g. `subscribe_mempool`) without contending on a
    /// shared mutex.
    pub fn jito_client(&self) -> crate::jito::searcher::searcher_service_client::SearcherServiceClient<Channel> {
        self.jito_client.clone()
    }

    // ── Event hooks ───────────────────────────────────────────────────────────

    /// Called once for every account update.
    ///
    /// Add pool-reserve or price-feed monitoring logic here.
    pub async fn on_account_update(
        &self,
        account: &SubscribeUpdateAccountInfo,
        slot: u64,
    ) -> Result<()> {
        let pubkey = match Pubkey::try_from(account.pubkey.as_slice()) {
            Ok(pk) => pk,
            Err(e) => {
                warn!(
                    "[Executor] skipping account update with invalid pubkey ({} bytes): {:?}",
                    account.pubkey.len(),
                    e
                );
                return Ok(());
            }
        };

        // ── Filter: only process accounts owned by watched programs ───────────
        let owner = Pubkey::try_from(account.owner.as_slice()).unwrap_or_default();
        let is_watched = WATCHED_PROGRAMS.iter().any(|p| *p == owner);
        if !is_watched {
            return Ok(());
        }

        debug!(
            "[Executor] pool account update | pubkey={} owner={} data_len={} slot={}",
            pubkey,
            owner,
            account.data.len(),
            slot
        );

        // ══════════════════════════════════════════════════════════════════════
        // TODO: Decode account.data to update cached pool reserves.
        //
        //  ✦  Match `pubkey` against your ArbRoute pool/vault addresses.
        //  ✦  Decode reserve balances using the offsets documented below.
        //  ✦  Store in Arc<RwLock<HashMap<Pubkey, (u64, u64)>>> on the executor.
        //  ✦  BackrunEngine::detect_opportunity can then read live reserves for
        //     accurate profit estimation via arb::amm_output().
        //
        // Raydium AMM v4 pool-state layout (partial):
        //   offset 253: coin_vault_balance (u64 LE) — base-token reserve
        //   offset 261: pc_vault_balance   (u64 LE) — quote-token reserve
        //
        // Example:
        //   if owner == RAYDIUM_LIQUIDITY_POOL_V4 && account.data.len() >= 269 {
        //       let coin = u64::from_le_bytes(account.data[253..261].try_into()?);
        //       let pc   = u64::from_le_bytes(account.data[261..269].try_into()?);
        //       self.pool_reserves.write().await.insert(pubkey, (coin, pc));
        //   }
        //
        // Orca Whirlpool pool-state layout (partial):
        //   offset  65: sqrt_price (u128 LE) — current pool price (Q64.64 format)
        //   offset 101: liquidity  (u128 LE) — total active liquidity
        // ══════════════════════════════════════════════════════════════════════

        Ok(())
    }

    /// Called once for every transaction update – the hot-path entry point.
    ///
    /// Applies fast-path guards in order of cheapest first:
    /// 1. Skip failed transactions.
    /// 2. Skip transactions that do not touch a known DEX program.
    /// 3. Skip transactions whose maximum SOL balance delta is below the
    ///    configured threshold (unless a swap log is present as corroborating signal).
    /// 4. Scan `meta.log_messages` for swap keywords as a secondary signal.
    ///
    /// No network I/O is performed inside this method.
    ///
    /// ## Note on timing
    ///
    /// This hook receives **confirmed** transactions from the Geyser stream –
    /// they are already finalized on-chain.  Backrunning a confirmed trade
    /// requires submitting a bundle that lands in the **next** block, which is
    /// only viable when price discrepancies persist across slots (~400 ms).
    ///
    /// For lower-latency backrunning use [`crate::arb::BackrunEngine`], which
    /// subscribes to the Jito mempool and sees transactions before confirmation.
    pub async fn on_transaction(
        &self,
        tx: &SubscribeUpdateTransactionInfo,
        slot: u64,
    ) -> Result<()> {
        // ── Guard 1: skip failed transactions immediately ─────────────────────
        let meta = match tx.meta.as_ref() {
            Some(m) if m.err.is_none() => m,
            _ => return Ok(()),
        };

        // ── Guard 2: fast-path DEX program check ─────────────────────────────
        // Check the message account list for target DEX program IDs.  This is
        // O(n) over the (typically small) account list with zero allocations.
        let is_target_dex = tx
            .transaction
            .as_ref()
            .and_then(|t| t.message.as_ref())
            .map(|msg| {
                msg.account_keys.iter().any(|k| {
                    let arr: [u8; 32] = k.as_slice().try_into().unwrap_or([0u8; 32]);
                    let pk = Pubkey::from(arr);
                    pk == JUPITER_V6_PROGRAM_ID
                        || pk == RAYDIUM_LIQUIDITY_POOL_V4
                        || pk == ORCA_WHIRLPOOL_PROGRAM
                })
            })
            .unwrap_or(false);

        if !is_target_dex {
            return Ok(());
        }

        // ── Guard 3: minimum SOL balance delta ───────────────────────────────
        let largest_delta = meta
            .pre_balances
            .iter()
            .zip(meta.post_balances.iter())
            .map(|(pre, post)| pre.abs_diff(*post))
            .max()
            .unwrap_or(0);

        // ── Guard 4: scan log messages for swap keywords ──────────────────────
        // "Instruction: Swap" / "SwapBaseIn" appear in Raydium/Orca program logs.
        // This secondary signal catches swaps on newer instruction variants.
        let has_swap_log = meta
            .log_messages
            .iter()
            .any(|log| log.contains("Swap") || log.contains("swap"));

        // Require either a large-enough delta OR a swap log.
        if largest_delta < self.min_lamports && !has_swap_log {
            return Ok(());
        }

        debug!(
            "[Executor] confirmed DEX swap | slot={} delta={} lamports has_swap_log={}",
            slot, largest_delta, has_swap_log
        );

        // ══════════════════════════════════════════════════════════════════════
        // TODO: Optionally submit a next-block backrun bundle here.
        //
        // These are *confirmed* trades.  A next-block bundle is viable when:
        //   - The price discrepancy between two pools persists for ≥ 1 slot.
        //   - Competition is low enough that a 400 ms reaction time can profit.
        //
        // Requires Arc<crate::arb::ArbRoute> stored in the Executor config.
        // Example skeleton (add `arb_route: Option<Arc<crate::arb::ArbRoute>>`
        // to ExecutorConfig and the Executor struct):
        //
        //   if let Some(route) = &self.arb_route {
        //       if let Some(opp) = crate::arb::estimate(largest_delta, route) {
        //           match self.send_bundle(opp.instructions()).await {
        //               Ok(uuid) => info!("[Executor] next-block bundle uuid={}", uuid),
        //               Err(e)   => warn!("[Executor] bundle failed: {:#}", e),
        //           }
        //       }
        //   }
        // ══════════════════════════════════════════════════════════════════════

        Ok(())
    }

    // ── Bundle submission ─────────────────────────────────────────────────────

    /// Wrap `instructions` in a signed [`Transaction`], pack it into a Jito
    /// [`Bundle`], and submit it to the Block Engine.
    ///
    /// Uses the **cached** blockhash – zero network I/O in the submission path.
    ///
    /// Every transaction is automatically prefixed with two compute budget
    /// instructions:
    /// 1. `SetComputeUnitPrice` – sets the priority fee from [`ExecutorConfig`].
    /// 2. `SetComputeUnitLimit` – caps compute consumption to prevent runaway cost.
    ///
    /// Returns the Jito bundle UUID on success.
    #[allow(dead_code)]
    pub async fn send_bundle(&self, instructions: Vec<Instruction>) -> Result<String> {
        // Read the cached blockhash (RwLock held only long enough to copy).
        let blockhash = *self.state.latest_blockhash.read().await;
        if blockhash == Hash::default() {
            return Err(anyhow::anyhow!(
                "[Executor] blockhash not yet cached – background refresher may not have run"
            ));
        }

        // Prepend compute budget instructions so the transaction competes for
        // block space under congestion.
        let priority_ix =
            ComputeBudgetInstruction::set_compute_unit_price(self.config.compute_unit_price);
        let limit_ix =
            ComputeBudgetInstruction::set_compute_unit_limit(self.config.compute_unit_limit);

        let mut all_instructions = Vec::with_capacity(instructions.len() + 2);
        all_instructions.push(priority_ix);
        all_instructions.push(limit_ix);
        all_instructions.extend(instructions);

        // Sign the transaction using the cached blockhash – no network call.
        let tx = Transaction::new_signed_with_payer(
            &all_instructions,
            Some(&self.wallet.pubkey()),
            &[self.wallet.as_ref()],
            blockhash,
        );

        // Serialize in the bincode format the Solana runtime expects.
        let data = bincode::serialize(&tx).context("Failed to serialize transaction")?;
        let data_len = data.len() as u64;

        let packet = Packet {
            data,
            meta: Some(Meta {
                size: data_len,
                addr: String::new(),
                port: 0,
                flags: None,
                sender_stake: 0,
            }),
        };

        let bundle = Bundle {
            packets: vec![packet],
        };

        // Clone the client – tonic clients share the underlying gRPC channel so
        // this is a cheap reference-count increment, not a new connection.
        let mut client = self.jito_client.clone();
        let response = client
            .send_bundle(SendBundleRequest {
                bundle: Some(bundle),
            })
            .await
            .context("Jito SendBundle RPC failed")?;

        let uuid = response.into_inner().uuid;
        info!("[Executor] bundle submitted | uuid={}", uuid);
        Ok(uuid)
    }

    /// Simulate a transaction via the RPC endpoint to verify it would succeed.
    ///
    /// Returns `true` if the simulation reports no on-chain error, `false`
    /// otherwise.
    ///
    /// Call this **before** submitting a bundle to Jito so that a failing arb
    /// does not burn tip lamports.  The simulation is an RPC call (~10–30 ms)
    /// but this is acceptable in the submission path (not the detection path).
    pub async fn simulate_transaction(&self, tx: &Transaction) -> Result<bool> {
        use solana_client::rpc_config::RpcSimulateTransactionConfig;

        let config = RpcSimulateTransactionConfig {
            sig_verify: false, // skip signature verification for speed
            replace_recent_blockhash: true, // avoid stale-blockhash sim failures
            ..Default::default()
        };

        let result = self
            .rpc
            .simulate_transaction_with_config(tx, config)
            .await
            .context("RPC simulate_transaction failed")?;

        if let Some(err) = &result.value.err {
            debug!("[Executor] simulation rejected: {:?}", err);
            if let Some(logs) = &result.value.logs {
                for log in logs
                    .iter()
                    .filter(|l| l.contains("Error") || l.contains("failed"))
                {
                    debug!("[Executor]   sim log: {}", log);
                }
            }
            return Ok(false);
        }

        debug!(
            "[Executor] simulation OK | units_consumed={:?}",
            result.value.units_consumed
        );
        Ok(true)
    }

    /// Retrieve Jito tip accounts.  At least one must receive a lamport tip per
    /// submitted bundle.
    #[allow(dead_code)]
    pub async fn get_tip_accounts(&self) -> Result<Vec<String>> {
        let mut client = self.jito_client.clone();
        let response = client
            .get_tip_accounts(GetTipAccountsRequest {})
            .await
            .context("Jito GetTipAccounts RPC failed")?;
        Ok(response.into_inner().accounts)
    }

    /// Fallback: sign and broadcast a transaction through the configured Solana
    /// RPC endpoint (bypassing Jito).  Use [`send_bundle`] for MEV submissions.
    ///
    /// Like [`send_bundle`], this method automatically prepends compute budget
    /// instructions for priority fee and compute unit limit.
    ///
    /// [`send_bundle`]: Executor::send_bundle
    #[allow(dead_code)]
    pub async fn send_transaction(&self, instructions: Vec<Instruction>) -> Result<Signature> {
        let priority_ix =
            ComputeBudgetInstruction::set_compute_unit_price(self.config.compute_unit_price);
        let limit_ix =
            ComputeBudgetInstruction::set_compute_unit_limit(self.config.compute_unit_limit);

        let mut all_instructions = Vec::with_capacity(instructions.len() + 2);
        all_instructions.push(priority_ix);
        all_instructions.push(limit_ix);
        all_instructions.extend(instructions);

        let recent_blockhash = self.rpc.get_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(
            &all_instructions,
            Some(&self.wallet.pubkey()),
            &[self.wallet.as_ref()],
            recent_blockhash,
        );
        let sig = self.rpc.send_and_confirm_transaction(&tx).await?;
        info!("[Executor] transaction confirmed: {}", sig);
        Ok(sig)
    }
}
