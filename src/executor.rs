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

use {
    anyhow::{Context, Result},
    log::{debug, info, warn},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
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
    /// Solana JSON-RPC endpoint used for blockhash refresh and fallback
    /// transaction broadcast.
    ///
    /// **Recommendation:** Use your private Alchemy endpoint for lower latency:
    /// `https://solana-mainnet.g.alchemy.com/v2/YOUR_API_KEY`
    pub rpc_url: String,

    /// Jito Block Engine gRPC endpoint.
    pub jito_url: String,

    /// Minimum SOL balance delta (lamports) to trigger a backrun attempt.
    pub min_lamports: u64,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            rpc_url: std::env::var("RPC_URL")
                .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string()),
            jito_url: std::env::var("JITO_BLOCK_ENGINE_URL")
                .unwrap_or_else(|_| "https://mainnet.block-engine.jito.wtf:443".to_string()),
            min_lamports: DEFAULT_MIN_LAMPORTS_DELTA,
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
    #[allow(dead_code)]
    config: ExecutorConfig,
    /// Non-blocking RPC client – used only by the background blockhash refresher
    /// and the fallback [`send_transaction`].
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
    /// # TODO – load your real wallet before going live
    ///
    /// ```rust,ignore
    /// // Option A – JSON key file
    /// use solana_sdk::signature::read_keypair_file;
    /// let wallet = read_keypair_file("/path/to/wallet.json")?;
    ///
    /// // Option B – base-58 env var
    /// let wallet = Keypair::from_base58_string(&std::env::var("WALLET_PRIVATE_KEY")?);
    /// ```
    pub async fn new(config: ExecutorConfig) -> Result<Self> {
        let rpc = Arc::new(RpcClient::new(config.rpc_url.clone()));

        // Ephemeral keypair – replace with a real funded wallet before going live.
        let wallet = Arc::new(Keypair::new());

        // Lazy Jito gRPC channel: connects on the first RPC call, not at startup.
        let channel = Channel::from_shared(config.jito_url.clone())
            .context("Invalid JITO_BLOCK_ENGINE_URL")?
            .connect_lazy();
        let jito_client = SearcherServiceClient::new(channel);

        info!(
            "[Executor] initialised | rpc={} | jito={} | wallet={}",
            config.rpc_url,
            config.jito_url,
            wallet.pubkey()
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

        debug!(
            "[Executor] account update | pubkey={} lamports={} data_len={} slot={}",
            pubkey,
            account.lamports,
            account.data.len(),
            slot
        );

        // ══════════════════════════════════════════════════════════════════════
        // TODO: Add account-based strategy logic here.
        //
        //  ✦  Match `pubkey` against watched pool / vault addresses.
        //  ✦  Decode `account.data` using your program's state layout.
        //  ✦  Derive an arb opportunity and call `self.send_bundle(...)`.
        // ══════════════════════════════════════════════════════════════════════

        Ok(())
    }

    /// Called once for every transaction update – the hot-path entry point.
    ///
    /// Applies fast-path guards in order of cheapest first:
    /// 1. Skip failed transactions.
    /// 2. Skip transactions that do not touch a known DEX program.
    /// 3. Skip transactions whose maximum SOL balance delta is below the
    ///    configured threshold.
    ///
    /// No network I/O is performed inside this method.
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
                    pk == JUPITER_V6_PROGRAM_ID || pk == RAYDIUM_LIQUIDITY_POOL_V4
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

        if largest_delta < self.min_lamports {
            return Ok(());
        }

        debug!(
            "[Executor] arb candidate | slot={} delta={}",
            slot, largest_delta
        );

        // ══════════════════════════════════════════════════════════════════════
        // TODO: Build your flash-loan arbitrage instructions and submit via
        // `send_bundle`.
        //
        // Example skeleton:
        //   let borrow_ix = build_borrow_ix(&self.wallet.pubkey(), largest_delta);
        //   let swap_ix   = build_swap_ix(...);
        //   let repay_ix  = build_repay_ix(...);
        //   let tip_ix    = build_tip_ix(&tip_account, TIP_LAMPORTS);
        //   self.send_bundle(vec![borrow_ix, swap_ix, repay_ix, tip_ix]).await?;
        // ══════════════════════════════════════════════════════════════════════

        Ok(())
    }

    // ── Bundle submission ─────────────────────────────────────────────────────

    /// Wrap `instructions` in a signed [`Transaction`], pack it into a Jito
    /// [`Bundle`], and submit it to the Block Engine.
    ///
    /// Uses the **cached** blockhash – zero network I/O in the submission path.
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

        // Sign the transaction using the cached blockhash – no network call.
        let tx = Transaction::new_signed_with_payer(
            &instructions,
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
    /// [`send_bundle`]: Executor::send_bundle
    #[allow(dead_code)]
    pub async fn send_transaction(&self, instructions: Vec<Instruction>) -> Result<Signature> {
        let recent_blockhash = self.rpc.get_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.wallet.pubkey()),
            &[self.wallet.as_ref()],
            recent_blockhash,
        );
        let sig = self.rpc.send_and_confirm_transaction(&tx).await?;
        info!("[Executor] transaction confirmed: {}", sig);
        Ok(sig)
    }
}
