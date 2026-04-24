//! # Execution Logic
//!
//! This is the home of your **trading strategy**.
//!
//! The module is intentionally left as a well-structured skeleton.  The stream
//! in `main.rs` is already wired up to call [`Executor::on_account_update`] and
//! [`Executor::on_transaction`] for every Geyser event.  All you need to do is
//! fill in the `TODO` sections below with your own buy / sell rules.
//!
//! ## Step-by-step guide
//!
//! 1. **Configure your wallet** – see [`ExecutorConfig`] and the `TODO` inside
//!    [`Executor::new`].
//! 2. **Add account-based rules** – see [`Executor::on_account_update`].
//! 3. **Add transaction-based rules** – see [`Executor::on_transaction`].
//! 4. **Send a transaction** – see [`Executor::send_transaction`] for the ready-
//!    made helper; fill in the `TODO` inside it when you are ready.
//!
//! ## Solana SDK imports you will need
//!
//! ```rust,ignore
//! use solana_sdk::{
//!     instruction::Instruction,
//!     pubkey::Pubkey,
//!     signature::Keypair,
//!     signer::Signer,
//!     system_instruction,          // e.g. system_instruction::transfer(...)
//!     transaction::Transaction,
//! };
//! use solana_client::nonblocking::rpc_client::RpcClient;
//! ```

use {
    anyhow::Result,
    log::{debug, info, warn},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        pubkey::Pubkey,
        signature::Keypair,
        signer::Signer,
    },
    yellowstone_grpc_proto::prelude::{
        SubscribeUpdateAccountInfo, SubscribeUpdateTransactionInfo,
    },
};

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Strategy-level configuration.
///
/// Add any parameters your strategy needs here (price thresholds, target
/// addresses, position sizes, etc.) and pass them in when you call
/// [`Executor::new`].
#[derive(Debug)]
pub struct ExecutorConfig {
    /// Solana JSON-RPC endpoint used to broadcast signed transactions.
    ///
    /// **TODO:** Replace the default mainnet-beta URL with your private Alchemy
    /// HTTP RPC endpoint for lower latency and higher rate limits:
    ///
    /// ```text
    /// rpc_url: "https://solana-mainnet.g.alchemy.com/v2/YOUR_ALCHEMY_API_KEY".to_string(),
    /// ```
    pub rpc_url: String,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            rpc_url: "https://api.mainnet-beta.solana.com".to_string(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Executor
// ─────────────────────────────────────────────────────────────────────────────

/// The central strategy handler.
///
/// `main.rs` creates one instance of this struct and passes every incoming
/// Geyser event to its methods.  Your trading logic lives here.
pub struct Executor {
    /// Strategy configuration (RPC URL, thresholds, …).
    pub config: ExecutorConfig,
    /// Non-blocking JSON-RPC client – used by `send_transaction` to broadcast
    /// signed transactions.
    #[allow(dead_code)]
    rpc: RpcClient,
    /// The wallet that signs every outgoing transaction.
    #[allow(dead_code)]
    wallet: Keypair,
}

impl Executor {
    /// Construct a new `Executor`.
    ///
    /// # TODO – load your real wallet
    ///
    /// Replace the `Keypair::new()` call below with code that loads your actual
    /// keypair from a file or environment variable **before** going live:
    ///
    /// ```rust,ignore
    /// // Option A: load from a Solana CLI-style JSON key file
    /// use solana_sdk::signature::read_keypair_file;
    /// let wallet = read_keypair_file("/path/to/wallet.json")
    ///     .expect("Failed to read wallet keypair");
    ///
    /// // Option B: load from an environment variable (base-58 private key)
    /// let secret = std::env::var("WALLET_PRIVATE_KEY").expect("WALLET_PRIVATE_KEY not set");
    /// let wallet = Keypair::from_base58_string(&secret);
    /// ```
    pub fn new(config: ExecutorConfig) -> Self {
        let rpc = RpcClient::new(config.rpc_url.clone());

        // ── TODO (REQUIRED BEFORE GOING LIVE) ────────────────────────────────
        // Replace `Keypair::new()` with a real keypair loaded from disk or env.
        // Using a throwaway keypair for now so the project compiles out-of-the-box.
        let wallet = Keypair::new();
        // ─────────────────────────────────────────────────────────────────────

        info!("[Executor] wallet address: {}", wallet.pubkey());
        Self { config, rpc, wallet }
    }

    // ── Event hooks ───────────────────────────────────────────────────────────

    /// Called once for **every account update** the stream delivers.
    ///
    /// This is your primary entry point for data-driven buy/sell decisions
    /// based on on-chain account state (e.g. price feeds, pool reserves,
    /// vault balances).
    ///
    /// # How to add your logic
    ///
    /// 1. Decode `account.data` to extract the numbers that matter to your
    ///    strategy (price, liquidity, flag bits, …).
    /// 2. Apply your conditions.
    /// 3. Call `self.send_transaction(...)` when a trade should fire.
    ///
    /// ```rust,ignore
    /// // ── Example skeleton ──────────────────────────────────────────────────
    /// use solana_sdk::{pubkey, instruction::Instruction};
    ///
    /// const WATCHED: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
    ///
    /// if Pubkey::try_from(account.pubkey.as_slice())? == WATCHED {
    ///     // TODO: parse account.data here
    ///     let price = decode_price(&account.data);
    ///
    ///     if price < BUY_THRESHOLD {
    ///         let ix = build_buy_instruction(&self.wallet.pubkey(), AMOUNT_LAMPORTS);
    ///         self.send_transaction(vec![ix]).await?;
    ///     }
    /// }
    /// ```
    pub async fn on_account_update(
        &self,
        account: &SubscribeUpdateAccountInfo,
        slot: u64,
    ) -> Result<()> {
        let pubkey = Pubkey::try_from(account.pubkey.as_slice()).unwrap_or_default();

        debug!(
            "[Executor] account update | pubkey={} lamports={} data_len={} slot={}",
            pubkey,
            account.lamports,
            account.data.len(),
            slot
        );

        // ══════════════════════════════════════════════════════════════════════
        // TODO: ADD YOUR ACCOUNT-BASED TRADING LOGIC HERE
        //
        //  ✦  Check `pubkey` against a watched address list.
        //  ✦  Decode `account.data` using your program's state layout.
        //  ✦  Compare values against your entry/exit thresholds.
        //  ✦  Call `self.send_transaction(vec![your_instruction])` to trade.
        // ══════════════════════════════════════════════════════════════════════

        Ok(())
    }

    /// Called once for **every transaction update** the stream delivers.
    ///
    /// Use this hook to react to on-chain activity — DEX swaps, token mints,
    /// program invocations — rather than raw account state.
    ///
    /// # How to add your logic
    ///
    /// 1. Inspect `tx.transaction` for the message (accounts, instructions).
    /// 2. Inspect `tx.meta` for pre/post token balances and log messages.
    /// 3. Call `self.send_transaction(...)` when your conditions are met.
    ///
    /// ```rust,ignore
    /// // ── Example skeleton ──────────────────────────────────────────────────
    /// if let Some(meta) = &tx.meta {
    ///     for log in &meta.log_messages {
    ///         if log.contains("Swap") {
    ///             // TODO: parse log, compute arb opportunity
    ///             let ix = build_arb_instruction(&self.wallet.pubkey());
    ///             self.send_transaction(vec![ix]).await?;
    ///             break;
    ///         }
    ///     }
    /// }
    /// ```
    pub async fn on_transaction(
        &self,
        tx: &SubscribeUpdateTransactionInfo,
        slot: u64,
    ) -> Result<()> {
        debug!(
            "[Executor] transaction | sig={} slot={}",
            bs58_encode(&tx.signature),
            slot
        );

        // ══════════════════════════════════════════════════════════════════════
        // TODO: ADD YOUR TRANSACTION-BASED TRADING LOGIC HERE
        //
        //  ✦  Inspect `tx.transaction` (message accounts + instructions).
        //  ✦  Inspect `tx.meta` (log_messages, pre/post token balances, err).
        //  ✦  Call `self.send_transaction(vec![your_instruction])` to trade.
        // ══════════════════════════════════════════════════════════════════════

        Ok(())
    }

    // ── Transaction helper ────────────────────────────────────────────────────

    /// Sign and send a transaction, waiting for on-chain confirmation.
    ///
    /// Pass in the list of [`solana_sdk::instruction::Instruction`]s you want
    /// to execute.  The helper fetches the latest blockhash, builds a
    /// [`solana_sdk::transaction::Transaction`], signs it with `self.wallet`,
    /// and submits it to the RPC node.
    ///
    /// # TODO – implement when ready to trade
    ///
    /// The method body currently logs a warning and returns `Ok(())` so the
    /// project compiles and runs without a real wallet.  When you are ready to
    /// send real transactions, replace the body with the code in the doc
    /// example below:
    ///
    /// ```rust,ignore
    /// use solana_sdk::{signer::Signer, transaction::Transaction};
    ///
    /// pub async fn send_transaction(
    ///     &self,
    ///     instructions: Vec<solana_sdk::instruction::Instruction>,
    /// ) -> Result<()> {
    ///     let recent_blockhash = self.rpc.get_latest_blockhash().await?;
    ///     let tx = Transaction::new_signed_with_payer(
    ///         &instructions,
    ///         Some(&self.wallet.pubkey()),
    ///         &[&self.wallet],
    ///         recent_blockhash,
    ///     );
    ///     let sig = self.rpc.send_and_confirm_transaction(&tx).await?;
    ///     info!("[Executor] transaction confirmed: {}", sig);
    ///     Ok(())
    /// }
    /// ```
    ///
    /// > **Note on SDK versions:** `solana-client` and `solana-sdk` must use
    /// > compatible versions of `solana-transaction` internally.  If you see a
    /// > `SerializableTransaction` trait error, pin both crates to the same
    /// > Solana release series (e.g. both to `2.x` or both to `4.x`).
    #[allow(dead_code)]
    pub async fn send_transaction(
        &self,
        instructions: Vec<solana_sdk::instruction::Instruction>,
    ) -> Result<()> {
        // ══════════════════════════════════════════════════════════════════════
        // TODO: REPLACE THIS STUB WITH THE REAL IMPLEMENTATION (see doc above)
        //
        // self.rpc and self.wallet are already available.
        // The code example in the rustdoc above shows exactly what to write.
        // ══════════════════════════════════════════════════════════════════════
        warn!(
            "[Executor] send_transaction called with {} instruction(s) — \
             stub not yet implemented. Add your real signing + RPC logic here.",
            instructions.len()
        );
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Encode a raw byte slice as a base-58 string (Solana's standard encoding).
fn bs58_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    if bytes.is_empty() {
        return String::new();
    }
    let leading_zeros = bytes.iter().take_while(|&&b| b == 0).count();
    let mut digits: Vec<u8> = vec![0];
    for &byte in bytes {
        let mut carry = byte as u32;
        for d in digits.iter_mut() {
            carry += (*d as u32) << 8;
            *d = (carry % 58) as u8;
            carry /= 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }
    let mut result = String::with_capacity(leading_zeros + digits.len());
    for _ in 0..leading_zeros {
        result.push(ALPHABET[0] as char);
    }
    for d in digits.iter().rev() {
        result.push(ALPHABET[*d as usize] as char);
    }
    result
}

