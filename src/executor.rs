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
//! 4. **Send a transaction** – [`Executor::send_transaction`] is fully
//!    implemented; just call it with your instructions.
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
    borsh::BorshDeserialize,
    log::{debug, info, warn},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        compute_budget::ComputeBudgetInstruction,
        instruction::Instruction,
        pubkey::Pubkey,
        signature::{Keypair, Signature},
        signer::Signer,
        transaction::Transaction,
    },
    yellowstone_grpc_proto::prelude::{SubscribeUpdateAccountInfo, SubscribeUpdateTransactionInfo},
};

const PRIORITY_FEE_MICROLAMPORTS: u64 = 100_000;
const WATCHED_PROGRAM: Pubkey = solana_sdk::pubkey!("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8");

#[derive(BorshDeserialize, Debug)]
struct WatchedAccountState {
    price: u64,
    liquidity: u64,
}

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
    /// **TODO:** Replace the default public endpoint with your private Alchemy
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
    #[allow(dead_code)]
    pub config: ExecutorConfig,
    /// Non-blocking JSON-RPC client – used by [`send_transaction`] to broadcast
    /// signed transactions.
    ///
    /// [`send_transaction`]: Executor::send_transaction
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
    /// // Option B: load from a base-58 private key in an env variable
    /// let secret = std::env::var("WALLET_PRIVATE_KEY")
    ///     .expect("WALLET_PRIVATE_KEY not set");
    /// let wallet = Keypair::from_base58_string(&secret);
    /// ```
    pub fn new(config: ExecutorConfig) -> Self {
        let rpc = RpcClient::new(config.rpc_url.clone());
        let wallet_secret = std::env::var("WALLET_PRIVATE_KEY")
            .expect("WALLET_PRIVATE_KEY environment variable not set");
        let wallet = Keypair::from_base58_string(&wallet_secret);

        info!(
            "[Executor] initialised | rpc={} | wallet={}",
            config.rpc_url,
            wallet.pubkey()
        );
        Self {
            config,
            rpc,
            wallet,
        }
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
    /// 2. Apply your entry/exit conditions.
    /// 3. Call `self.send_transaction(vec![your_instruction]).await?` to trade.
    ///
    /// ```rust,ignore
    /// // ── Example skeleton ──────────────────────────────────────────────────
    /// use solana_sdk::{pubkey, system_instruction};
    ///
    /// const WATCHED: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
    ///
    /// if Pubkey::try_from(account.pubkey.as_slice())? == WATCHED {
    ///     let price = decode_price(&account.data); // your decode function
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

        if pubkey != WATCHED_PROGRAM {
            return Ok(());
        }

        match WatchedAccountState::try_from_slice(account.data.as_slice()) {
            Ok(state) => {
                debug!(
                    "[Executor] decoded watched account | pubkey={} price={} liquidity={} slot={}",
                    pubkey, state.price, state.liquidity, slot
                );
            }
            Err(err) => {
                debug!(
                    "[Executor] watched account decode skeleton could not deserialize {} bytes for {}: {}",
                    account.data.len(),
                    pubkey,
                    err
                );
            }
        }

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
    /// 3. Call `self.send_transaction(vec![your_instruction]).await?` to trade.
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
        let signature = bs58::encode(&tx.signature).into_string();
        debug!("[Executor] transaction | sig={} slot={}", signature, slot);

        let Some(meta) = tx.meta.as_ref() else {
            debug!("[Executor] transaction has no metadata | sig={}", signature);
            return Ok(());
        };

        if let Some(log_message) = meta
            .log_messages
            .iter()
            .find(|log| log.contains("Swap") || log.contains("initialize"))
        {
            info!(
                "[Executor] trading opportunity candidate | sig={} slot={} log={}",
                signature, slot, log_message
            );
        }

        Ok(())
    }

    // ── Transaction helper ────────────────────────────────────────────────────

    /// Sign and send a transaction, waiting for on-chain confirmation.
    ///
    /// Pass in the list of [`Instruction`]s you want to execute.  The helper
    /// fetches the latest blockhash, builds a signed [`Transaction`], and
    /// submits it via the configured RPC endpoint.
    ///
    /// Returns the confirmed [`Signature`] on success.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use solana_sdk::system_instruction;
    ///
    /// // Send 0.001 SOL to a recipient
    /// let ix = system_instruction::transfer(
    ///     &self.wallet.pubkey(),
    ///     &recipient_pubkey,
    ///     1_000_000, // lamports
    /// );
    /// let sig = self.send_transaction(vec![ix]).await?;
    /// println!("confirmed: {}", sig);
    /// ```
    #[allow(dead_code)]
    pub async fn send_transaction(&self, instructions: Vec<Instruction>) -> Result<Signature> {
        let recent_blockhash = self.rpc.get_latest_blockhash().await?;
        let mut instructions_with_priority_fee = Vec::with_capacity(instructions.len() + 1);
        instructions_with_priority_fee.push(ComputeBudgetInstruction::set_compute_unit_price(
            PRIORITY_FEE_MICROLAMPORTS,
        ));
        instructions_with_priority_fee.extend(instructions);
        let tx = Transaction::new_signed_with_payer(
            &instructions_with_priority_fee,
            Some(&self.wallet.pubkey()),
            &[&self.wallet],
            recent_blockhash,
        );
        let sig = self.rpc.send_and_confirm_transaction(&tx).await?;
        info!("[Executor] transaction confirmed: {}", sig);
        Ok(sig)
    }
}
