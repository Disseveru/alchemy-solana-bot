//! # Execution Logic
//!
//! This module is the place to add your **buy / sell rules** once the data
//! stream is stable.
//!
//! ## How to extend this module
//!
//! 1. Fill in [`ExecutorConfig`] with whatever parameters your strategy needs
//!    (e.g. target pubkey, price threshold, wallet keypair path).
//! 2. Implement your logic inside [`Executor::on_account_update`] and/or
//!    [`Executor::on_transaction`].
//! 3. Use [`Executor::send_transaction`] (or build your own helper) to sign
//!    and submit a transaction via the Solana RPC client.
//!
//! ## Solana SDK quick-reference
//!
//! ```rust,ignore
//! use solana_sdk::{
//!     instruction::Instruction,
//!     pubkey::Pubkey,
//!     signature::Keypair,
//!     signer::Signer,
//!     system_instruction,
//!     transaction::Transaction,
//! };
//! use solana_client::nonblocking::rpc_client::RpcClient;
//! ```

use {
    anyhow::Result,
    log::{debug, info},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        pubkey::Pubkey,
        signature::Keypair,
        signer::Signer,
        transaction::Transaction,
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
/// Extend this struct with whatever parameters your buy/sell rules require.
#[derive(Debug)]
pub struct ExecutorConfig {
    /// Solana JSON-RPC endpoint used to broadcast transactions.
    ///
    /// Defaults to the public mainnet-beta endpoint; replace with a private
    /// RPC URL (e.g. your Alchemy HTTP endpoint) for production use.
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

/// Stateful handler that receives live Geyser updates and decides whether to
/// act on them.
///
/// # Example – wiring in your own logic
///
/// ```rust,ignore
/// // Inside on_account_update, after detecting your target account:
/// if account.pubkey == MY_TARGET.to_bytes().to_vec() && account.lamports < THRESHOLD {
///     self.send_transaction(build_buy_instruction(&self.wallet)).await?;
/// }
/// ```
pub struct Executor {
    config: ExecutorConfig,
    /// Non-blocking RPC client for submitting transactions.
    rpc: RpcClient,
    /// The wallet keypair that signs outgoing transactions.
    ///
    /// Replace this with a real keypair loaded from disk or env in production.
    wallet: Keypair,
}

impl Executor {
    /// Create a new `Executor` from the given configuration.
    pub fn new(config: ExecutorConfig) -> Self {
        let rpc = RpcClient::new(config.rpc_url.clone());
        // TODO: Load your wallet keypair from a file or environment variable.
        //
        //   let wallet = Keypair::read_from_file("/path/to/wallet.json")
        //       .expect("Failed to load wallet keypair");
        //
        // For now a throwaway keypair is used so the project compiles without
        // any additional setup.
        let wallet = Keypair::new();
        info!("Executor wallet address: {}", wallet.pubkey());
        Self { config, rpc, wallet }
    }

    // ── Event hooks ──────────────────────────────────────────────────────────

    /// Called for every **account update** received from the stream.
    ///
    /// # What to add here
    /// * Parse account data to detect price changes, liquidity shifts, etc.
    /// * Trigger a buy or sell when your conditions are met.
    pub async fn on_account_update(&self, account: &SubscribeUpdateAccountInfo, slot: u64) -> Result<()> {
        let pubkey = Pubkey::try_from(account.pubkey.as_slice())
            .unwrap_or_default();

        debug!(
            "[Executor] account update: pubkey={} lamports={} slot={}",
            pubkey, account.lamports, slot
        );

        // ── TODO: Add your account-based strategy logic here ─────────────────
        //
        // Example skeleton:
        //
        //   if pubkey == MY_WATCHED_ACCOUNT && account.lamports > BUY_THRESHOLD {
        //       let ix = build_buy_instruction(&self.wallet.pubkey(), amount);
        //       self.send_transaction(vec![ix]).await?;
        //   }

        Ok(())
    }

    /// Called for every **transaction update** received from the stream.
    ///
    /// # What to add here
    /// * Inspect instruction data, log messages, or pre/post token balances.
    /// * React to specific program interactions (e.g. a DEX swap).
    pub async fn on_transaction(&self, tx: &SubscribeUpdateTransactionInfo, slot: u64) -> Result<()> {
        let sig = bs58_encode(&tx.signature);

        debug!(
            "[Executor] transaction: sig={} slot={}",
            sig, slot
        );

        // ── TODO: Add your transaction-based strategy logic here ──────────────
        //
        // Example skeleton:
        //
        //   if let Some(meta) = &tx.meta {
        //       for log in &meta.log_messages {
        //           if log.contains("Swap") {
        //               self.send_transaction(build_arb_tx()).await?;
        //               break;
        //           }
        //       }
        //   }

        Ok(())
    }

    // ── Transaction helpers ───────────────────────────────────────────────────

    /// Sign and send a transaction to the network, waiting for confirmation.
    ///
    /// # Usage
    ///
    /// ```rust,ignore
    /// use solana_sdk::{system_instruction, transaction::Transaction};
    ///
    /// let ix = system_instruction::transfer(&self.wallet.pubkey(), &recipient, lamports);
    /// self.send_transaction(vec![ix]).await?;
    /// ```
    pub async fn send_transaction(
        &self,
        instructions: Vec<solana_sdk::instruction::Instruction>,
    ) -> Result<solana_sdk::signature::Signature> {
        use solana_sdk::signer::Signer;

        let recent_blockhash = self.rpc.get_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.wallet.pubkey()),
            &[&self.wallet],
            recent_blockhash,
        );
        let sig = self.rpc.send_and_confirm_transaction(&tx).await?;
        info!("[Executor] transaction confirmed: {}", sig);
        Ok(sig)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Encode a raw byte slice as a base-58 string (mirrors the helper in main.rs).
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
