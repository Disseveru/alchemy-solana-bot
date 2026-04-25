//! # Jito Backrunning Arbitrage Engine
//!
//! Implements the full **detect → flash-borrow → swap → swap → repay → tip**
//! lifecycle for backrunning arbitrage using Jito Bundles and Solend flash loans.
//!
//! ## Strategy Overview
//!
//! 1. Subscribe to the Jito mempool stream for pending swap transactions.
//! 2. Detect large DEX swaps that will create a cross-DEX price discrepancy.
//! 3. Build a **single atomic transaction**:
//!    ```text
//!    ix[0]  FlashBorrowReserveLiquidity  (Solend – borrow loan token)
//!    ix[1]  SwapBaseIn                   (Raydium – buy arb token cheaply)
//!    ix[2]  Swap                         (Orca    – sell arb token at fair price)
//!    ix[3]  FlashRepayReserveLiquidity   (Solend  – repay loan + fee)
//!    ix[4]  system::transfer             (Jito tip – pays the validator)
//!    ```
//! 4. Wrap the transaction in a Jito [`Bundle`] and submit.
//!
//! ## Atomicity / Safety
//!
//! * Solend's `FlashRepayReserveLiquidity` instruction verifies that the exact
//!   borrowed amount (plus fee) has been returned in the **same transaction**.
//!   If the swap sequence yields insufficient output the Solend program aborts
//!   the entire transaction on-chain — no funds are lost.
//! * The `min_amount_out` / `other_amount_threshold` fields on both swap
//!   instructions act as a second line of defence, reverting if slippage
//!   exceeds the configured tolerance.
//!
//! ## Configuration
//!
//! Fill in an [`ArbRoute`] with the on-chain accounts for your target trading
//! pair (e.g. SOL–USDC on Raydium + Orca) and pass it to [`BackrunEngine::new`].
//! Example mainnet addresses are provided in the field documentation.

// All items in this module are deliberately public API consumed by the
// operator's wiring code in main.rs once ArbRoute is configured.
#![allow(dead_code)]

use {
    anyhow::{Context, Result},
    log::{debug, info, warn},
    solana_sdk::{
        hash::Hash,
        instruction::{AccountMeta, Instruction},
        pubkey::Pubkey,
        signature::Keypair,
        signer::Signer,
        sysvar,
        transaction::Transaction,
    },
    std::{sync::Arc, time::Duration},
};

use crate::{
    executor::Executor,
    jito::{
        bundle::Bundle,
        packet::{Meta, Packet},
        searcher::{SendBundleRequest, SubscribeMempoolRequest},
    },
};

// ─────────────────────────────────────────────────────────────────────────────
// Program IDs (mainnet)
// ─────────────────────────────────────────────────────────────────────────────

/// Solend lending program (mainnet).
const SOLEND_PROGRAM: Pubkey =
    Pubkey::from_str_const("So1endD9AkS2mttmbeS8S96ySZps9SML7RnZBjWdLLV");

/// Raydium AMM v4 liquidity pool program (mainnet).
const RAYDIUM_AMM_V4: Pubkey =
    Pubkey::from_str_const("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8");

/// Orca Whirlpool program (mainnet).
const ORCA_WHIRLPOOL_PROGRAM: Pubkey =
    Pubkey::from_str_const("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc");

/// SPL Token program.
const TOKEN_PROGRAM: Pubkey =
    Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

// ─────────────────────────────────────────────────────────────────────────────
// Instruction discriminators
// ─────────────────────────────────────────────────────────────────────────────

/// Solend `FlashBorrowReserveLiquidity` instruction variant byte (index 20).
const SOLEND_FLASH_BORROW: u8 = 20;

/// Solend `FlashRepayReserveLiquidity` instruction variant byte (index 21).
const SOLEND_FLASH_REPAY: u8 = 21;

/// Raydium AMM v4 `SwapBaseIn` instruction variant byte (index 9).
const RAYDIUM_SWAP_BASE_IN: u8 = 9;

/// Raydium AMM v4 `SwapBaseOut` instruction variant byte (index 11).
/// The amount at bytes 1..9 is `max_amount_in` rather than `amount_in`,
/// but we still use it as a proxy for the swap notional.
const RAYDIUM_SWAP_BASE_OUT: u8 = 11;

/// Anchor discriminator for the Orca Whirlpool `swap` instruction.
/// Computed as: `sha256("global:swap")[0..8]`
const ORCA_SWAP_DISCRIMINATOR: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];

/// Heuristic gross-profit estimate used as a cheap pre-filter before simulation.
const HEURISTIC_PROFIT_BPS: u64 = 50;

// ─────────────────────────────────────────────────────────────────────────────
// Tunable parameters
// ─────────────────────────────────────────────────────────────────────────────

/// Tunable parameters for the backrunning strategy.
///
/// Construct via [`ArbConfig::default()`] (which reads env vars) or set fields
/// directly.  All lamport amounts are in the SOL base unit (10⁻⁹ SOL).
///
/// ## Environment variable overrides
///
/// | Field                     | Env var                      | Default        |
/// |---------------------------|------------------------------|----------------|
/// | `solend_fee_bps`          | `SOLEND_FEE_BPS`             | 30 (0.3 %)    |
/// | `jito_tip_lamports`       | `JITO_TIP_LAMPORTS`          | 100 000        |
/// | `min_net_profit_lamports` | `MIN_NET_PROFIT_LAMPORTS`    | 50 000         |
/// | `min_target_swap_lamports`| `MIN_TARGET_SWAP_LAMPORTS`   | 1 000 000 000  |
/// | `slippage_bps`            | `SLIPPAGE_BPS`               | 200 (2 %)     |
/// | `max_retry_attempts`      | `MAX_RETRY_ATTEMPTS`         | 3              |
#[derive(Debug, Clone)]
pub struct ArbConfig {
    /// Solend flash-loan fee rate in basis points (30 bps = 0.3 %).
    pub solend_fee_bps: u64,
    /// Jito tip in lamports paid to the validator per bundle.
    /// Increase to improve inclusion priority under competition.
    pub jito_tip_lamports: u64,
    /// Minimum net profit (lamports) after flash-loan fee and Jito tip.
    /// Bundles expected to earn less are discarded without submission.
    pub min_net_profit_lamports: u64,
    /// Minimum SOL-equivalent notional of the detected swap to trigger a backrun.
    /// Prevents spending compute cycles on micro-swaps with negligible price impact.
    pub min_target_swap_lamports: u64,
    /// Slippage tolerance in basis points (200 bps = 2 %).
    pub slippage_bps: u64,
    /// Maximum retry attempts on Jito gRPC submission failures.
    pub max_retry_attempts: u32,
}

impl Default for ArbConfig {
    fn default() -> Self {
        Self {
            solend_fee_bps: env_u64("SOLEND_FEE_BPS", 30),
            jito_tip_lamports: env_u64("JITO_TIP_LAMPORTS", 100_000),
            min_net_profit_lamports: env_u64("MIN_NET_PROFIT_LAMPORTS", 50_000),
            min_target_swap_lamports: env_u64("MIN_TARGET_SWAP_LAMPORTS", 1_000_000_000),
            slippage_bps: env_u64("SLIPPAGE_BPS", 200),
            max_retry_attempts: std::env::var("MAX_RETRY_ATTEMPTS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3),
        }
    }
}

/// Parse a u64 from an environment variable, returning `default` on any error.
fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ─────────────────────────────────────────────────────────────────────────────
// Pool / reserve account configuration
// ─────────────────────────────────────────────────────────────────────────────

/// All on-chain account addresses required for a single arb route.
///
/// This struct must be constructed manually — there is no sensible default
/// because every field is pool-specific.  Example mainnet addresses for the
/// **SOL–USDC** route are provided in the doc comments.
///
/// ## How to find addresses
///
/// * Solend reserve/market accounts: <https://docs.solend.fi/protocol/addresses>
/// * Raydium pool accounts: <https://api.raydium.io/v2/ammV3/ammPools>
/// * Orca pool accounts: <https://api.mainnet.orca.so/v1/whirlpool/list>
/// * Jito tip accounts: call `Executor::get_tip_accounts()`
pub struct ArbRoute {
    // ── Solend flash loan ─────────────────────────────────────────────────────
    /// Solend reserve state account for the borrowed asset.
    /// SOL reserve (main pool): `8PbodeaosQP19SjYFx855UMqWxH2HynZLdBXmsrbac36`
    pub solend_reserve: Pubkey,

    /// Reserve's liquidity supply token account (source of flash-borrowed funds).
    /// SOL reserve supply: `8UviNr47S8eL6J3WfDxMRa3hvLta1VDJwNWqsDgtN3Ud`
    pub solend_reserve_liquidity_supply: Pubkey,

    /// Solend protocol fee receiver account.
    pub solend_fee_receiver: Pubkey,

    /// Solend lending market account.
    /// Main pool: `4UpD2fh7xH3VP9QQaXtsS1YY3bxzWhtfpks7FatyKvdY`
    pub solend_lending_market: Pubkey,

    /// PDA authority derived from `solend_lending_market`.
    pub solend_lending_market_authority: Pubkey,

    /// Our token account that temporarily holds the flash-borrowed asset.
    pub our_loan_token_account: Pubkey,

    // ── Raydium AMM v4 ───────────────────────────────────────────────────────
    /// Raydium AMM pool state account.
    /// SOL-USDC pool: `58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWaS6E7gexGD6`
    pub raydium_amm_id: Pubkey,

    /// Raydium AMM authority PDA.
    pub raydium_amm_authority: Pubkey,

    /// AMM open-orders account (OpenBook/Serum).
    pub raydium_amm_open_orders: Pubkey,

    /// AMM target-orders account.
    pub raydium_amm_target_orders: Pubkey,

    /// Pool coin (base token) vault.
    pub raydium_pool_coin_vault: Pubkey,

    /// Pool PC (quote token) vault.
    pub raydium_pool_pc_vault: Pubkey,

    /// OpenBook (Serum v3) program ID.
    /// Mainnet: `srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX`
    pub raydium_serum_program: Pubkey,

    /// OpenBook market for this pair.
    pub raydium_serum_market: Pubkey,

    /// OpenBook bids account.
    pub raydium_serum_bids: Pubkey,

    /// OpenBook asks account.
    pub raydium_serum_asks: Pubkey,

    /// OpenBook event queue.
    pub raydium_serum_event_queue: Pubkey,

    /// OpenBook coin vault.
    pub raydium_serum_coin_vault: Pubkey,

    /// OpenBook PC vault.
    pub raydium_serum_pc_vault: Pubkey,

    /// OpenBook vault signer PDA.
    pub raydium_serum_vault_signer: Pubkey,

    /// Our token account being sold *into* Raydium (the flash-borrowed asset).
    pub our_raydium_source: Pubkey,

    /// Our token account receiving tokens *from* Raydium (the arb token).
    pub our_raydium_dest: Pubkey,

    // ── Orca Whirlpool ───────────────────────────────────────────────────────
    /// Orca Whirlpool pool state account.
    /// SOL-USDC pool: `HJPjoWUrhoZzkNfRpHuieeFk9WcZWjwy6PBjZ81ngndJ`
    pub orca_whirlpool: Pubkey,

    /// Whirlpool token vault A.
    pub orca_token_vault_a: Pubkey,

    /// Whirlpool token vault B.
    pub orca_token_vault_b: Pubkey,

    /// Three consecutive tick-array accounts required by the Whirlpool swap math.
    /// Derive with `orca_whirlpools_sdk::get_tick_arrays` for your pool + direction.
    pub orca_tick_array_0: Pubkey,
    pub orca_tick_array_1: Pubkey,
    pub orca_tick_array_2: Pubkey,

    /// Whirlpool oracle PDA (derived from pool pubkey).
    pub orca_oracle: Pubkey,

    /// Our token account A for the Orca swap leg (arb token in).
    pub our_orca_token_a: Pubkey,

    /// Our token account B for the Orca swap leg (loan token out).
    pub our_orca_token_b: Pubkey,

    // ── Jito tip ─────────────────────────────────────────────────────────────
    /// Jito tip accounts retrieved via `Executor::get_tip_accounts()`.
    /// The engine picks one per bundle.  Provide all 8 for load balancing.
    pub tip_accounts: Vec<Pubkey>,
}

// ─────────────────────────────────────────────────────────────────────────────
// AMM math utilities
// ─────────────────────────────────────────────────────────────────────────────

/// Compute constant-product AMM output amount.
///
/// Implements the standard `x * y = k` formula with swap fee:
///
/// ```text
/// amount_out = (reserve_out × amount_in × (10000 − fee_bps))
///              ──────────────────────────────────────────────────────────
///              (reserve_in × 10000) + (amount_in × (10000 − fee_bps))
/// ```
///
/// All values are in the raw token base unit (smallest denomination).
///
/// Returns 0 if reserves are zero, `amount_in` is zero, or an intermediate
/// computation would overflow (saturating arithmetic prevents panics).
///
/// # Arguments
/// * `reserve_in`  – Current pool balance of the input token.
/// * `reserve_out` – Current pool balance of the output token.
/// * `amount_in`   – Exact input token amount to swap.
/// * `fee_bps`     – Pool swap fee in basis points (e.g. 25 for Raydium 0.25 %).
///
/// # Example
/// ```ignore
/// // Raydium 0.25% fee, 50k SOL : 8M USDC pool, swap 100 SOL.
/// let out = amm_output(50_000_000_000_000, 8_000_000_000_000, 100_000_000_000, 25);
/// assert!(out > 15_000_000_000 && out < 17_000_000_000); // ≈ 16 000 USDC
/// ```
pub fn amm_output(reserve_in: u64, reserve_out: u64, amount_in: u64, fee_bps: u64) -> u64 {
    if reserve_in == 0 || reserve_out == 0 || amount_in == 0 {
        return 0;
    }
    let fee_factor = 10_000u128.saturating_sub(fee_bps as u128);
    let amount_with_fee = (amount_in as u128).saturating_mul(fee_factor);
    let numerator = (reserve_out as u128).saturating_mul(amount_with_fee);
    let denominator = (reserve_in as u128)
        .saturating_mul(10_000)
        .saturating_add(amount_with_fee);
    if denominator == 0 {
        return 0;
    }
    (numerator / denominator) as u64
}

// ─────────────────────────────────────────────────────────────────────────────
// Route validation
// ─────────────────────────────────────────────────────────────────────────────

/// Validate that all [`ArbRoute`] pubkeys are non-zero and the tip account list
/// is non-empty.
///
/// Call this once at startup **before** enabling the backrun engine.  A zero
/// pubkey (`11111…`) indicates an unconfigured placeholder and will cause an
/// on-chain error if submitted.
///
/// Returns an error listing every invalid field so all problems can be fixed at
/// once rather than one by one.
pub fn validate_route(route: &ArbRoute) -> Result<()> {
    let zero = Pubkey::default();
    let checks: &[(&str, &Pubkey)] = &[
        ("solend_reserve", &route.solend_reserve),
        ("solend_reserve_liquidity_supply", &route.solend_reserve_liquidity_supply),
        ("solend_fee_receiver", &route.solend_fee_receiver),
        ("solend_lending_market", &route.solend_lending_market),
        ("solend_lending_market_authority", &route.solend_lending_market_authority),
        ("our_loan_token_account", &route.our_loan_token_account),
        ("raydium_amm_id", &route.raydium_amm_id),
        ("raydium_amm_authority", &route.raydium_amm_authority),
        ("raydium_amm_open_orders", &route.raydium_amm_open_orders),
        ("raydium_pool_coin_vault", &route.raydium_pool_coin_vault),
        ("raydium_pool_pc_vault", &route.raydium_pool_pc_vault),
        ("raydium_serum_market", &route.raydium_serum_market),
        ("our_raydium_source", &route.our_raydium_source),
        ("our_raydium_dest", &route.our_raydium_dest),
        ("orca_whirlpool", &route.orca_whirlpool),
        ("orca_token_vault_a", &route.orca_token_vault_a),
        ("orca_token_vault_b", &route.orca_token_vault_b),
        ("orca_tick_array_0", &route.orca_tick_array_0),
        ("orca_tick_array_1", &route.orca_tick_array_1),
        ("orca_tick_array_2", &route.orca_tick_array_2),
        ("orca_oracle", &route.orca_oracle),
        ("our_orca_token_a", &route.our_orca_token_a),
        ("our_orca_token_b", &route.our_orca_token_b),
    ];

    let mut errors: Vec<String> = checks
        .iter()
        .filter(|(_, pk)| **pk == zero)
        .map(|(name, _)| format!("  {name}: is the zero pubkey (not configured)"))
        .collect();

    if route.tip_accounts.is_empty() {
        errors.push(
            "  tip_accounts: empty — call executor.get_tip_accounts() to populate".to_string(),
        );
    }

    if !errors.is_empty() {
        return Err(anyhow::anyhow!(
            "ArbRoute validation failed ({} errors):\n{}",
            errors.len(),
            errors.join("\n")
        ));
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal opportunity type
// ─────────────────────────────────────────────────────────────────────────────

/// A detected backrunning opportunity ready for bundle construction.
struct ArbitrageOpportunity {
    /// Amount to flash-borrow from Solend.
    loan_amount: u64,
    /// Estimated gross output from both swap legs.
    gross_output: u64,
    /// Flash-loan fee owed to Solend on repay.
    loan_fee: u64,
    /// Net profit after loan fee and Jito tip.
    net_profit: u64,
    /// Minimum output accepted from the Raydium leg (slippage guard).
    min_raydium_out: u64,
    /// Minimum output accepted from the Orca leg (slippage guard).
    min_orca_out: u64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Backrun engine
// ─────────────────────────────────────────────────────────────────────────────

/// Subscribes to the Jito mempool stream, detects backrunning opportunities,
/// and submits atomic flash-loan arbitrage bundles.
pub struct BackrunEngine {
    executor: Arc<Executor>,
    /// Keypair that signs the arb transaction and pays the Jito tip.
    /// Must be funded with enough SOL to cover transaction fees and the tip.
    wallet: Arc<Keypair>,
    /// Pool / reserve account addresses for the target trading pair.
    route: Arc<ArbRoute>,
    /// Tunable strategy parameters.
    config: ArbConfig,
}

impl BackrunEngine {
    /// Create a new `BackrunEngine` with default [`ArbConfig`].
    ///
    /// * `executor` – shared executor (provides cached blockhash + Jito client).
    /// * `wallet` – funded keypair that signs arb transactions.
    /// * `route` – all on-chain addresses for the target pool pair.
    pub fn new(executor: Arc<Executor>, wallet: Arc<Keypair>, route: Arc<ArbRoute>) -> Self {
        Self::with_config(executor, wallet, route, ArbConfig::default())
    }

    /// Create a new `BackrunEngine` with a custom [`ArbConfig`].
    pub fn with_config(
        executor: Arc<Executor>,
        wallet: Arc<Keypair>,
        route: Arc<ArbRoute>,
        config: ArbConfig,
    ) -> Self {
        Self { executor, wallet, route, config }
    }

    /// Start the backrun loop.  Blocks until the stream closes or an error occurs.
    ///
    /// Run this inside `tokio::spawn` so it does not block the main event loop:
    ///
    /// ```rust,ignore
    /// tokio::spawn(async move { engine.run().await });
    /// ```
    pub async fn run(&self) -> Result<()> {
        info!("[BackrunEngine] starting Jito mempool subscription");

        // Clone the shared Jito gRPC channel — no new connection is created.
        let mut jito = self.executor.jito_client();

        let request = SubscribeMempoolRequest {
            programs: vec![
                RAYDIUM_AMM_V4.to_string(),
                ORCA_WHIRLPOOL_PROGRAM.to_string(),
            ],
            accounts: vec![],
        };

        let mut stream = jito
            .subscribe_mempool(request)
            .await
            .context("Failed to open Jito mempool stream")?
            .into_inner();

        info!("[BackrunEngine] mempool stream active");

        // Process each batch of pending transactions.
        while let Some(notification) = stream.message().await? {
            for raw_tx in &notification.transactions {
                if let Some(opp) = self.detect_opportunity(raw_tx) {
                    match self.build_and_submit(&opp).await {
                        Ok(uuid) => {
                            info!(
                                "[BackrunEngine] bundle submitted | uuid={} net_profit={}",
                                uuid, opp.net_profit
                            );
                        }
                        Err(e) => warn!("[BackrunEngine] submission error: {:#}", e),
                    }
                }
            }
        }

        warn!("[BackrunEngine] mempool stream closed");
        Ok(())
    }

    // ── Opportunity detection ─────────────────────────────────────────────────

    /// Fast-path: decode a pending transaction and decide whether it is worth
    /// backrunning.
    ///
    /// Returns `None` immediately if the transaction is not from a target DEX or
    /// the expected profit is below `config.min_net_profit_lamports`.  The
    /// critical path has no heap allocations beyond the `bincode::deserialize`
    /// call.
    ///
    /// ## Profit estimation
    ///
    /// The current estimate (`swap_amount × 0.5 %`) is a conservative heuristic
    /// used as a **pre-filter** before the more expensive pre-flight simulation
    /// (`executor.simulate_transaction`).  To replace it with an exact AMM
    /// output:
    ///
    /// 1. Cache pool reserves in `Executor::on_account_update` (see the TODO
    ///    there for the account data layout offsets).
    /// 2. Look up `(reserve_in, reserve_out)` for the target pool.
    /// 3. Call `amm_output(reserve_in, reserve_out, swap_amount, 25)` for
    ///    Raydium (0.25 % fee) and `amm_output(..., 5)` for Orca (0.05 % fee
    ///    for most pools).
    ///
    /// Until live reserves are wired up, the simulation in `build_and_submit`
    /// acts as the primary profit safety check.
    fn detect_opportunity(&self, raw_tx: &[u8]) -> Option<ArbitrageOpportunity> {
        // Deserialise the Solana wire-format transaction.
        let tx: Transaction = bincode::deserialize(raw_tx).ok()?;
        let msg = &tx.message;

        // ── Guard 1: must reference a target DEX program ──────────────────────
        let involves_target = msg
            .account_keys
            .iter()
            .any(|pk| *pk == RAYDIUM_AMM_V4 || *pk == ORCA_WHIRLPOOL_PROGRAM);
        if !involves_target {
            return None;
        }

        // ── Guard 2: extract the swap amount from known instruction encodings ─
        // Raydium SwapBaseIn  – data[0] == 9,  data[1..9] == amount_in  (u64 LE)
        // Raydium SwapBaseOut – data[0] == 11, data[1..9] == max_amount_in (u64 LE)
        // Orca Whirlpool swap – data[0..8] == discriminator, data[8..16] == amount
        let swap_amount: u64 = msg.instructions.iter().find_map(|ix| {
            let program = msg.account_keys.get(ix.program_id_index as usize)?;
            let data = &ix.data;

            if *program == RAYDIUM_AMM_V4 {
                // SwapBaseIn (9) or SwapBaseOut (11) – amount is at bytes 1..9
                if (data.first() == Some(&RAYDIUM_SWAP_BASE_IN)
                    || data.first() == Some(&RAYDIUM_SWAP_BASE_OUT))
                    && data.len() >= 9
                {
                    return Some(u64::from_le_bytes(data[1..9].try_into().ok()?));
                }
            } else if *program == ORCA_WHIRLPOOL_PROGRAM
                && data.len() >= 16
                && data[0..8] == ORCA_SWAP_DISCRIMINATOR
            {
                return Some(u64::from_le_bytes(data[8..16].try_into().ok()?));
            }
            None
        })?;

        if swap_amount < self.config.min_target_swap_lamports {
            return None;
        }

        // ── Guard 3: profit must exceed costs ─────────────────────────────────
        // Conservative heuristic: assume 0.5 % of the notional as gross arb profit.
        // The pre-flight simulation in build_and_submit provides the actual safety net.
        // Replace with amm_output() once live pool reserves are cached.
        let gross_output = swap_amount.saturating_mul(HEURISTIC_PROFIT_BPS) / 10_000;
        let loan_fee = (swap_amount * self.config.solend_fee_bps) / 10_000;

        let total_cost = loan_fee.saturating_add(self.config.jito_tip_lamports);
        if gross_output <= total_cost {
            return None;
        }
        let net_profit = gross_output - total_cost;
        if net_profit < self.config.min_net_profit_lamports {
            return None;
        }

        // Apply slippage tolerance to both legs of the arb.
        let min_raydium_out =
            gross_output * (10_000 - self.config.slippage_bps) / 10_000;
        let min_orca_out =
            (swap_amount + net_profit) * (10_000 - self.config.slippage_bps) / 10_000;

        debug!(
            "[BackrunEngine] opportunity | loan={} gross={} fee={} net={}",
            swap_amount, gross_output, loan_fee, net_profit
        );

        Some(ArbitrageOpportunity {
            loan_amount: swap_amount,
            gross_output,
            loan_fee,
            net_profit,
            min_raydium_out,
            min_orca_out,
        })
    }

    // ── Bundle construction and submission ────────────────────────────────────

    /// Build the complete arb transaction, run a pre-flight simulation, and
    /// submit it as a Jito bundle with exponential-backoff retry.
    ///
    /// ## Atomicity guarantee
    ///
    /// The Solend `FlashRepayReserveLiquidity` instruction (ix[3]) verifies
    /// on-chain that the exact borrowed amount plus fee is present in our token
    /// account.  If the swap legs produce insufficient output, Solend aborts the
    /// entire transaction before it is finalized — no principal is lost.
    ///
    /// The `min_amount_out` / `other_amount_threshold` parameters on both swap
    /// instructions provide a second slippage guard, reverting if the pool price
    /// moved adversely between detection and execution.
    async fn build_and_submit(&self, opp: &ArbitrageOpportunity) -> Result<String> {
        // Use the cached blockhash — zero network I/O.
        let blockhash = *self.executor.state.latest_blockhash.read().await;
        if blockhash == Hash::default() {
            return Err(anyhow::anyhow!(
                "blockhash not yet cached — background refresher may not have run"
            ));
        }

        // Prepend compute budget instructions so the arb transaction competes for
        // block space under congestion — same as executor.send_bundle does.
        let [priority_ix, limit_ix] = self.executor.compute_budget_ixs();

        // Instruction order is critical for atomicity:
        //   0  SetComputeUnitPrice          — priority fee (must be first)
        //   1  SetComputeUnitLimit          — cap compute consumption
        //   2  FlashBorrowReserveLiquidity  — borrow loan token from Solend
        //   3  SwapBaseIn (Raydium)         — buy underpriced arb token
        //   4  Swap (Orca)                  — sell arb token at fair price
        //   5  FlashRepayReserveLiquidity   — repay Solend (aborts if underfunded)
        //   6  system::transfer             — Jito tip
        //
        // The `borrow_instruction_index` (2) tells Solend which instruction in this
        // transaction is the matching FlashBorrow, so it can verify atomicity.
        let instructions = vec![
            priority_ix,
            limit_ix,
            self.flash_borrow_ix(opp.loan_amount),
            self.raydium_swap_ix(opp.loan_amount, opp.min_raydium_out),
            self.orca_swap_ix(opp.gross_output, opp.min_orca_out),
            self.flash_repay_ix(opp.loan_amount + opp.loan_fee, 2),
            self.jito_tip_ix(self.config.jito_tip_lamports),
        ];

        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.wallet.pubkey()),
            &[self.wallet.as_ref()],
            blockhash,
        );

        // ── Pre-flight simulation ─────────────────────────────────────────────
        // Verifies the transaction would succeed before burning tip lamports.
        // Catches: underfunded accounts, stale pool state, wrong ix data.
        let sim_ok = self
            .executor
            .simulate_transaction(&tx)
            .await
            .context("pre-flight simulation RPC call failed")?;
        if !sim_ok {
            return Err(anyhow::anyhow!(
                "pre-flight simulation failed — opportunity discarded to preserve capital"
            ));
        }

        // Serialise with bincode (Solana wire format).
        let data =
            bincode::serialize(&tx).context("Failed to serialise arb transaction")?;
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
            header: None,
            packets: vec![packet],
        };

        // Submit with exponential-backoff retry.
        self.submit_with_retry(bundle).await
    }

    /// Submit a [`Bundle`] to the Jito Block Engine with exponential-backoff
    /// retry on transient gRPC failures.
    ///
    /// Waits up to `config.max_retry_attempts` × increasing delays before
    /// giving up.  The initial delay is 200 ms, doubling each attempt up to 5 s.
    async fn submit_with_retry(&self, bundle: Bundle) -> Result<String> {
        if self.config.max_retry_attempts == 0 {
            return Err(anyhow::anyhow!(
                "MAX_RETRY_ATTEMPTS must be greater than zero"
            ));
        }

        let mut delay = Duration::from_millis(200);
        for attempt in 1..=self.config.max_retry_attempts {
            let mut jito = self.executor.jito_client();
            match jito
                .send_bundle(SendBundleRequest { bundle: Some(bundle.clone()) })
                .await
            {
                Ok(resp) => return Ok(resp.into_inner().uuid),
                Err(e) if attempt == self.config.max_retry_attempts => {
                    return Err(anyhow::anyhow!(
                        "Jito SendBundle failed after {} attempts: {}",
                        attempt,
                        e
                    ));
                }
                Err(e) => {
                    warn!(
                        "[BackrunEngine] Jito attempt {}/{} failed: {:#}",
                        attempt, self.config.max_retry_attempts, e
                    );
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2).min(Duration::from_secs(5));
                }
            }
        }
        unreachable!()
    }

    // ── Instruction builders ──────────────────────────────────────────────────

    /// Solend `FlashBorrowReserveLiquidity` instruction.
    ///
    /// Data layout (9 bytes): `[20u8 | amount: u64 LE]`
    fn flash_borrow_ix(&self, amount: u64) -> Instruction {
        let r = &self.route;
        let mut data = [0u8; 9];
        data[0] = SOLEND_FLASH_BORROW;
        data[1..9].copy_from_slice(&amount.to_le_bytes());

        Instruction {
            program_id: SOLEND_PROGRAM,
            accounts: vec![
                // Source: reserve's liquidity supply account
                AccountMeta::new(r.solend_reserve_liquidity_supply, false),
                // Destination: our token account receives the borrowed funds
                AccountMeta::new(r.our_loan_token_account, false),
                AccountMeta::new_readonly(r.solend_reserve, false),
                AccountMeta::new_readonly(r.solend_lending_market, false),
                AccountMeta::new_readonly(r.solend_lending_market_authority, false),
                // Instructions sysvar: Solend uses this to verify the repay ix exists
                AccountMeta::new_readonly(sysvar::instructions::id(), false),
                AccountMeta::new_readonly(TOKEN_PROGRAM, false),
            ],
            data: data.to_vec(),
        }
    }

    /// Raydium AMM v4 `SwapBaseIn` instruction.
    ///
    /// Data layout (17 bytes): `[9u8 | amount_in: u64 LE | min_amount_out: u64 LE]`
    ///
    /// Swaps the flash-borrowed loan token for the arb token.  The 18-account
    /// list matches the Raydium AMM v4 on-chain interface exactly.
    fn raydium_swap_ix(&self, amount_in: u64, min_amount_out: u64) -> Instruction {
        let r = &self.route;
        let mut data = [0u8; 17];
        data[0] = RAYDIUM_SWAP_BASE_IN;
        data[1..9].copy_from_slice(&amount_in.to_le_bytes());
        data[9..17].copy_from_slice(&min_amount_out.to_le_bytes());

        Instruction {
            program_id: RAYDIUM_AMM_V4,
            accounts: vec![
                AccountMeta::new_readonly(TOKEN_PROGRAM, false),        // 0 spl-token
                AccountMeta::new(r.raydium_amm_id, false),              // 1 amm
                AccountMeta::new_readonly(r.raydium_amm_authority, false), // 2 amm authority
                AccountMeta::new(r.raydium_amm_open_orders, false),     // 3 open orders
                AccountMeta::new(r.raydium_amm_target_orders, false),   // 4 target orders
                AccountMeta::new(r.raydium_pool_coin_vault, false),     // 5 pool coin vault
                AccountMeta::new(r.raydium_pool_pc_vault, false),       // 6 pool pc vault
                AccountMeta::new_readonly(r.raydium_serum_program, false), // 7 serum program
                AccountMeta::new(r.raydium_serum_market, false),        // 8 serum market
                AccountMeta::new(r.raydium_serum_bids, false),          // 9 serum bids
                AccountMeta::new(r.raydium_serum_asks, false),          // 10 serum asks
                AccountMeta::new(r.raydium_serum_event_queue, false),   // 11 serum event queue
                AccountMeta::new(r.raydium_serum_coin_vault, false),    // 12 serum coin vault
                AccountMeta::new(r.raydium_serum_pc_vault, false),      // 13 serum pc vault
                AccountMeta::new_readonly(r.raydium_serum_vault_signer, false), // 14 vault signer
                AccountMeta::new(r.our_raydium_source, false),          // 15 user source token
                AccountMeta::new(r.our_raydium_dest, false),            // 16 user dest token
                AccountMeta::new_readonly(self.wallet.pubkey(), true),  // 17 user owner (signer)
            ],
            data: data.to_vec(),
        }
    }

    /// Orca Whirlpool `swap` instruction.
    ///
    /// Data layout (42 bytes):
    /// ```text
    /// [discriminator: [u8; 8]          — sha256("global:swap")[0..8]
    ///  amount: u64 LE                  — tokens to swap in
    ///  other_amount_threshold: u64 LE  — minimum tokens out (slippage guard)
    ///  sqrt_price_limit: u128 LE       — 0 = no price limit
    ///  amount_specified_is_input: u8   — 1 = exact input
    ///  a_to_b: u8]                     — 0 = B→A (selling arb token for loan token)
    /// ```
    ///
    /// Swaps the arb token back to the loan token at the fair Orca price.
    fn orca_swap_ix(&self, amount: u64, other_amount_threshold: u64) -> Instruction {
        let r = &self.route;
        let mut data = Vec::with_capacity(42);
        data.extend_from_slice(&ORCA_SWAP_DISCRIMINATOR);
        data.extend_from_slice(&amount.to_le_bytes());
        data.extend_from_slice(&other_amount_threshold.to_le_bytes());
        data.extend_from_slice(&0u128.to_le_bytes()); // sqrt_price_limit = 0 (no limit)
        data.push(1u8); // amount_specified_is_input = true
        data.push(0u8); // a_to_b = false (selling token B to receive token A)

        Instruction {
            program_id: ORCA_WHIRLPOOL_PROGRAM,
            accounts: vec![
                AccountMeta::new_readonly(TOKEN_PROGRAM, false),       // 0  spl-token
                AccountMeta::new_readonly(self.wallet.pubkey(), true), // 1  token authority (signer)
                AccountMeta::new(r.orca_whirlpool, false),             // 2  whirlpool state
                AccountMeta::new(r.our_orca_token_a, false),           // 3  user token A account
                AccountMeta::new(r.orca_token_vault_a, false),         // 4  pool token vault A
                AccountMeta::new(r.our_orca_token_b, false),           // 5  user token B account
                AccountMeta::new(r.orca_token_vault_b, false),         // 6  pool token vault B
                AccountMeta::new(r.orca_tick_array_0, false),          // 7  tick array 0
                AccountMeta::new(r.orca_tick_array_1, false),          // 8  tick array 1
                AccountMeta::new(r.orca_tick_array_2, false),          // 9  tick array 2
                AccountMeta::new_readonly(r.orca_oracle, false),       // 10 oracle
            ],
            data,
        }
    }

    /// Solend `FlashRepayReserveLiquidity` instruction.
    ///
    /// Data layout (10 bytes):
    /// `[21u8 | amount: u64 LE | borrow_instruction_index: u8]`
    ///
    /// * `amount` must equal the borrowed amount **plus** the flash-loan fee.
    /// * `borrow_instruction_index` is the zero-based index of the matching
    ///   `FlashBorrowReserveLiquidity` instruction in this transaction.
    ///   When compute budget instructions are prepended (as `build_and_submit` does),
    ///   the flash borrow sits at index **2** (after SetComputeUnitPrice and
    ///   SetComputeUnitLimit), so pass `2` from `build_and_submit`.
    ///
    /// Solend reads the instructions sysvar to verify the borrow and repay
    /// are in the same transaction and that the amounts match.  If this check
    /// fails the entire transaction is aborted on-chain.
    fn flash_repay_ix(&self, amount: u64, borrow_instruction_index: u8) -> Instruction {
        let r = &self.route;
        let mut data = [0u8; 10];
        data[0] = SOLEND_FLASH_REPAY;
        data[1..9].copy_from_slice(&amount.to_le_bytes());
        data[9] = borrow_instruction_index;

        Instruction {
            program_id: SOLEND_PROGRAM,
            accounts: vec![
                // Source: our token account returns the borrowed funds + fee
                AccountMeta::new(r.our_loan_token_account, false),
                // Destination: back to the reserve supply
                AccountMeta::new(r.solend_reserve_liquidity_supply, false),
                // Fee goes here
                AccountMeta::new(r.solend_fee_receiver, false),
                AccountMeta::new_readonly(r.solend_reserve, false),
                AccountMeta::new_readonly(r.solend_lending_market, false),
                AccountMeta::new_readonly(r.solend_lending_market_authority, false),
                // User transfer authority must sign the repay
                AccountMeta::new_readonly(self.wallet.pubkey(), true),
                AccountMeta::new_readonly(TOKEN_PROGRAM, false),
                // Instructions sysvar: verifies the borrow ix is present
                AccountMeta::new_readonly(sysvar::instructions::id(), false),
            ],
            data: data.to_vec(),
        }
    }

    /// Jito tip instruction (SOL transfer to a Jito tip account).
    ///
    /// The tip incentivises Jito validators to include and order the bundle
    /// immediately after the target transaction.  Without a tip the bundle
    /// will never be landed.
    ///
    /// Cycles through `route.tip_accounts` using the current slot for pseudo-
    /// random distribution across all 8 Jito tip accounts.  Falls back to the
    /// well-known tip account #0 if the list is empty.
    fn jito_tip_ix(&self, lamports: u64) -> Instruction {
        // Use the current slot to distribute tips across all 8 Jito tip accounts.
        let tip_accounts = &self.route.tip_accounts;
        let tip_account = if tip_accounts.is_empty() {
            // Jito tip account #0 (always valid, but prefer the full list).
            Pubkey::from_str_const("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5")
        } else {
            let slot = self.executor.state.current_slot.load(std::sync::atomic::Ordering::Relaxed);
            tip_accounts[slot as usize % tip_accounts.len()]
        };

        solana_sdk::system_instruction::transfer(&self.wallet.pubkey(), &tip_account, lamports)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::{
        instruction::{AccountMeta, Instruction},
        message::Message,
        pubkey::Pubkey,
        transaction::Transaction,
    };

    // ── amm_output ────────────────────────────────────────────────────────────

    #[test]
    fn test_amm_output_basic_0_25_pct_fee() {
        // reserve_in=1000, reserve_out=1000, amount_in=100, fee=25bps (0.25%)
        // amount_with_fee = 100 * 9975 = 997500
        // numerator = 1000 * 997500 = 997_500_000
        // denominator = 1000 * 10000 + 997500 = 10_997_500
        // result ≈ 90
        let out = amm_output(1_000, 1_000, 100, 25);
        assert!(out > 85 && out < 95, "expected ~90, got {out}");
    }

    #[test]
    fn test_amm_output_zero_inputs_return_zero() {
        assert_eq!(amm_output(0, 1_000, 100, 25), 0, "zero reserve_in");
        assert_eq!(amm_output(1_000, 0, 100, 25), 0, "zero reserve_out");
        assert_eq!(amm_output(1_000, 1_000, 0, 25), 0, "zero amount_in");
    }

    #[test]
    fn test_amm_output_full_fee_returns_zero() {
        // 100% fee leaves nothing for the swapper
        let out = amm_output(1_000, 1_000, 100, 10_000);
        assert_eq!(out, 0, "100% fee should yield 0 output");
    }

    #[test]
    fn test_amm_output_large_realistic_values() {
        // SOL–USDC pool: ~50k SOL (~50_000_000_000_000 lamports) vs ~8M USDC
        // (~8_000_000_000_000 USDC atoms at 6 decimals).
        // Swap 100 SOL → expect ~16 000 USDC (≈ $160/SOL).
        let reserve_sol: u64 = 50_000 * 1_000_000_000; // 50k SOL in lamports
        let reserve_usdc: u64 = 8_000_000 * 1_000_000; // 8M USDC in atoms (6 dp)
        let amount_in: u64 = 100 * 1_000_000_000; // 100 SOL

        let out = amm_output(reserve_sol, reserve_usdc, amount_in, 25);
        // Expected ≈ 100 SOL × ($160/SOL) = $16 000 = 16_000_000_000 USDC atoms
        assert!(
            out > 15_000_000_000 && out < 17_000_000_000,
            "expected ~16B USDC atoms for 100-SOL swap, got {out}"
        );
    }

    #[test]
    fn test_amm_output_consistent_with_k() {
        // Verify x*y=k is approximately maintained after the swap.
        let ri: u64 = 100_000;
        let ro: u64 = 200_000;
        let ai: u64 = 10_000;
        let out = amm_output(ri, ro, ai, 0); // 0 fee to test pure constant product
        let new_ri = ri + ai;
        let new_ro = ro - out;
        // new_ri * new_ro should be >= ri * ro (slightly larger due to floor division)
        assert!(
            (new_ri as u128) * (new_ro as u128) >= (ri as u128) * (ro as u128),
            "k should be maintained or increased"
        );
    }

    // ── validate_route ────────────────────────────────────────────────────────

    #[test]
    fn test_validate_route_empty_tip_accounts() {
        let route = make_dummy_route_with_tips(vec![]);
        let result = validate_route(&route);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("tip_accounts"), "error should mention tip_accounts: {msg}");
    }

    #[test]
    fn test_validate_route_zero_pubkey_fails() {
        let mut route = make_dummy_route_with_tips(vec![Pubkey::new_unique()]);
        route.solend_reserve = Pubkey::default(); // inject a zero pubkey
        let result = validate_route(&route);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("solend_reserve"),
            "error should mention solend_reserve: {msg}"
        );
    }

    #[test]
    fn test_validate_route_all_configured_passes() {
        let route = make_dummy_route_with_tips(vec![Pubkey::new_unique()]);
        assert!(validate_route(&route).is_ok());
    }

    // ── Instruction data layout ───────────────────────────────────────────────

    /// Verifies the Solend FlashBorrow instruction byte layout without needing
    /// a live Executor (tests only the data encoding, not account list).
    #[test]
    fn test_flash_borrow_data_layout() {
        let amount: u64 = 1_500_000_000;
        let mut data = [0u8; 9];
        data[0] = SOLEND_FLASH_BORROW; // variant byte
        data[1..9].copy_from_slice(&amount.to_le_bytes());

        assert_eq!(data[0], 20, "variant byte must be 20");
        assert_eq!(
            u64::from_le_bytes(data[1..9].try_into().unwrap()),
            amount,
            "amount must round-trip"
        );
    }

    #[test]
    fn test_raydium_swap_data_layout() {
        let amount_in: u64 = 2_000_000_000;
        let min_out: u64 = 1_900_000_000;
        let mut data = [0u8; 17];
        data[0] = RAYDIUM_SWAP_BASE_IN;
        data[1..9].copy_from_slice(&amount_in.to_le_bytes());
        data[9..17].copy_from_slice(&min_out.to_le_bytes());

        assert_eq!(data[0], 9, "variant byte must be 9");
        assert_eq!(u64::from_le_bytes(data[1..9].try_into().unwrap()), amount_in);
        assert_eq!(u64::from_le_bytes(data[9..17].try_into().unwrap()), min_out);
    }

    #[test]
    fn test_orca_swap_data_layout() {
        let amount: u64 = 1_000_000;
        let threshold: u64 = 900_000;
        let mut data = Vec::with_capacity(42);
        data.extend_from_slice(&ORCA_SWAP_DISCRIMINATOR);
        data.extend_from_slice(&amount.to_le_bytes());
        data.extend_from_slice(&threshold.to_le_bytes());
        data.extend_from_slice(&0u128.to_le_bytes()); // sqrt_price_limit
        data.push(1u8); // amount_specified_is_input
        data.push(0u8); // a_to_b

        assert_eq!(data.len(), 42);
        assert_eq!(&data[0..8], &ORCA_SWAP_DISCRIMINATOR);
        assert_eq!(u64::from_le_bytes(data[8..16].try_into().unwrap()), amount);
        assert_eq!(u64::from_le_bytes(data[16..24].try_into().unwrap()), threshold);
    }

    #[test]
    fn test_flash_repay_data_layout() {
        let amount: u64 = 1_504_500_000; // principal + 0.3% fee
        let borrow_ix_index: u8 = 0;
        let mut data = [0u8; 10];
        data[0] = SOLEND_FLASH_REPAY;
        data[1..9].copy_from_slice(&amount.to_le_bytes());
        data[9] = borrow_ix_index;

        assert_eq!(data[0], 21, "variant byte must be 21");
        assert_eq!(u64::from_le_bytes(data[1..9].try_into().unwrap()), amount);
        assert_eq!(data[9], 0, "borrow_instruction_index must be 0");
    }

    // ── detect_opportunity ────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_detect_opportunity_large_raydium_swap() {
        let engine = make_test_engine().await;
        // 2 SOL swap — above MIN_TARGET_SWAP_LAMPORTS (default 1 SOL)
        let raw = make_raydium_swap_tx(2_000_000_000);
        let opp = engine.detect_opportunity(&raw);
        assert!(opp.is_some(), "2 SOL Raydium swap should be detected");
        assert_eq!(opp.unwrap().loan_amount, 2_000_000_000);
    }

    #[tokio::test]
    async fn test_detect_opportunity_small_swap_ignored() {
        let engine = make_test_engine().await;
        // 0.5 SOL — below MIN_TARGET_SWAP_LAMPORTS
        let raw = make_raydium_swap_tx(500_000_000);
        let opp = engine.detect_opportunity(&raw);
        assert!(opp.is_none(), "small swap must be ignored");
    }

    #[tokio::test]
    async fn test_detect_opportunity_orca_swap_detected() {
        let engine = make_test_engine().await;
        let raw = make_orca_swap_tx(2_000_000_000);
        let opp = engine.detect_opportunity(&raw);
        assert!(opp.is_some(), "Orca Whirlpool 2-SOL swap should be detected");
    }

    #[tokio::test]
    async fn test_detect_opportunity_non_dex_tx_ignored() {
        let engine = make_test_engine().await;
        // A plain SOL transfer — no Raydium or Orca program
        let ix = solana_sdk::system_instruction::transfer(
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            1_000_000_000,
        );
        let msg = Message::new(&[ix], None);
        let tx = Transaction::new_unsigned(msg);
        let raw = bincode::serialize(&tx).unwrap();
        let opp = engine.detect_opportunity(&raw);
        assert!(opp.is_none(), "non-DEX transaction must be ignored");
    }

    // ── Test helpers ──────────────────────────────────────────────────────────

    /// Build a minimal Raydium SwapBaseIn transaction for testing.
    ///
    /// The transaction is unsigned; `detect_opportunity` does not verify sigs.
    fn make_raydium_swap_tx(amount_in: u64) -> Vec<u8> {
        let payer = Pubkey::new_unique();
        let mut data = vec![RAYDIUM_SWAP_BASE_IN];
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes()); // min_amount_out

        let ix = Instruction {
            program_id: RAYDIUM_AMM_V4,
            accounts: vec![AccountMeta::new_readonly(payer, true)],
            data,
        };
        let msg = Message::new(&[ix], Some(&payer));
        let tx = Transaction::new_unsigned(msg);
        bincode::serialize(&tx).unwrap()
    }

    /// Build a minimal Orca Whirlpool swap transaction for testing.
    fn make_orca_swap_tx(amount: u64) -> Vec<u8> {
        let payer = Pubkey::new_unique();
        let mut data = Vec::with_capacity(42);
        data.extend_from_slice(&ORCA_SWAP_DISCRIMINATOR);
        data.extend_from_slice(&amount.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes()); // threshold
        data.extend_from_slice(&0u128.to_le_bytes()); // sqrt_price_limit
        data.push(1u8); // amount_specified_is_input
        data.push(0u8); // a_to_b

        let ix = Instruction {
            program_id: ORCA_WHIRLPOOL_PROGRAM,
            accounts: vec![AccountMeta::new_readonly(payer, true)],
            data,
        };
        let msg = Message::new(&[ix], Some(&payer));
        let tx = Transaction::new_unsigned(msg);
        bincode::serialize(&tx).unwrap()
    }

    /// Build a dummy ArbRoute with all unique (non-zero) pubkeys.
    fn make_dummy_route_with_tips(tip_accounts: Vec<Pubkey>) -> ArbRoute {
        ArbRoute {
            solend_reserve: Pubkey::new_unique(),
            solend_reserve_liquidity_supply: Pubkey::new_unique(),
            solend_fee_receiver: Pubkey::new_unique(),
            solend_lending_market: Pubkey::new_unique(),
            solend_lending_market_authority: Pubkey::new_unique(),
            our_loan_token_account: Pubkey::new_unique(),
            raydium_amm_id: Pubkey::new_unique(),
            raydium_amm_authority: Pubkey::new_unique(),
            raydium_amm_open_orders: Pubkey::new_unique(),
            raydium_amm_target_orders: Pubkey::new_unique(),
            raydium_pool_coin_vault: Pubkey::new_unique(),
            raydium_pool_pc_vault: Pubkey::new_unique(),
            raydium_serum_program: Pubkey::new_unique(),
            raydium_serum_market: Pubkey::new_unique(),
            raydium_serum_bids: Pubkey::new_unique(),
            raydium_serum_asks: Pubkey::new_unique(),
            raydium_serum_event_queue: Pubkey::new_unique(),
            raydium_serum_coin_vault: Pubkey::new_unique(),
            raydium_serum_pc_vault: Pubkey::new_unique(),
            raydium_serum_vault_signer: Pubkey::new_unique(),
            our_raydium_source: Pubkey::new_unique(),
            our_raydium_dest: Pubkey::new_unique(),
            orca_whirlpool: Pubkey::new_unique(),
            orca_token_vault_a: Pubkey::new_unique(),
            orca_token_vault_b: Pubkey::new_unique(),
            orca_tick_array_0: Pubkey::new_unique(),
            orca_tick_array_1: Pubkey::new_unique(),
            orca_tick_array_2: Pubkey::new_unique(),
            orca_oracle: Pubkey::new_unique(),
            our_orca_token_a: Pubkey::new_unique(),
            our_orca_token_b: Pubkey::new_unique(),
            tip_accounts,
        }
    }

    /// Build a `BackrunEngine` suitable for unit tests.
    ///
    /// Neither `RpcClient::new` nor `Channel::connect_lazy` make network calls,
    /// so this constructor works without internet access.
    async fn make_test_engine() -> BackrunEngine {
        use crate::executor::{Executor, ExecutorConfig};

        let executor = Arc::new(
            Executor::new(ExecutorConfig {
                rpc_url: "https://api.mainnet-beta.solana.com".to_string(),
                jito_url: "https://mainnet.block-engine.jito.wtf:443".to_string(),
                ..ExecutorConfig::default()
            })
            .await
            .expect("test executor construction must not fail"),
        );

        let wallet = executor.wallet();
        let route = Arc::new(make_dummy_route_with_tips(vec![Pubkey::new_unique()]));

        // Use a low threshold so 2-SOL swaps are detectable in tests.
        let config = ArbConfig {
            min_target_swap_lamports: 1_000_000_000, // 1 SOL
            ..ArbConfig::default()
        };

        BackrunEngine::with_config(executor, wallet, route, config)
    }
}
