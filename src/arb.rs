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
    std::sync::Arc,
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

/// Anchor discriminator for the Orca Whirlpool `swap` instruction.
/// Computed as: `sha256("global:swap")[0..8]`
const ORCA_SWAP_DISCRIMINATOR: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];

// ─────────────────────────────────────────────────────────────────────────────
// Tunable parameters
// ─────────────────────────────────────────────────────────────────────────────

/// Solend flash-loan fee rate in basis points (30 bps = 0.3 %).
const SOLEND_FEE_BPS: u64 = 30;

/// Jito tip in lamports paid to the validator per bundle.
/// Increase to improve inclusion priority under competition.
const JITO_TIP_LAMPORTS: u64 = 100_000; // 0.0001 SOL

/// Minimum net profit (lamports) after flash-loan fee and Jito tip.
/// Bundles expected to earn less than this are discarded without submission.
const MIN_NET_PROFIT_LAMPORTS: u64 = 50_000; // 0.00005 SOL

/// Minimum SOL-equivalent notional of the detected swap to trigger a backrun.
/// Prevents spending compute cycles on micro-swaps with negligible price impact.
const MIN_TARGET_SWAP_LAMPORTS: u64 = 1_000_000_000; // 1 SOL

/// Slippage tolerance in basis points (200 bps = 2 %).
const SLIPPAGE_BPS: u64 = 200;

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
}

impl BackrunEngine {
    /// Create a new `BackrunEngine`.
    ///
    /// * `executor` – shared executor (provides cached blockhash + Jito client).
    /// * `wallet` – funded keypair that signs arb transactions.
    /// * `route` – all on-chain addresses for the target pool pair.
    pub fn new(executor: Arc<Executor>, wallet: Arc<Keypair>, route: Arc<ArbRoute>) -> Self {
        Self { executor, wallet, route }
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
                match self.detect_opportunity(raw_tx) {
                    Some(opp) => {
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
                    None => {} // not an opportunity — continue without allocation
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
    /// the expected profit is below `MIN_NET_PROFIT_LAMPORTS`.  The critical path
    /// has no heap allocations beyond the `bincode::deserialize` call.
    fn detect_opportunity(&self, raw_tx: &[u8]) -> Option<ArbitrageOpportunity> {
        // Deserialise the Solana wire-format transaction.
        let tx: Transaction = bincode::deserialize(raw_tx).ok()?;
        let msg = &tx.message;

        // ── Guard 1: must reference a target DEX program ──────────────────────
        let involves_target = msg.account_keys.iter().any(|pk| {
            *pk == RAYDIUM_AMM_V4 || *pk == ORCA_WHIRLPOOL_PROGRAM
        });
        if !involves_target {
            return None;
        }

        // ── Guard 2: extract the swap amount from a known instruction encoding ─
        // Raydium SwapBaseIn  – data[0] == 9,  data[1..9] == amount_in (u64 LE)
        // Orca Whirlpool swap – data[0..8] == discriminator, data[8..16] == amount
        let swap_amount: u64 = msg.instructions.iter().find_map(|ix| {
            let program = msg.account_keys.get(ix.program_id_index as usize)?;
            let data = &ix.data;

            if *program == RAYDIUM_AMM_V4 {
                if data.first() == Some(&RAYDIUM_SWAP_BASE_IN) && data.len() >= 9 {
                    return Some(u64::from_le_bytes(data[1..9].try_into().ok()?));
                }
            } else if *program == ORCA_WHIRLPOOL_PROGRAM {
                if data.len() >= 16 && &data[0..8] == &ORCA_SWAP_DISCRIMINATOR {
                    return Some(u64::from_le_bytes(data[8..16].try_into().ok()?));
                }
            }
            None
        })?;

        if swap_amount < MIN_TARGET_SWAP_LAMPORTS {
            return None;
        }

        // ── Guard 3: profit must exceed costs ─────────────────────────────────
        // Conservative: assume 0.5 % of the notional as gross arb profit.
        // Replace with an exact AMM output calculation using live pool reserves
        // for production use.
        let gross_output = swap_amount / 200; // 0.5 %
        let loan_fee = (swap_amount * SOLEND_FEE_BPS) / 10_000;

        let total_cost = loan_fee.saturating_add(JITO_TIP_LAMPORTS);
        if gross_output <= total_cost {
            return None;
        }
        let net_profit = gross_output - total_cost;
        if net_profit < MIN_NET_PROFIT_LAMPORTS {
            return None;
        }

        // Apply slippage tolerance to both legs of the arb.
        let min_raydium_out = gross_output * (10_000 - SLIPPAGE_BPS) / 10_000;
        let min_orca_out = (swap_amount + net_profit) * (10_000 - SLIPPAGE_BPS) / 10_000;

        debug!(
            "[BackrunEngine] opportunity found | loan={} gross={} fee={} net={}",
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

    /// Build the complete arb transaction, wrap it in a Jito bundle, and submit.
    async fn build_and_submit(&self, opp: &ArbitrageOpportunity) -> Result<String> {
        // Use the cached blockhash — zero network I/O.
        let blockhash = *self.executor.state.latest_blockhash.read().await;
        if blockhash == Hash::default() {
            return Err(anyhow::anyhow!(
                "blockhash not yet cached — background refresher may not have run"
            ));
        }

        // Instruction order is critical for atomicity:
        //   0  FlashBorrowReserveLiquidity  — borrow loan token from Solend
        //   1  SwapBaseIn (Raydium)         — buy underpriced arb token
        //   2  Swap (Orca)                  — sell arb token at fair price
        //   3  FlashRepayReserveLiquidity   — repay Solend (aborts if underfunded)
        //   4  system::transfer             — Jito tip
        let instructions = [
            self.flash_borrow_ix(opp.loan_amount),
            self.raydium_swap_ix(opp.loan_amount, opp.min_raydium_out),
            self.orca_swap_ix(opp.gross_output, opp.min_orca_out),
            self.flash_repay_ix(opp.loan_amount + opp.loan_fee, 0),
            self.jito_tip_ix(JITO_TIP_LAMPORTS),
        ];

        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.wallet.pubkey()),
            &[self.wallet.as_ref()],
            blockhash,
        );

        // Serialise with bincode (Solana wire format).
        let data = bincode::serialize(&tx).context("Failed to serialise arb transaction")?;
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

        let bundle = Bundle { packets: vec![packet] };

        let mut jito = self.executor.jito_client();
        let response = jito
            .send_bundle(SendBundleRequest { bundle: Some(bundle) })
            .await
            .context("Jito SendBundle RPC failed")?;

        Ok(response.into_inner().uuid)
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
    ///   `FlashBorrowReserveLiquidity` instruction in this transaction (0 here).
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
    /// random distribution.  Falls back to the well-known tip account if the
    /// list is empty.
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
