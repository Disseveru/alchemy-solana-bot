//! # Execution Logic – High-Performance Flash-Loan Arbitrage Executor
//!
//! This module keeps the newer executor interfaces from `main` (shared wallet,
//! cached blockhash helpers, reusable Jito client) while preserving this PR's
//! flash-loan/Jito bundle execution flow.

use {
    crate::jito::{
        bundle::{bundle_result::Result as BundleResultType, rejected::Reason, Bundle},
        get_searcher_client_no_auth, proto_packet_from_versioned_tx,
        searcher::{
            searcher_service_client::SearcherServiceClient, GetTipAccountsRequest,
            NextScheduledLeaderRequest, SendBundleRequest, SubscribeBundleResultsRequest,
        },
    },
    anyhow::{anyhow, bail, Context, Result},
    futures::StreamExt,
    log::{debug, info, warn},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        commitment_config::CommitmentConfig,
        compute_budget::ComputeBudgetInstruction,
        hash::Hash,
        instruction::{AccountMeta, Instruction},
        pubkey::Pubkey,
        signature::{Keypair, Signature},
        signer::Signer,
        transaction::{Transaction, VersionedTransaction},
    },
    std::{
        collections::HashMap,
        env,
        str::FromStr,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    },
    tokio::{
        sync::RwLock,
        time::{sleep, timeout},
    },
    tonic::transport::Channel,
    yellowstone_grpc_proto::prelude::{
        SubscribeUpdateAccountInfo, SubscribeUpdateTransactionInfo, TransactionStatusMeta,
    },
};

const JUPITER_V6_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4");
const RAYDIUM_LIQUIDITY_POOL_V4: Pubkey =
    Pubkey::from_str_const("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8");
const ORCA_WHIRLPOOL_PROGRAM: Pubkey =
    Pubkey::from_str_const("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc");
const SOLEND_PROGRAM: Pubkey =
    Pubkey::from_str_const("So1endD9AkS2mttmbeS8S96ySZps9SML7RnZBjWdLLV");

const WATCHED_PROGRAMS: [Pubkey; 4] = [
    RAYDIUM_LIQUIDITY_POOL_V4,
    ORCA_WHIRLPOOL_PROGRAM,
    SOLEND_PROGRAM,
    JUPITER_V6_PROGRAM_ID,
];

const DEFAULT_MIN_LAMPORTS_DELTA: u64 = 100_000_000;
const BPS_DENOMINATOR: u64 = 10_000;
const DEFAULT_FLASH_LOAN_FEE_BPS: u64 = 30;
const DEFAULT_JITO_TIP_LAMPORTS: u64 = 50_000;
const DEFAULT_NETWORK_FEE_LAMPORTS: u64 = 15_000;
const DEFAULT_MIN_OBSERVED_NOTIONAL_LAMPORTS: u64 = 1_000_000_000;
const DEFAULT_EXPECTED_EDGE_BPS: u64 = 35;
const DEFAULT_MAX_JITO_LEADER_SLOTS: u64 = 2;
const BUNDLE_RESULT_TIMEOUT: Duration = Duration::from_secs(5);
const RPC_CONFIRM_RETRIES: usize = 10;
const RPC_CONFIRM_DELAY: Duration = Duration::from_millis(500);
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYDk2Rf6zqsD7xCMV5aPSi1R";

pub struct SharedState {
    pub latest_blockhash: RwLock<Hash>,
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

#[derive(Debug)]
pub struct ExecutorConfig {
    pub rpc_url: String,
    pub jito_url: String,
    pub min_lamports: u64,
    pub compute_unit_price: u64,
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

#[derive(Debug, Clone)]
struct AtomicArbStrategy {
    block_engine_url: Option<String>,
    max_leader_slots: u64,
    flash_loan_fee_bps: u64,
    jito_tip_lamports: u64,
    network_fee_lamports: u64,
    min_observed_notional_lamports: u64,
    expected_edge_bps: u64,
    static_tip_account: Option<Pubkey>,
    borrow: StrategyInstructionTemplate,
    swap: StrategyInstructionTemplate,
    repay: StrategyInstructionTemplate,
}

#[derive(Debug, Clone)]
struct StrategyInstructionTemplate {
    label: &'static str,
    program_id: Pubkey,
    accounts: Vec<AccountMeta>,
    discriminator: Vec<u8>,
}

#[derive(Debug, Clone)]
struct ArbitrageOpportunity {
    observed_signature: String,
    slot: u64,
    kind: OpportunityKind,
    flash_loan_amount_lamports: u64,
    expected_profit_lamports: u64,
    flash_loan_fee_lamports: u64,
    total_cost_lamports: u64,
    jito_tip_lamports: u64,
    min_swap_out_lamports: u64,
}

#[derive(Debug, Clone, Copy)]
enum OpportunityKind {
    Swap,
    Liquidation,
}

pub struct Executor {
    config: ExecutorConfig,
    rpc: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    pub state: Arc<SharedState>,
    jito_client: SearcherServiceClient<Channel>,
    min_lamports: u64,
    strategy: Option<AtomicArbStrategy>,
}

impl Executor {
    pub async fn new(config: ExecutorConfig) -> Result<Self> {
        let rpc = Arc::new(RpcClient::new(config.rpc_url.clone()));

        let wallet: Arc<Keypair> = if let Ok(secret) = std::env::var("WALLET_PRIVATE_KEY") {
            let key_bytes = bs58::decode(&secret)
                .into_vec()
                .context("WALLET_PRIVATE_KEY contains invalid base-58")?;
            let kp = Keypair::try_from(key_bytes.as_slice())
                .map_err(|e| anyhow!("Invalid keypair bytes in WALLET_PRIVATE_KEY: {:?}", e))?;
            info!(
                "[Executor] wallet loaded from WALLET_PRIVATE_KEY | pubkey={}",
                kp.pubkey()
            );
            Arc::new(kp)
        } else if let Ok(path) = std::env::var("WALLET_KEY_FILE") {
            let kp = solana_sdk::signature::read_keypair_file(&path)
                .map_err(|e| anyhow!("Failed to read keypair file '{}': {}", path, e))?;
            info!("[Executor] wallet loaded from {} | pubkey={}", path, kp.pubkey());
            Arc::new(kp)
        } else {
            warn!(
                "[Executor] WALLET_PRIVATE_KEY and WALLET_KEY_FILE not set – using ephemeral keypair"
            );
            Arc::new(Keypair::new())
        };

        let channel = Channel::from_shared(config.jito_url.clone())
            .context("Invalid JITO_BLOCK_ENGINE_URL")?
            .connect_lazy();
        let jito_client = SearcherServiceClient::new(channel);

        let strategy = match AtomicArbStrategy::from_env() {
            Ok(strategy) => strategy,
            Err(error) => {
                warn!(
                    "[Executor] flash-loan strategy disabled because configuration failed: {:#}",
                    error
                );
                None
            }
        };

        info!(
            "[Executor] initialised | rpc={} | jito={} | wallet={} | compute_price={} µL/CU | compute_limit={} CU | flash_loan_enabled={}",
            config.rpc_url,
            config.jito_url,
            wallet.pubkey(),
            config.compute_unit_price,
            config.compute_unit_limit,
            strategy.is_some(),
        );

        let min_lamports = config.min_lamports;
        Ok(Self {
            config,
            rpc,
            wallet,
            state: Arc::new(SharedState::new()),
            jito_client,
            min_lamports,
            strategy,
        })
    }

    pub fn wallet(&self) -> Arc<Keypair> {
        Arc::clone(&self.wallet)
    }

    pub fn update_slot(&self, slot: u64) {
        self.state.current_slot.store(slot, Ordering::Relaxed);
    }

    pub async fn refresh_blockhash(&self) -> Result<()> {
        let bh = self.rpc.get_latest_blockhash().await?;
        *self.state.latest_blockhash.write().await = bh;
        debug!("[Executor] blockhash refreshed: {}", bh);
        Ok(())
    }

    pub fn compute_budget_ixs(&self) -> [Instruction; 2] {
        [
            ComputeBudgetInstruction::set_compute_unit_price(self.config.compute_unit_price),
            ComputeBudgetInstruction::set_compute_unit_limit(self.config.compute_unit_limit),
        ]
    }

    pub fn jito_client(
        &self,
    ) -> crate::jito::searcher::searcher_service_client::SearcherServiceClient<Channel> {
        self.jito_client.clone()
    }

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

        let owner = Pubkey::try_from(account.owner.as_slice()).unwrap_or_default();
        if !WATCHED_PROGRAMS.contains(&owner) {
            return Ok(());
        }

        debug!(
            "[Executor] pool account update | pubkey={} owner={} data_len={} slot={}",
            pubkey,
            owner,
            account.data.len(),
            slot
        );

        Ok(())
    }

    pub async fn on_transaction(
        &self,
        tx: &SubscribeUpdateTransactionInfo,
        slot: u64,
    ) -> Result<()> {
        let meta = match tx.meta.as_ref() {
            Some(m) if m.err.is_none() => m,
            _ => return Ok(()),
        };

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

        let largest_delta = meta
            .pre_balances
            .iter()
            .zip(meta.post_balances.iter())
            .map(|(pre, post)| pre.abs_diff(*post))
            .max()
            .unwrap_or(0);
        let has_swap_log = meta
            .log_messages
            .iter()
            .any(|log| log.contains("Swap") || log.contains("swap"));
        if largest_delta < self.min_lamports && !has_swap_log {
            return Ok(());
        }

        debug!(
            "[Executor] confirmed DEX swap | slot={} delta={} lamports has_swap_log={}",
            slot, largest_delta, has_swap_log
        );

        let Some(strategy) = self.strategy.as_ref() else {
            return Ok(());
        };

        let Some(opportunity) = self.detect_opportunity(tx, slot, strategy)? else {
            return Ok(());
        };

        info!(
            "[Executor] opportunity detected | source_sig={} slot={} kind={:?} borrow={} expected_profit={} total_cost={}",
            opportunity.observed_signature,
            opportunity.slot,
            opportunity.kind,
            opportunity.flash_loan_amount_lamports,
            opportunity.expected_profit_lamports,
            opportunity.total_cost_lamports,
        );

        let instructions = self
            .build_atomic_flash_loan_instructions(strategy, &opportunity)
            .with_context(|| {
                format!(
                    "failed to build atomic flash-loan transaction for {}",
                    opportunity.observed_signature
                )
            })?;

        match self.execute_opportunity(&opportunity, instructions).await {
            Ok(signature) => {
                info!(
                    "[Executor] arbitrage submitted | source_sig={} backrun_sig={}",
                    opportunity.observed_signature,
                    signature
                );
            }
            Err(error) => {
                warn!(
                    "[Executor] arbitrage execution failed | source_sig={} expected_profit={} total_cost={} error={:#}",
                    opportunity.observed_signature,
                    opportunity.expected_profit_lamports,
                    opportunity.total_cost_lamports,
                    error
                );
            }
        }

        Ok(())
    }

    async fn execute_opportunity(
        &self,
        opportunity: &ArbitrageOpportunity,
        instructions: Vec<Instruction>,
    ) -> Result<Signature> {
        if opportunity.expected_profit_lamports <= opportunity.total_cost_lamports {
            bail!(
                "pre-execution profit check failed: expected_profit={} total_cost={}",
                opportunity.expected_profit_lamports,
                opportunity.total_cost_lamports
            );
        }

        let pre_balance = self.rpc.get_balance(&self.wallet.pubkey()).await.unwrap_or_default();
        let [priority_ix, limit_ix] = self.compute_budget_ixs();
        let mut all_instructions = Vec::with_capacity(instructions.len() + 2);
        all_instructions.push(priority_ix);
        all_instructions.push(limit_ix);
        all_instructions.extend(instructions);

        if let Some(strategy) = self.strategy.as_ref() {
            if let Some(block_engine_url) = strategy.block_engine_url.as_deref() {
            let tip_account = self.resolve_tip_account(strategy, block_engine_url).await?;
            #[allow(deprecated)]
            let tip_instruction = solana_sdk::system_instruction::transfer(
                &self.wallet.pubkey(),
                &tip_account,
                opportunity.jito_tip_lamports,
            );
            all_instructions.push(tip_instruction);
            }
        }

        let recent_blockhash = self.rpc.get_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(
            &all_instructions,
            Some(&self.wallet.pubkey()),
            &[self.wallet.as_ref()],
            recent_blockhash,
        );
        let versioned_tx = VersionedTransaction::from(tx.clone());
        let sig = versioned_tx.signatures[0];

        if let Some(block_engine_url) = self
            .strategy
            .as_ref()
            .and_then(|strategy| strategy.block_engine_url.as_deref())
        {
            self.submit_jito_bundle(block_engine_url, &versioned_tx).await?;
            self.wait_for_signature(&sig).await?;
        } else {
            self.rpc.send_and_confirm_transaction(&tx).await?;
        }

        let post_balance = self.rpc.get_balance(&self.wallet.pubkey()).await.unwrap_or_default();
        let actual_balance_delta = post_balance as i128 - pre_balance as i128;
        info!(
            "[Executor] profit report | expected_profit={} actual_balance_delta={} flash_loan_fee={} jito_tip={} network_fees={} sig={}",
            opportunity.expected_profit_lamports,
            actual_balance_delta,
            opportunity.flash_loan_fee_lamports,
            opportunity.jito_tip_lamports,
            opportunity
                .total_cost_lamports
                .saturating_sub(opportunity.flash_loan_fee_lamports + opportunity.jito_tip_lamports),
            sig,
        );

        Ok(sig)
    }

    #[allow(dead_code)]
    pub async fn send_bundle(&self, instructions: Vec<Instruction>) -> Result<String> {
        let blockhash = *self.state.latest_blockhash.read().await;
        if blockhash == Hash::default() {
            return Err(anyhow!(
                "[Executor] blockhash not yet cached – background refresher may not have run"
            ));
        }

        let [priority_ix, limit_ix] = self.compute_budget_ixs();
        let mut all_instructions = Vec::with_capacity(instructions.len() + 2);
        all_instructions.push(priority_ix);
        all_instructions.push(limit_ix);
        all_instructions.extend(instructions);

        let tx = Transaction::new_signed_with_payer(
            &all_instructions,
            Some(&self.wallet.pubkey()),
            &[self.wallet.as_ref()],
            blockhash,
        );
        let packet = proto_packet_from_versioned_tx(&VersionedTransaction::from(tx))?;

        let mut client = self.jito_client.clone();
        let response = client
            .send_bundle(SendBundleRequest {
                bundle: Some(Bundle {
                    header: None,
                    packets: vec![packet],
                }),
            })
            .await
            .context("Jito SendBundle RPC failed")?;

        let uuid = response.into_inner().uuid;
        info!("[Executor] bundle submitted | uuid={}", uuid);
        Ok(uuid)
    }

    pub async fn simulate_transaction(&self, tx: &Transaction) -> Result<bool> {
        use solana_client::rpc_config::RpcSimulateTransactionConfig;

        let config = RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
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

    #[allow(dead_code)]
    pub async fn get_tip_accounts(&self) -> Result<Vec<String>> {
        let mut client = self.jito_client.clone();
        let response = client
            .get_tip_accounts(GetTipAccountsRequest {})
            .await
            .context("Jito GetTipAccounts RPC failed")?;
        Ok(response.into_inner().accounts)
    }

    #[allow(dead_code)]
    pub async fn send_transaction(&self, instructions: Vec<Instruction>) -> Result<Signature> {
        let [priority_ix, limit_ix] = self.compute_budget_ixs();
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

    fn detect_opportunity(
        &self,
        tx: &SubscribeUpdateTransactionInfo,
        slot: u64,
        strategy: &AtomicArbStrategy,
    ) -> Result<Option<ArbitrageOpportunity>> {
        let Some(meta) = tx.meta.as_ref() else {
            return Ok(None);
        };
        if meta.err.is_some() {
            return Ok(None);
        }

        let log_matches = meta.log_messages.iter().any(|line| {
            let line = line.to_ascii_lowercase();
            ["swap", "liquidat", "ray_log", "jupiter", "amm"]
                .iter()
                .any(|needle| line.contains(needle))
        });
        let largest_notional = self.estimate_notional_lamports(meta);
        if !log_matches && largest_notional < strategy.min_observed_notional_lamports {
            return Ok(None);
        }

        let kind = if meta
            .log_messages
            .iter()
            .any(|line| line.to_ascii_lowercase().contains("liquidat"))
        {
            OpportunityKind::Liquidation
        } else {
            OpportunityKind::Swap
        };

        let flash_loan_amount_lamports =
            largest_notional.max(strategy.min_observed_notional_lamports);
        let expected_profit_lamports = flash_loan_amount_lamports
            .saturating_mul(strategy.expected_edge_bps)
            .saturating_div(BPS_DENOMINATOR);
        let flash_loan_fee_lamports = ceil_div(
            flash_loan_amount_lamports.saturating_mul(strategy.flash_loan_fee_bps),
            BPS_DENOMINATOR,
        );
        let total_cost_lamports = flash_loan_fee_lamports
            .saturating_add(strategy.jito_tip_lamports)
            .saturating_add(strategy.network_fee_lamports);
        if expected_profit_lamports <= total_cost_lamports {
            debug!(
                "[Executor] skipping tx={} because expected_profit={} <= total_cost={}",
                bs58::encode(&tx.signature).into_string(),
                expected_profit_lamports,
                total_cost_lamports
            );
            return Ok(None);
        }

        Ok(Some(ArbitrageOpportunity {
            observed_signature: bs58::encode(&tx.signature).into_string(),
            slot,
            kind,
            flash_loan_amount_lamports,
            expected_profit_lamports,
            flash_loan_fee_lamports,
            total_cost_lamports,
            jito_tip_lamports: strategy.jito_tip_lamports,
            min_swap_out_lamports: flash_loan_amount_lamports
                .saturating_add(total_cost_lamports)
                .saturating_add(expected_profit_lamports),
        }))
    }

    fn estimate_notional_lamports(&self, meta: &TransactionStatusMeta) -> u64 {
        let native_balance_delta = meta
            .pre_balances
            .iter()
            .zip(meta.post_balances.iter())
            .map(|(pre, post)| pre.abs_diff(*post))
            .max()
            .unwrap_or_default();

        let mut pre_balances = HashMap::new();
        for balance in &meta.pre_token_balances {
            pre_balances.insert((balance.account_index, balance.mint.clone()), balance);
        }

        let token_balance_delta = meta
            .post_token_balances
            .iter()
            .filter_map(|post| {
                let pre = pre_balances.get(&(post.account_index, post.mint.clone()))?;
                let pre_ui = pre.ui_token_amount.as_ref()?;
                let post_ui = post.ui_token_amount.as_ref()?;
                let pre_amount = pre_ui.amount.parse::<u64>().ok()?;
                let post_amount = post_ui.amount.parse::<u64>().ok()?;
                Some(self.token_amount_to_lamports_equivalent(
                    &post.mint,
                    post_ui.decimals,
                    pre_amount.abs_diff(post_amount),
                ))
            })
            .max()
            .unwrap_or_default();

        native_balance_delta.max(token_balance_delta)
    }

    fn token_amount_to_lamports_equivalent(&self, mint: &str, decimals: u32, amount: u64) -> u64 {
        if amount == 0 {
            return 0;
        }
        // `spl_token::native_mint::id()` is this same well-known wSOL mint address.
        if mint == WSOL_MINT {
            return amount;
        }
        if mint == USDC_MINT || mint == USDT_MINT {
            return amount.saturating_mul(1_000);
        }

        match decimals.cmp(&9) {
            std::cmp::Ordering::Equal => amount,
            std::cmp::Ordering::Less => amount.saturating_mul(10u64.saturating_pow(9 - decimals)),
            std::cmp::Ordering::Greater => amount / 10u64.saturating_pow(decimals - 9),
        }
    }

    fn build_atomic_flash_loan_instructions(
        &self,
        strategy: &AtomicArbStrategy,
        opportunity: &ArbitrageOpportunity,
    ) -> Result<Vec<Instruction>> {
        let borrow_ix = strategy
            .borrow
            .render_borrow(opportunity.flash_loan_amount_lamports)?;
        let swap_ix = strategy.swap.render_swap(
            opportunity.flash_loan_amount_lamports,
            opportunity.min_swap_out_lamports,
        )?;
        let repay_ix = strategy.repay.render_repay(
            opportunity.flash_loan_amount_lamports,
            opportunity.flash_loan_fee_lamports,
        )?;

        Ok(vec![borrow_ix, swap_ix, repay_ix])
    }

    async fn resolve_tip_account(
        &self,
        strategy: &AtomicArbStrategy,
        block_engine_url: &str,
    ) -> Result<Pubkey> {
        if let Some(account) = strategy.static_tip_account {
            return Ok(account);
        }

        let mut client = get_searcher_client_no_auth(block_engine_url)
            .await
            .with_context(|| format!("failed to connect to Jito block engine at {block_engine_url}"))?;
        let response = client
            .get_tip_accounts(GetTipAccountsRequest {})
            .await
            .context("failed to fetch Jito tip accounts")?
            .into_inner();
        let account = response
            .accounts
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("Jito did not return any tip accounts"))?;
        let account = Pubkey::from_str(&account)
            .context("invalid Jito tip account returned by block engine")?;
        debug!("[Executor] fetched Jito tip account {}", account);
        Ok(account)
    }

    async fn submit_jito_bundle(
        &self,
        block_engine_url: &str,
        tx: &VersionedTransaction,
    ) -> Result<()> {
        let strategy = self
            .strategy
            .as_ref()
            .ok_or_else(|| anyhow!("strategy must be configured before bundle submission"))?;
        let mut client = get_searcher_client_no_auth(block_engine_url)
            .await
            .with_context(|| format!("failed to connect to Jito block engine at {block_engine_url}"))?;

        let next_leader = client
            .get_next_scheduled_leader(NextScheduledLeaderRequest { regions: vec![] })
            .await
            .context("failed to fetch next Jito leader")?
            .into_inner();
        let slots_until_leader = next_leader
            .next_leader_slot
            .saturating_sub(next_leader.current_slot);
        if slots_until_leader > strategy.max_leader_slots {
            bail!(
                "next Jito leader is {} slots away, above configured limit {}",
                slots_until_leader,
                strategy.max_leader_slots
            );
        }

        let mut bundle_results = client
            .subscribe_bundle_results(SubscribeBundleResultsRequest {})
            .await
            .context("failed to subscribe to Jito bundle results")?
            .into_inner();

        let packet = proto_packet_from_versioned_tx(tx)?;
        let response = client
            .send_bundle(SendBundleRequest {
                bundle: Some(Bundle {
                    header: None,
                    packets: vec![packet],
                }),
            })
            .await
            .context("failed to send bundle to Jito")?
            .into_inner();

        let bundle_id = response.uuid;
        info!("[Executor] Jito bundle submitted | bundle_id={}", bundle_id);

        let deadline = Instant::now() + BUNDLE_RESULT_TIMEOUT;
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match timeout(remaining, bundle_results.next()).await {
                Ok(Some(Ok(result))) => {
                    if result.bundle_id != bundle_id {
                        continue;
                    }
                    match result.result {
                        Some(BundleResultType::Accepted(accepted)) => {
                            info!(
                                "[Executor] bundle accepted | bundle_id={} slot={} validator={}",
                                bundle_id,
                                accepted.slot,
                                accepted.validator_identity
                            );
                            return Ok(());
                        }
                        Some(BundleResultType::Rejected(rejected)) => {
                            let reason = match rejected.reason {
                                Some(Reason::WinningBatchBidRejected(reason)) => format!(
                                    "winning batch bid rejected: auction={} tip={}",
                                    reason.auction_id, reason.simulated_bid_lamports
                                ),
                                Some(Reason::StateAuctionBidRejected(reason)) => format!(
                                    "state auction rejected: auction={} tip={}",
                                    reason.auction_id, reason.simulated_bid_lamports
                                ),
                                Some(Reason::SimulationFailure(reason)) => format!(
                                    "simulation failure: tx_signature={} msg={:?}",
                                    reason.tx_signature, reason.msg
                                ),
                                Some(Reason::InternalError(reason)) => {
                                    format!("internal error: {}", reason.msg)
                                }
                                Some(Reason::DroppedBundle(reason)) => {
                                    format!("dropped bundle: {}", reason.msg)
                                }
                                None => "unknown rejection".to_string(),
                            };
                            bail!("bundle {} rejected by Jito: {}", bundle_id, reason);
                        }
                        Some(BundleResultType::Processed(processed)) => {
                            debug!("[Executor] bundle processed | bundle_id={} msg={:?}", bundle_id, processed);
                        }
                        Some(BundleResultType::Finalized(finalized)) => {
                            info!("[Executor] bundle finalized | bundle_id={} msg={:?}", bundle_id, finalized);
                            return Ok(());
                        }
                        Some(BundleResultType::Dropped(dropped)) => {
                            bail!("bundle {} dropped by Jito: {:?}", bundle_id, dropped);
                        }
                        None => {}
                    }
                }
                Ok(Some(Err(error))) => {
                    return Err(error).context("bundle result stream returned an error");
                }
                Ok(None) | Err(_) => break,
            }
        }

        bail!("timed out waiting for bundle result for {}", bundle_id)
    }

    async fn wait_for_signature(&self, signature: &Signature) -> Result<()> {
        for _ in 0..RPC_CONFIRM_RETRIES {
            let status = self
                .rpc
                .get_signature_status_with_commitment(signature, CommitmentConfig::confirmed())
                .await?;
            if matches!(status, Some(Ok(()))) {
                return Ok(());
            }
            sleep(RPC_CONFIRM_DELAY).await;
        }

        bail!(
            "transaction {} was not confirmed after bundle submission",
            signature
        )
    }
}

impl AtomicArbStrategy {
    fn from_env() -> Result<Option<Self>> {
        if env::var("FLASH_LOAN_BORROW_PROGRAM_ID").is_err()
            && env::var("FLASH_LOAN_SWAP_PROGRAM_ID").is_err()
            && env::var("FLASH_LOAN_REPAY_PROGRAM_ID").is_err()
        {
            return Ok(None);
        }

        let block_engine_url = env::var("JITO_BLOCK_ENGINE_URL").ok();
        let static_tip_account = env::var("JITO_TIP_ACCOUNT")
            .ok()
            .map(|value| Pubkey::from_str(&value))
            .transpose()
            .context("invalid JITO_TIP_ACCOUNT")?;

        Ok(Some(Self {
            block_engine_url,
            max_leader_slots: env_u64("JITO_MAX_LEADER_SLOTS")
                .unwrap_or(DEFAULT_MAX_JITO_LEADER_SLOTS),
            flash_loan_fee_bps: env_u64("FLASH_LOAN_FEE_BPS")
                .unwrap_or(DEFAULT_FLASH_LOAN_FEE_BPS),
            jito_tip_lamports: env_u64("JITO_TIP_LAMPORTS")
                .unwrap_or(DEFAULT_JITO_TIP_LAMPORTS),
            network_fee_lamports: env_u64("NETWORK_FEE_LAMPORTS")
                .unwrap_or(DEFAULT_NETWORK_FEE_LAMPORTS),
            min_observed_notional_lamports: env_u64("MIN_OBSERVED_NOTIONAL_LAMPORTS")
                .unwrap_or(DEFAULT_MIN_OBSERVED_NOTIONAL_LAMPORTS),
            expected_edge_bps: env_u64("EXPECTED_ARB_EDGE_BPS")
                .unwrap_or(DEFAULT_EXPECTED_EDGE_BPS),
            static_tip_account,
            borrow: StrategyInstructionTemplate::from_env("FLASH_LOAN_BORROW", "borrow")?,
            swap: StrategyInstructionTemplate::from_env("FLASH_LOAN_SWAP", "swap")?,
            repay: StrategyInstructionTemplate::from_env("FLASH_LOAN_REPAY", "repay")?,
        }))
    }
}

impl StrategyInstructionTemplate {
    fn from_env(prefix: &str, label: &'static str) -> Result<Self> {
        let program_id = parse_pubkey_env(&format!("{prefix}_PROGRAM_ID"))?;
        let accounts = parse_accounts_env(&format!("{prefix}_ACCOUNTS"))?;
        let discriminator = parse_hex_env(&format!("{prefix}_DISCRIMINATOR_HEX"))?;
        Ok(Self {
            label,
            program_id,
            accounts,
            discriminator,
        })
    }

    fn render_borrow(&self, amount: u64) -> Result<Instruction> {
        self.render(amount.to_le_bytes().to_vec())
    }

    fn render_swap(&self, amount_in: u64, min_amount_out: u64) -> Result<Instruction> {
        let mut data = Vec::with_capacity(16);
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_amount_out.to_le_bytes());
        self.render(data)
    }

    fn render_repay(&self, amount: u64, fee_lamports: u64) -> Result<Instruction> {
        let mut data = Vec::with_capacity(16);
        data.extend_from_slice(&amount.to_le_bytes());
        data.extend_from_slice(&fee_lamports.to_le_bytes());
        self.render(data)
    }

    fn render(&self, mut payload: Vec<u8>) -> Result<Instruction> {
        let mut data = self.discriminator.clone();
        data.append(&mut payload);
        if data.is_empty() {
            bail!("{} instruction template produced empty data", self.label);
        }
        Ok(Instruction {
            program_id: self.program_id,
            accounts: self.accounts.clone(),
            data,
        })
    }
}

fn env_u64(name: &str) -> Option<u64> {
    env::var(name).ok()?.parse().ok()
}

fn parse_pubkey_env(name: &str) -> Result<Pubkey> {
    let value =
        env::var(name).with_context(|| format!("missing required environment variable {name}"))?;
    Pubkey::from_str(&value).with_context(|| format!("invalid pubkey in {name}"))
}

fn parse_hex_env(name: &str) -> Result<Vec<u8>> {
    let value =
        env::var(name).with_context(|| format!("missing required environment variable {name}"))?;
    decode_hex(value.trim()).with_context(|| format!("invalid hex payload in {name}"))
}

fn parse_accounts_env(name: &str) -> Result<Vec<AccountMeta>> {
    let value =
        env::var(name).with_context(|| format!("missing required environment variable {name}"))?;
    value
        .split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let mut parts = entry.split('|').map(str::trim);
            let pubkey = parts
                .next()
                .ok_or_else(|| anyhow!("missing pubkey in account entry {entry}"))?;
            let writable = parse_bool(parts.next().unwrap_or("false"))?;
            let signer = parse_bool(parts.next().unwrap_or("false"))?;
            let pubkey = Pubkey::from_str(pubkey)
                .with_context(|| format!("invalid pubkey in {name}: {pubkey}"))?;
            Ok(if writable {
                AccountMeta::new(pubkey, signer)
            } else {
                AccountMeta::new_readonly(pubkey, signer)
            })
        })
        .collect()
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "t" | "1" | "yes" | "y" => Ok(true),
        "false" | "f" | "0" | "no" | "n" => Ok(false),
        _ => bail!("invalid boolean value {value}"),
    }
}

fn decode_hex(value: &str) -> Result<Vec<u8>> {
    let trimmed = value.trim_start_matches("0x");
    if !trimmed.len().is_multiple_of(2) {
        bail!("hex string must have an even length");
    }

    (0..trimmed.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&trimmed[index..index + 2], 16).map_err(anyhow::Error::from)
        })
        .collect()
}

fn ceil_div(value: u64, divisor: u64) -> u64 {
    if divisor == 0 {
        return 0;
    }
    value.saturating_add(divisor - 1) / divisor
}
