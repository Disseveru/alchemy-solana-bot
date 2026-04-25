use {
    anyhow::{Context, Result},
    dotenvy::dotenv,
    futures::StreamExt,
    log::{error, info, warn},
    solana_sdk::pubkey::Pubkey,
    std::{collections::HashMap, env, sync::Arc, time::Duration},
    tokio::time::sleep,
    yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient},
    yellowstone_grpc_proto::prelude::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions,
    },
};
mod arb;
mod executor;
mod jito;
use executor::{Executor, ExecutorConfig};

/// How long to wait before attempting a reconnect after a stream failure.
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// How often the background task refreshes the cached blockhash (~2 slots).
const BLOCKHASH_REFRESH_INTERVAL: Duration = Duration::from_millis(800);

/// Well-known DEX programs we want account updates from.
///
/// Restricting the `owner` filter means Geyser only sends us accounts owned by
/// these programs (pool state, vault accounts, reserve accounts) rather than
/// the entire account universe, which dramatically reduces stream bandwidth.
const WATCHED_OWNERS: &[&str] = &[
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", // Raydium AMM v4
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",  // Orca Whirlpool
    "So1endD9AkS2mttmbeS8S96ySZps9SML7RnZBjWdLLV",  // Solend
];

/// Well-known DEX programs we want transaction updates from.
///
/// Only transactions that touch one of these programs will be streamed.
const WATCHED_TX_PROGRAMS: &[&str] = &[
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", // Raydium AMM v4
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",  // Orca Whirlpool
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",  // Jupiter v6
];

/// Build a subscription request that listens for slot updates, account updates,
/// and all non-vote transactions.
fn build_subscribe_request() -> SubscribeRequest {
    let mut accounts: HashMap<String, SubscribeRequestFilterAccounts> = HashMap::new();
    // Filter to accounts owned by watched DEX programs to reduce stream bandwidth.
    accounts.insert(
        "dex_accounts".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: WATCHED_OWNERS.iter().map(|s| s.to_string()).collect(),
            filters: vec![],
            nonempty_txn_signature: None,
        },
    );

    let mut transactions: HashMap<String, SubscribeRequestFilterTransactions> = HashMap::new();
    // Subscribe to non-vote transactions that touch our target DEX programs.
    transactions.insert(
        "dex_txns".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: None,
            signature: None,
            account_include: WATCHED_TX_PROGRAMS.iter().map(|s| s.to_string()).collect(),
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    let mut slots: HashMap<String, SubscribeRequestFilterSlots> = HashMap::new();
    // Subscribe to slot updates so we can keep current_slot up to date.
    slots.insert(
        "slots".to_string(),
        SubscribeRequestFilterSlots { filter_by_commitment: None, interslot_updates: None },
    );

    SubscribeRequest {
        accounts,
        transactions,
        slots,
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    }
}

/// Connect to the Yellowstone gRPC endpoint and stream updates, dispatching to
/// the [`Executor`] for strategy evaluation.
///
/// Returns an error only if the initial connection fails; stream errors are
/// handled by logging and triggering a reconnect from the caller.
async fn run_stream(grpc_url: &str, x_token: &str, executor: Arc<Executor>) -> Result<()> {
    info!("Connecting to gRPC endpoint: {}", grpc_url);

    let tls_config = ClientTlsConfig::new().with_native_roots();

    let mut client = GeyserGrpcClient::build_from_shared(grpc_url.to_string())
        .context("Failed to build gRPC endpoint")?
        .x_token(Some(x_token))
        .context("Invalid X-Token value")?
        .tls_config(tls_config)
        .context("Failed to configure TLS")?
        .connect()
        .await
        .context("Failed to connect to gRPC endpoint")?;

    info!("Connected. Subscribing to slot, account, and transaction updates…");

    let request = build_subscribe_request();
    let mut stream = client
        .subscribe_once(request)
        .await
        .context("Failed to subscribe")?;

    info!("Subscription active. Waiting for updates…");

    while let Some(msg) = stream.next().await {
        match msg {
            Ok(update) => {
                if let Some(update_oneof) = update.update_oneof {
                    match update_oneof {
                        UpdateOneof::Slot(slot_update) => {
                            // Keep the cached slot counter up to date.
                            executor.update_slot(slot_update.slot);
                        }
                        UpdateOneof::Account(account_update) => {
                            if let Some(info) = &account_update.account {
                                info!(
                                    "[Account] pubkey={} lamports={} slot={}",
                                    bs58::encode(&info.pubkey).into_string(),
                                    info.lamports,
                                    account_update.slot
                                );
                                // ── Dispatch to Execution Logic ──────────
                                if let Err(e) = executor
                                    .on_account_update(info, account_update.slot)
                                    .await
                                {
                                    warn!("[Executor] account handler error: {:#}", e);
                                }
                            }
                        }
                        UpdateOneof::Transaction(tx_update) => {
                            if let Some(tx_info) = &tx_update.transaction {
                                info!(
                                    "[Transaction] signature={} slot={}",
                                    bs58::encode(&tx_info.signature).into_string(),
                                    tx_update.slot
                                );
                                // ── Dispatch to Execution Logic ──────────
                                if let Err(e) = executor
                                    .on_transaction(tx_info, tx_update.slot)
                                    .await
                                {
                                    warn!("[Executor] transaction handler error: {:#}", e);
                                }
                            }
                        }
                        UpdateOneof::Ping(_) => {
                            // Heartbeat – ignore silently.
                        }
                        other => {
                            info!("[Other] {:?}", other);
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Stream error: {:?}", e);
                return Err(e.into());
            }
        }
    }

    warn!("Stream ended unexpectedly.");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file if present (errors are ignored so the binary works with
    // real environment variables too).
    let _ = dotenv();

    // Initialise the logger. Use RUST_LOG=info by default.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let grpc_url = env::var("GRPC_URL").context("GRPC_URL environment variable not set")?;
    let x_token = env::var("X_TOKEN").context("X_TOKEN environment variable not set")?;

    info!("Alchemy Solana gRPC Bot starting…");

    // Build the executor once and share it across reconnect iterations.
    let executor = Arc::new(
        Executor::new(ExecutorConfig::default())
            .await
            .context("Failed to initialise Executor")?,
    );

    // ── Background blockhash refresh task ────────────────────────────────────
    // Keeps SharedState::latest_blockhash fresh so on_transaction never needs
    // to call the RPC endpoint in the hot-path.
    {
        let executor_ref = Arc::clone(&executor);
        tokio::spawn(async move {
            loop {
                if let Err(e) = executor_ref.refresh_blockhash().await {
                    warn!("[Blockhash] refresh failed: {:#}", e);
                }
                sleep(BLOCKHASH_REFRESH_INTERVAL).await;
            }
        });
    }

    // ── Backrun engine task ───────────────────────────────────────────────────
    // Subscribes to the Jito mempool and submits flash-loan arbitrage bundles
    // when a profitable opportunity is detected.
    //
    // Enable by setting ARB_ENABLED=true in .env.  Then fill in the ArbRoute
    // below with the correct mainnet addresses for your target pair.
    //
    // ## SOL–USDC example addresses (verify before use!)
    //
    // Solend main pool (see https://docs.solend.fi/protocol/addresses):
    //   solend_reserve:                  "8PbodeaosQP19SjYFx855UMqWxH2HynZLdBXmsrbac36"
    //   solend_reserve_liquidity_supply: "8UviNr47S8eL6J3WfDxMRa3hvLta1VDJwNWqsDgtN3Ud"
    //   solend_lending_market:           "4UpD2fh7xH3VP9QQaXtsS1YY3bxzWhtfpks7FatyKvdY"
    //
    // Raydium SOL–USDC pool v4 (see https://api.raydium.io/v2/ammV3/ammPools):
    //   raydium_amm_id:      "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWaS6E7gexGD6"
    //
    // Orca SOL–USDC Whirlpool (see https://api.mainnet.orca.so/v1/whirlpool/list):
    //   orca_whirlpool:      "HJPjoWUrhoZzkNfRpHuieeFk9WcZWjwy6PBjZ81ngndJ"
    //
    // Token accounts (our_*) must be pre-created SPL token accounts owned by
    // the bot wallet.  Create them with:
    //   spl-token create-account <MINT> --owner <WALLET_PUBKEY>
    //
    // ## Steps to go live
    //
    //   1.  Fund the bot wallet with ≥ 0.1 SOL for fees + tips.
    //   2.  Create the required token accounts (our_loan_token_account, etc.).
    //   3.  Verify all route addresses on-chain (e.g. with `solana account <PUBKEY>`).
    //   4.  Set ARB_ENABLED=true, fill in all ArbRoute fields below.
    //   5.  Set RUST_LOG=debug for verbose output during testing.
    //   6.  Run with `cargo run --release` for optimised performance.
    //
    // > ⚠️  WARNING: Flash-loan arbitrage carries real financial risk.
    // >    Failed bundles still pay Jito tips.  Priority fees are burned even
    // >    when the transaction reverts.  Competition from other MEV bots means
    // >    profit is never guaranteed.  Start with small loan amounts.
    if env::var("ARB_ENABLED").as_deref() == Ok("true") {
        // Validate that the wallet is funded (non-ephemeral) before starting.
        // The executor will have logged a warning if WALLET_PRIVATE_KEY is unset.

        // Fetch Jito tip accounts from the block engine.
        let tip_accounts: Vec<Pubkey> = match executor.get_tip_accounts().await {
            Ok(accounts) => {
                info!("[BackrunEngine] fetched {} Jito tip accounts", accounts.len());
                accounts
                    .iter()
                    .filter_map(|s| s.parse().ok())
                    .collect()
            }
            Err(e) => {
                warn!(
                    "[BackrunEngine] could not fetch tip accounts (Jito endpoint may be \
                     unreachable): {:#}. Using hardcoded fallback.",
                    e
                );
                // Jito tip accounts are publicly known and stable.
                // Source: https://jito-labs.gitbook.io/mev/searcher-resources/tip-accounts
                vec![
                    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5".parse()?,
                    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe".parse()?,
                    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY".parse()?,
                    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt13ij6F8k".parse()?,
                ]
            }
        };

        // ── Build the ArbRoute ────────────────────────────────────────────────
        // Replace every `Pubkey::default()` below with the verified mainnet address.
        // Verify each address with `solana account <PUBKEY>` before going live.
        //
        // IMPORTANT: The bot will NOT start if any required field is still
        // Pubkey::default() — validate_route() enforces this at runtime.
        // Fields marked "MUST be replaced" require custom SPL token account
        // creation before the engine can run.
        let route = Arc::new(arb::ArbRoute {
            // ── Solend SOL reserve (main pool) ────────────────────────────────
            // Verify: https://docs.solend.fi/protocol/addresses
            solend_reserve: "8PbodeaosQP19SjYFx855UMqWxH2HynZLdBXmsrbac36".parse()
                .context("invalid solend_reserve")?,
            solend_reserve_liquidity_supply: "8UviNr47S8eL6J3WfDxMRa3hvLta1VDJwNWqsDgtN3Ud"
                .parse()
                .context("invalid solend_reserve_liquidity_supply")?,
            // Verify at https://docs.solend.fi/protocol/addresses — exact account may differ.
            solend_fee_receiver: "5bFegCNDLR5QfSTnFSHN42M5MejkgYDMBzE9agFi5DCC".parse()
                .context("invalid solend_fee_receiver")?,
            solend_lending_market: "4UpD2fh7xH3VP9QQaXtsS1YY3bxzWhtfpks7FatyKvdY".parse()
                .context("invalid solend_lending_market")?,
            // Lending market authority PDA:
            //   seeds = [lending_market.as_ref()], program = SOLEND_PROGRAM
            // Verify by running: `solana account DdZR6zRFiUt4S5mg7AV1uKB2z1f1WzcNYCaTEEWPAuby`
            solend_lending_market_authority: "DdZR6zRFiUt4S5mg7AV1uKB2z1f1WzcNYCaTEEWPAuby"
                .parse()
                .context("invalid solend_lending_market_authority")?,
            // MUST be replaced: SPL token account for the flash-borrowed asset (wSOL).
            // Create: `spl-token create-account So11111111111111111111111111111111111111112`
            our_loan_token_account: Pubkey::default(),

            // ── Raydium SOL–USDC AMM v4 pool ──────────────────────────────────
            // Fetch pool accounts from: https://api.raydium.io/v2/sdk/liquidity/mainnet.json
            raydium_amm_id: "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWaS6E7gexGD6".parse()
                .context("invalid raydium_amm_id")?,
            // Authority PDA: seeds = [b"amm authority"], program = RAYDIUM_AMM_V4
            raydium_amm_authority: "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1".parse()
                .context("invalid raydium_amm_authority")?,
            // Fetch open_orders, target_orders, vaults from pool state account:
            //   `solana account 58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWaS6E7gexGD6 --output json`
            raydium_amm_open_orders: Pubkey::default(),   // TODO: from pool state
            raydium_amm_target_orders: Pubkey::default(), // TODO: from pool state
            raydium_pool_coin_vault: Pubkey::default(),   // TODO: SOL vault from pool state
            raydium_pool_pc_vault: Pubkey::default(),     // TODO: USDC vault from pool state
            // OpenBook (Serum v3) program — mainnet, stable address
            raydium_serum_program: "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX".parse()
                .context("invalid raydium_serum_program")?,
            // Fetch Serum accounts from pool state (marketId field and derive from market):
            raydium_serum_market: Pubkey::default(),      // TODO: from pool state
            raydium_serum_bids: Pubkey::default(),        // TODO: from OpenBook market state
            raydium_serum_asks: Pubkey::default(),        // TODO: from OpenBook market state
            raydium_serum_event_queue: Pubkey::default(), // TODO: from OpenBook market state
            raydium_serum_coin_vault: Pubkey::default(),  // TODO: from OpenBook market state
            raydium_serum_pc_vault: Pubkey::default(),    // TODO: from OpenBook market state
            raydium_serum_vault_signer: Pubkey::default(), // TODO: from OpenBook market state
            // MUST be replaced: SPL token accounts for the Raydium swap leg.
            our_raydium_source: Pubkey::default(), // wSOL account owned by this wallet
            our_raydium_dest: Pubkey::default(),   // USDC account owned by this wallet

            // ── Orca Whirlpool SOL–USDC ────────────────────────────────────────
            // Fetch pool accounts from: https://api.mainnet.orca.so/v1/whirlpool/list
            orca_whirlpool: "HJPjoWUrhoZzkNfRpHuieeFk9WcZWjwy6PBjZ81ngndJ".parse()
                .context("invalid orca_whirlpool")?,
            // Fetch vault addresses from whirlpool state:
            //   `solana account HJPjoWUrhoZzkNfRpHuieeFk9WcZWjwy6PBjZ81ngndJ --output json`
            orca_token_vault_a: Pubkey::default(), // TODO: tokenVaultA from pool state
            orca_token_vault_b: Pubkey::default(), // TODO: tokenVaultB from pool state
            // Derive tick arrays using Orca Whirlpools SDK:
            //   npx ts-node -e "const {getTickArrays} = require('@orca-so/whirlpools-sdk'); ..."
            // See: https://orca-so.gitbook.io/orca-developer-portal
            orca_tick_array_0: Pubkey::default(), // TODO: tick array for current range
            orca_tick_array_1: Pubkey::default(), // TODO: next tick array
            orca_tick_array_2: Pubkey::default(), // TODO: tick array after that
            // Oracle PDA: seeds = [b"oracle", whirlpool.as_ref()], program = ORCA_WHIRLPOOL_PROGRAM
            orca_oracle: Pubkey::default(), // TODO: derive PDA
            // MUST be replaced: SPL token accounts for the Orca swap leg.
            our_orca_token_a: Pubkey::default(), // SOL/wSOL account owned by this wallet
            our_orca_token_b: Pubkey::default(), // USDC account owned by this wallet

            tip_accounts,
        });

        // Validate the route before starting the engine.
        // If any field is still Pubkey::default() the engine will NOT start —
        // this prevents accidental deployment with unconfigured placeholder addresses.
        if let Err(e) = arb::validate_route(&route) {
            warn!(
                "[BackrunEngine] route validation failed — engine NOT started.\n\
                 Fill in the Pubkey::default() placeholders in main.rs, then restart:\n{:#}",
                e
            );
        } else {
            let wallet = executor.wallet();
            let engine = Arc::new(arb::BackrunEngine::new(
                Arc::clone(&executor),
                wallet,
                route,
            ));
            tokio::spawn(async move {
                loop {
                    if let Err(e) = engine.run().await {
                        warn!("[BackrunEngine] restarting after error: {:#}", e);
                    }
                    sleep(Duration::from_secs(5)).await;
                }
            });
            info!("[BackrunEngine] engine started");
        }
    } else {
        info!(
            "[BackrunEngine] disabled (set ARB_ENABLED=true in .env to enable). \
             Fill in the ArbRoute in main.rs first."
        );
    }

    // Outer reconnect loop – keeps the bot running even if the stream drops.
    loop {
        match run_stream(&grpc_url, &x_token, Arc::clone(&executor)).await {
            Ok(()) => {
                warn!("Stream closed. Reconnecting in {:?}…", RECONNECT_DELAY);
            }
            Err(e) => {
                error!("Stream error: {:#}. Reconnecting in {:?}…", e, RECONNECT_DELAY);
            }
        }
        sleep(RECONNECT_DELAY).await;
    }
}
