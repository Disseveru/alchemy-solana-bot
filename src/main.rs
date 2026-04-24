use {
    anyhow::{Context, Result},
    dotenvy::dotenv,
    futures::StreamExt,
    log::{error, info, warn},
    std::{collections::HashMap, env, sync::Arc, time::Duration},
    tokio::time::sleep,
    yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient},
    yellowstone_grpc_proto::prelude::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterTransactions,
    },
};

mod executor;
use executor::{Executor, ExecutorConfig};

/// How long to wait before attempting a reconnect after a stream failure.
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// Build a subscription request that listens for all account updates and
/// all non-vote transactions.
fn build_subscribe_request() -> SubscribeRequest {
    let mut accounts: HashMap<String, SubscribeRequestFilterAccounts> = HashMap::new();
    // An empty filter matches every account update.
    accounts.insert("all_accounts".to_string(), SubscribeRequestFilterAccounts {
        account: vec![],
        owner: vec![],
        filters: vec![],
        nonempty_txn_signature: None,
    });

    let mut transactions: HashMap<String, SubscribeRequestFilterTransactions> = HashMap::new();
    // Subscribe to non-vote transactions only to reduce noise.
    transactions.insert(
        "all_txns".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: None,
            signature: None,
            account_include: vec![],
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    SubscribeRequest {
        accounts,
        transactions,
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    }
}

/// Connect to the Yellowstone gRPC endpoint and stream updates, printing each
/// one to stdout and dispatching to the [`Executor`] for strategy evaluation.
/// Returns an error only if the connection itself fails; stream errors are
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

    info!("Connected. Subscribing to account and transaction updates…");

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
                        UpdateOneof::Account(account_update) => {
                            if let Some(info) = &account_update.account {
                                info!(
                                    "[Account] pubkey={} lamports={} slot={}",
                                    bs58_encode(&info.pubkey),
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
                                    bs58_encode(&tx_info.signature),
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

/// Encode a raw byte slice as a base-58 string (Solana's standard encoding).
fn bs58_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    if bytes.is_empty() {
        return String::new();
    }
    // Count leading zeros.
    let leading_zeros = bytes.iter().take_while(|&&b| b == 0).count();
    // Convert bytes to a big-integer via base-256.
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
    let executor = Arc::new(Executor::new(ExecutorConfig::default()));

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
