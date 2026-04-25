use {
    anyhow::{Context, Result},
    solana_sdk::transaction::VersionedTransaction,
    tonic::transport::{Channel, ClientTlsConfig, Endpoint},
};

#[allow(dead_code)]
pub mod bundle {
    tonic::include_proto!("bundle");
}

#[allow(dead_code)]
pub mod packet {
    tonic::include_proto!("packet");
}

pub mod searcher {
    tonic::include_proto!("searcher");
}

#[allow(dead_code)]
pub mod shared {
    tonic::include_proto!("shared");
}

pub async fn get_searcher_client_no_auth(
    block_engine_url: &str,
) -> Result<searcher::searcher_service_client::SearcherServiceClient<Channel>> {
    let searcher_channel = create_grpc_channel(block_engine_url).await?;
    Ok(searcher::searcher_service_client::SearcherServiceClient::new(searcher_channel))
}

async fn create_grpc_channel(url: &str) -> Result<Channel> {
    let mut endpoint = Endpoint::from_shared(url.to_string()).context("invalid Jito block engine URL")?;
    if url.starts_with("https") {
        endpoint = endpoint
            .tls_config(ClientTlsConfig::new())
            .context("failed to configure Jito block engine TLS")?;
    }
    endpoint
        .connect()
        .await
        .context("failed to connect to Jito block engine")
}

pub fn proto_packet_from_versioned_tx(tx: &VersionedTransaction) -> Result<packet::Packet> {
    let data = bincode::serialize(tx).context("failed to serialize versioned transaction for Jito")?;
    Ok(packet::Packet {
        data: data.clone(),
        meta: Some(packet::Meta {
            size: data.len() as u64,
            addr: String::new(),
            port: 0,
            flags: None,
            sender_stake: 0,
        }),
    })
}
