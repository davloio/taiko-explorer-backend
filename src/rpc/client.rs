use anyhow::Result;
use ethers::prelude::*;
use std::sync::Arc;
use tracing::{info, warn};

#[derive(Clone)]
pub struct TaikoRpcClient {
    provider: Arc<Provider<Http>>,
    chain_id: u64,
}

impl TaikoRpcClient {
    pub async fn new(rpc_url: &str, chain_id: u64) -> Result<Self> {
        let provider = Provider::<Http>::try_from(rpc_url)?;
        let provider = Arc::new(provider);
        
        let client = Self {
            provider,
            chain_id,
        };
        
        client.validate_connection().await?;
        Ok(client)
    }

    async fn validate_connection(&self) -> Result<()> {
        let network_chain_id = self.provider.get_chainid().await?;
        
        if network_chain_id.as_u64() != self.chain_id {
            warn!(
                "Chain ID mismatch: expected {}, got {}",
                self.chain_id,
                network_chain_id.as_u64()
            );
        }
        
        info!("Connected to Taiko network, chain ID: {}", network_chain_id);
        Ok(())
    }

    pub async fn get_latest_block_number(&self) -> Result<u64> {
        let block_number = self.provider.get_block_number().await?;
        Ok(block_number.as_u64())
    }

    pub async fn get_block_by_number(&self, block_number: u64) -> Result<Option<Block<Transaction>>> {
        let block = self
            .provider
            .get_block_with_txs(BlockNumber::Number(block_number.into()))
            .await?;
        Ok(block)
    }

    pub async fn get_block_by_hash(&self, block_hash: H256) -> Result<Option<Block<Transaction>>> {
        let block = self.provider.get_block_with_txs(block_hash).await?;
        Ok(block)
    }

    pub async fn get_transaction_receipt(&self, tx_hash: H256) -> Result<Option<TransactionReceipt>> {
        let receipt = self.provider.get_transaction_receipt(tx_hash).await?;
        Ok(receipt)
    }

    pub async fn get_transaction_receipts_batch(&self, tx_hashes: Vec<H256>) -> Result<Vec<Option<TransactionReceipt>>> {
        let mut receipts = Vec::new();
        let mut consecutive_errors = 0;
        
        // Fetch receipts sequentially to avoid rate limiting
        for (i, tx_hash) in tx_hashes.iter().enumerate() {
            match self.get_transaction_receipt(*tx_hash).await {
                Ok(receipt) => {
                    receipts.push(receipt);
                    consecutive_errors = 0; // Reset error counter on success
                },
                Err(e) => {
                    let error_msg = e.to_string();
                    
                    // Check if it's a rate limit error
                    if error_msg.contains("Too many requests") || error_msg.contains("rate limit") {
                        consecutive_errors += 1;
                        warn!("Rate limited on tx {} ({}/{}). Waiting longer...", tx_hash, i+1, tx_hashes.len());
                        
                        // If we hit rate limits multiple times, just skip receipts for this block
                        if consecutive_errors > 3 {
                            warn!("Too many rate limit errors. Skipping remaining receipts for this block.");
                            // Fill remaining with None
                            for _ in i..tx_hashes.len() {
                                receipts.push(None);
                            }
                            break;
                        }
                        
                        // Exponential backoff
                        let wait_time = std::time::Duration::from_secs(2u64.pow(consecutive_errors));
                        tokio::time::sleep(wait_time).await;
                        receipts.push(None);
                    } else {
                        warn!("Failed to fetch receipt for tx {}: {}", tx_hash, e);
                        receipts.push(None);
                    }
                }
            }
            
            // Adaptive delay based on error state
            let delay_ms = if consecutive_errors > 0 {
                100 * (consecutive_errors + 1) as u64  // Longer delays after errors
            } else {
                50  // Normal delay between successful requests
            };
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
        
        Ok(receipts)
    }
}