use anyhow::{anyhow, Result};
use std::time::Duration;
use tokio::time::{interval, sleep};
use tracing::{error, info, warn};

use crate::db::operations::BlockRepository;
use crate::rpc::client::TaikoRpcClient;
use crate::rpc::types::block_to_new_block;

pub struct BlockIndexer {
    rpc_client: TaikoRpcClient,
    block_repo: BlockRepository,
    batch_size: u64,
}

impl BlockIndexer {
    pub fn new(
        rpc_client: TaikoRpcClient,
        block_repo: BlockRepository,
        batch_size: u64,
    ) -> Self {
        Self {
            rpc_client,
            block_repo,
            batch_size,
        }
    }

    pub async fn start_indexing(&self) -> Result<()> {
        info!("Starting block indexer...");
        
        self.catch_up_historical_blocks().await?;
        
        info!("Historical sync complete, starting real-time indexing...");
        self.start_real_time_indexing().await
    }

    async fn catch_up_historical_blocks(&self) -> Result<()> {
        let latest_rpc_block = self.rpc_client.get_latest_block_number().await?;
        let latest_db_block = self.block_repo.get_latest_block_number()?;
        
        let start_block = match latest_db_block {
            Some(block_num) => (block_num + 1) as u64,
            None => 1, // Start from block 1 if database is empty
        };

        if start_block > latest_rpc_block {
            info!("Database is up to date");
            return Ok(());
        }

        info!(
            "Syncing blocks from {} to {}",
            start_block, latest_rpc_block
        );

        for block_num in start_block..=latest_rpc_block {
            match self.index_single_block(block_num).await {
                Ok(_) => {
                    if block_num % 100 == 0 {
                        info!("Synced block #{}", block_num);
                    }
                }
                Err(e) => {
                    error!("Failed to index block #{}: {}", block_num, e);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }

        Ok(())
    }

    async fn start_real_time_indexing(&self) -> Result<()> {
        let mut interval = interval(Duration::from_secs(12)); // Taiko block time

        loop {
            interval.tick().await;
            
            match self.index_latest_blocks().await {
                Ok(_) => {}
                Err(e) => {
                    error!("Real-time indexing error: {}", e);
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn index_latest_blocks(&self) -> Result<()> {
        let latest_rpc_block = self.rpc_client.get_latest_block_number().await?;
        let latest_db_block = self.block_repo.get_latest_block_number()?;
        
        let start_block = match latest_db_block {
            Some(block_num) => (block_num + 1) as u64,
            None => latest_rpc_block,
        };

        for block_num in start_block..=latest_rpc_block {
            if !self.block_repo.block_exists(block_num as i64)? {
                self.index_single_block(block_num).await?;
                info!("Indexed new block #{}", block_num);
            }
        }

        Ok(())
    }

    async fn index_single_block(&self, block_number: u64) -> Result<()> {
        let block = self
            .rpc_client
            .get_block_by_number(block_number)
            .await?
            .ok_or_else(|| anyhow!("Block {} not found", block_number))?;

        let new_block = block_to_new_block(&block);
        
        match self.block_repo.insert_block(new_block) {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.to_string().contains("duplicate key") {
                    warn!("Block #{} already exists, skipping", block_number);
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    pub async fn get_indexer_status(&self) -> Result<IndexerStatus> {
        let latest_rpc_block = self.rpc_client.get_latest_block_number().await?;
        let latest_db_block = self.block_repo.get_latest_block_number()?;
        let total_blocks = self.block_repo.get_block_count()?;

        Ok(IndexerStatus {
            latest_rpc_block,
            latest_db_block,
            total_blocks,
            is_synced: latest_db_block.map_or(false, |db| db as u64 >= latest_rpc_block),
        })
    }
}

#[derive(Debug)]
pub struct IndexerStatus {
    pub latest_rpc_block: u64,
    pub latest_db_block: Option<i64>,
    pub total_blocks: i64,
    pub is_synced: bool,
}