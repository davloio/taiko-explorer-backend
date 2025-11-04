mod config;
mod db;
mod indexer;
mod models;
mod rpc;
mod schema;

use anyhow::Result;
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use crate::config::Config;
use crate::db::connection::establish_connection;
use crate::db::operations::BlockRepository;
use crate::indexer::BlockIndexer;
use crate::rpc::client::TaikoRpcClient;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!();

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env()?;
    
    init_logging(&config.log_level)?;
    
    info!("Starting Taiko Explorer Backend");
    info!("RPC URL: {}", config.taiko_rpc_url);
    info!("Chain ID: {}", config.taiko_chain_id);
    
    let pool = establish_connection();
    
    run_migrations(&pool)?;
    
    let rpc_client = TaikoRpcClient::new(&config.taiko_rpc_url, config.taiko_chain_id).await?;
    let block_repo = BlockRepository::new(pool);
    
    let indexer = BlockIndexer::new(rpc_client, block_repo, config.batch_size);
    
    let status = indexer.get_indexer_status().await?;
    info!("Indexer status: {:?}", status);
    
    indexer.start_indexing().await?;
    
    Ok(())
}

fn init_logging(log_level: &str) -> Result<()> {
    let level = match log_level.to_lowercase().as_str() {
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_target(false)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;
    
    Ok(())
}

fn run_migrations(pool: &crate::db::connection::DbPool) -> Result<()> {
    info!("Running database migrations...");
    
    let mut conn = pool.get()?;
    conn.run_pending_migrations(MIGRATIONS)
        .map_err(|e| anyhow::anyhow!("Migration error: {}", e))?;
    
    info!("Migrations completed successfully");
    Ok(())
}
