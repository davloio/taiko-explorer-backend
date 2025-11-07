use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    extract::Extension,
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use tower_http::cors::CorsLayer;

use crate::db::operations::BlockRepository;
use crate::db::address_analytics::AddressAnalyticsRepository;
use crate::graphql::schema::{create_schema, TaikoSchema};

pub fn create_router(block_repo: BlockRepository, analytics_repo: AddressAnalyticsRepository) -> Router {
    let schema = create_schema(block_repo.clone(), analytics_repo.clone());

    Router::new()
        .route("/", get(graphql_playground))
        .route("/graphql", post(graphql_handler))
        .route("/health", get(health_check))
        .route("/api/stats", get(api_stats))
        .route("/api/block/{number}", get(api_block))
        .route("/api/transaction/{hash}", get(api_transaction))
        .route("/api/transactions", get(api_transactions))
        .route("/api/address/{address}/transactions", get(api_address_transactions))
        .layer(Extension(schema))
        .layer(Extension(block_repo))
        .layer(CorsLayer::permissive())
}

async fn graphql_handler(
    schema: Extension<TaikoSchema>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

async fn graphql_playground() -> impl IntoResponse {
    Html(playground_source(GraphQLPlaygroundConfig::new("/graphql")))
}

async fn health_check() -> impl IntoResponse {
    Json(json!({
        "status": "ok",
        "service": "taiko-explorer-backend",
        "timestamp": chrono::Utc::now().timestamp()
    }))
}

async fn api_stats(
    Extension(block_repo): Extension<BlockRepository>,
) -> Result<impl IntoResponse, StatusCode> {
    let total_blocks = block_repo.get_block_count().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let latest_block = block_repo.get_latest_block_number().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(json!({
        "total_blocks": total_blocks,
        "latest_block": latest_block.unwrap_or(0),
        "network": "taiko",
        "chain_id": 167000
    })))
}

async fn api_block(
    axum::extract::Path(number): axum::extract::Path<i64>,
    Extension(block_repo): Extension<BlockRepository>,
) -> Result<impl IntoResponse, StatusCode> {
    match block_repo.get_block_by_number(number) {
        Ok(Some(block)) => Ok(Json(json!({
            "number": block.number,
            "hash": block.hash,
            "parent_hash": block.parent_hash,
            "timestamp": block.timestamp,
            "gas_limit": block.gas_limit,
            "gas_used": block.gas_used,
            "miner": block.miner,
            "transaction_count": block.transaction_count,
            "size": block.size
        }))),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn api_transaction(
    axum::extract::Path(hash): axum::extract::Path<String>,
    Extension(block_repo): Extension<BlockRepository>,
) -> Result<impl IntoResponse, StatusCode> {
    match block_repo.get_transaction_by_hash(&hash) {
        Ok(Some(tx)) => Ok(Json(json!({
            "hash": tx.hash,
            "block_number": tx.block_number,
            "block_hash": tx.block_hash,
            "transaction_index": tx.transaction_index,
            "from": tx.from_address,
            "to": tx.to_address,
            "value": tx.value.to_string(),
            "gas_limit": tx.gas_limit,
            "gas_used": tx.gas_used,
            "gas_price": tx.gas_price.map(|gp| gp.to_string()),
            "nonce": tx.nonce,
            "status": tx.status,
            "contract_address": tx.contract_address,
            "logs_count": tx.logs_count
        }))),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn api_transactions(
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    Extension(block_repo): Extension<BlockRepository>,
) -> Result<impl IntoResponse, StatusCode> {
    let limit = params.get("limit")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(20)
        .min(100);
    
    let offset = params.get("offset")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
        
    let order_desc = params.get("order")
        .map(|s| s == "desc")
        .unwrap_or(true);

    match block_repo.get_transactions_paginated(limit, offset, order_desc) {
        Ok(transactions) => {
            let total_count = block_repo.get_transaction_count().unwrap_or(0);
            
            Ok(Json(json!({
                "transactions": transactions.into_iter().map(|tx| json!({
                    "hash": tx.hash,
                    "block_number": tx.block_number,
                    "from": tx.from_address,
                    "to": tx.to_address,
                    "value": tx.value.to_string(),
                    "gas_used": tx.gas_used,
                    "status": tx.status
                })).collect::<Vec<_>>(),
                "total_count": total_count,
                "limit": limit,
                "offset": offset
            })))
        },
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn api_address_transactions(
    axum::extract::Path(address): axum::extract::Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    Extension(block_repo): Extension<BlockRepository>,
) -> Result<impl IntoResponse, StatusCode> {
    let limit = params.get("limit")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(20)
        .min(100);
    
    let offset = params.get("offset")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);

    match block_repo.get_transactions_by_address(&address, limit, offset) {
        Ok(transactions) => {
            let total_count = block_repo.get_transactions_count_by_address(&address).unwrap_or(0);
            
            Ok(Json(json!({
                "address": address,
                "transactions": transactions.into_iter().map(|tx| json!({
                    "hash": tx.hash,
                    "block_number": tx.block_number,
                    "from": tx.from_address,
                    "to": tx.to_address,
                    "value": tx.value.to_string(),
                    "gas_used": tx.gas_used,
                    "status": tx.status,
                    "is_incoming": tx.to_address.as_ref() == Some(&address)
                })).collect::<Vec<_>>(),
                "total_count": total_count,
                "limit": limit,
                "offset": offset
            })))
        },
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}