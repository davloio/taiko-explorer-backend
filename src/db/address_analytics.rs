use anyhow::Result;
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, Utc};
use diesel::prelude::*;
use std::collections::HashMap;

use crate::db::connection::DbPool;
use crate::models::address_analytics::{AddressBalance, AddressLabel, AddressStats, NewAddressBalance, NewAddressLabel, NewAddressStats};
use crate::models::bridge::{BridgeStats, BridgeTransaction, NewBridgeStats, NewBridgeTransaction};
use crate::schema::{address_balances, address_labels, address_stats, bridge_stats, bridge_transactions, transactions};

#[derive(Clone)]
pub struct AddressAnalyticsRepository {
    pool: DbPool,
}

impl AddressAnalyticsRepository {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    // Address Labels
    pub fn insert_address_label(&self, new_label: NewAddressLabel) -> Result<AddressLabel> {
        let mut conn = self.pool.get()?;
        
        let label = diesel::insert_into(address_labels::table)
            .values(&new_label)
            .on_conflict(address_labels::address)
            .do_update()
            .set((
                address_labels::label.eq(&new_label.label),
                address_labels::category.eq(&new_label.category),
                address_labels::description.eq(&new_label.description),
                address_labels::verified.eq(&new_label.verified),
                address_labels::updated_at.eq(Utc::now()),
            ))
            .get_result(&mut conn)?;
        
        Ok(label)
    }

    pub fn get_address_labels(&self, category: Option<String>) -> Result<Vec<AddressLabel>> {
        let mut conn = self.pool.get()?;
        
        let mut query = address_labels::table.into_boxed();
        
        if let Some(cat) = category {
            query = query.filter(address_labels::category.eq(cat));
        }
        
        let labels = query.load::<AddressLabel>(&mut conn)?;
        Ok(labels)
    }

    pub fn get_address_label(&self, address: &str) -> Result<Option<AddressLabel>> {
        let mut conn = self.pool.get()?;
        
        let label = address_labels::table
            .filter(address_labels::address.eq(address))
            .first::<AddressLabel>(&mut conn)
            .optional()?;
        
        Ok(label)
    }

    // Address Stats
    pub fn upsert_address_stats(&self, new_stats: NewAddressStats) -> Result<AddressStats> {
        let mut conn = self.pool.get()?;
        
        let stats = diesel::insert_into(address_stats::table)
            .values(&new_stats)
            .on_conflict(address_stats::address)
            .do_update()
            .set((
                address_stats::last_seen_block.eq(&new_stats.last_seen_block),
                address_stats::total_transactions.eq(&new_stats.total_transactions),
                address_stats::total_sent_transactions.eq(&new_stats.total_sent_transactions),
                address_stats::total_received_transactions.eq(&new_stats.total_received_transactions),
                address_stats::total_volume_sent.eq(&new_stats.total_volume_sent),
                address_stats::total_volume_received.eq(&new_stats.total_volume_received),
                address_stats::gas_used.eq(&new_stats.gas_used),
                address_stats::gas_fees_paid.eq(&new_stats.gas_fees_paid),
                address_stats::unique_counterparties.eq(&new_stats.unique_counterparties),
                address_stats::contract_deployments.eq(&new_stats.contract_deployments),
                address_stats::updated_at.eq(Utc::now()),
            ))
            .get_result(&mut conn)?;
        
        Ok(stats)
    }

    pub fn get_address_stats(&self, address: &str) -> Result<Option<AddressStats>> {
        let mut conn = self.pool.get()?;
        
        let stats = address_stats::table
            .filter(address_stats::address.eq(address))
            .first::<AddressStats>(&mut conn)
            .optional()?;
        
        Ok(stats)
    }

    pub fn get_top_addresses_by_volume(&self, limit: i64) -> Result<Vec<AddressStats>> {
        let mut conn = self.pool.get()?;
        
        let stats = address_stats::table
            .order(address_stats::total_volume_sent.desc())
            .limit(limit)
            .load::<AddressStats>(&mut conn)?;
        
        Ok(stats)
    }

    pub fn get_top_addresses_by_transactions(&self, limit: i64) -> Result<Vec<AddressStats>> {
        let mut conn = self.pool.get()?;
        
        let stats = address_stats::table
            .order(address_stats::total_transactions.desc())
            .limit(limit)
            .load::<AddressStats>(&mut conn)?;
        
        Ok(stats)
    }

    // Address Balances
    pub fn insert_address_balance(&self, new_balance: NewAddressBalance) -> Result<AddressBalance> {
        let mut conn = self.pool.get()?;
        
        let balance = diesel::insert_into(address_balances::table)
            .values(&new_balance)
            .get_result(&mut conn)?;
        
        Ok(balance)
    }

    pub fn get_address_balance_history(&self, address: &str, limit: Option<i64>) -> Result<Vec<AddressBalance>> {
        let mut conn = self.pool.get()?;
        
        let mut query = address_balances::table
            .filter(address_balances::address.eq(address))
            .order(address_balances::timestamp.desc())
            .into_boxed();
        
        if let Some(l) = limit {
            query = query.limit(l);
        }
        
        let balances = query.load::<AddressBalance>(&mut conn)?;
        Ok(balances)
    }

    // Bridge Transactions
    pub fn insert_bridge_transaction(&self, new_bridge_tx: NewBridgeTransaction) -> Result<BridgeTransaction> {
        let mut conn = self.pool.get()?;
        
        let bridge_tx = diesel::insert_into(bridge_transactions::table)
            .values(&new_bridge_tx)
            .on_conflict(bridge_transactions::transaction_hash)
            .do_update()
            .set((
                bridge_transactions::status.eq(&new_bridge_tx.status),
                bridge_transactions::proof_submitted.eq(&new_bridge_tx.proof_submitted),
                bridge_transactions::finalized.eq(&new_bridge_tx.finalized),
                bridge_transactions::updated_at.eq(Utc::now()),
            ))
            .get_result(&mut conn)?;
        
        Ok(bridge_tx)
    }

    pub fn get_bridge_transactions(&self, limit: i64, offset: i64) -> Result<Vec<BridgeTransaction>> {
        let mut conn = self.pool.get()?;
        
        let bridge_txs = bridge_transactions::table
            .order(bridge_transactions::timestamp.desc())
            .limit(limit)
            .offset(offset)
            .load::<BridgeTransaction>(&mut conn)?;
        
        Ok(bridge_txs)
    }

    pub fn get_bridge_transactions_by_address(&self, address: &str, limit: i64) -> Result<Vec<BridgeTransaction>> {
        let mut conn = self.pool.get()?;
        
        let bridge_txs = bridge_transactions::table
            .filter(
                bridge_transactions::from_address.eq(address)
                .or(bridge_transactions::to_address.eq(address))
            )
            .order(bridge_transactions::timestamp.desc())
            .limit(limit)
            .load::<BridgeTransaction>(&mut conn)?;
        
        Ok(bridge_txs)
    }

    pub fn get_bridge_volume_by_date(&self, days: i32) -> Result<Vec<BridgeStats>> {
        let mut conn = self.pool.get()?;
        
        let start_date = Utc::now().date_naive() - chrono::Duration::days(days as i64);
        
        let stats = bridge_stats::table
            .filter(bridge_stats::date.ge(start_date))
            .order(bridge_stats::date.desc())
            .load::<BridgeStats>(&mut conn)?;
        
        Ok(stats)
    }

    // Analytics computation functions
    pub fn compute_address_stats_from_transactions(&self, address: &str) -> Result<Option<NewAddressStats>> {
        let mut conn = self.pool.get()?;
        
        // Get all transactions for this address
        let address_txs = transactions::table
            .filter(
                transactions::from_address.eq(address)
                .or(transactions::to_address.eq(address))
            )
            .load::<crate::models::transaction::Transaction>(&mut conn)?;
        
        if address_txs.is_empty() {
            return Ok(None);
        }
        
        let mut sent_count = 0i64;
        let mut received_count = 0i64;
        let mut total_volume_sent = BigDecimal::from(0);
        let mut total_volume_received = BigDecimal::from(0);
        let mut total_gas_used = 0i64;
        let mut total_gas_fees = BigDecimal::from(0);
        let mut counterparties = std::collections::HashSet::new();
        let mut contract_deployments = 0i32;
        
        let first_seen_block = address_txs.iter().map(|tx| tx.block_number).min().unwrap_or(0);
        let last_seen_block = address_txs.iter().map(|tx| tx.block_number).max().unwrap_or(0);
        
        for tx in &address_txs {
            if tx.from_address == address {
                sent_count += 1;
                total_volume_sent += &tx.value;
                
                if let (Some(gas_used), Some(gas_price)) = (tx.gas_used, &tx.effective_gas_price) {
                    total_gas_used += gas_used;
                    total_gas_fees += gas_price * BigDecimal::from(gas_used);
                }
                
                if let Some(to_addr) = &tx.to_address {
                    counterparties.insert(to_addr.clone());
                } else {
                    // Contract deployment
                    contract_deployments += 1;
                }
            }
            
            if tx.to_address.as_ref() == Some(&address.to_string()) {
                received_count += 1;
                total_volume_received += &tx.value;
                counterparties.insert(tx.from_address.clone());
            }
        }
        
        Ok(Some(NewAddressStats {
            address: address.to_string(),
            first_seen_block,
            last_seen_block,
            total_transactions: Some(address_txs.len() as i64),
            total_sent_transactions: Some(sent_count),
            total_received_transactions: Some(received_count),
            total_volume_sent: Some(total_volume_sent),
            total_volume_received: Some(total_volume_received),
            gas_used: Some(total_gas_used),
            gas_fees_paid: Some(total_gas_fees),
            unique_counterparties: Some(counterparties.len() as i32),
            contract_deployments: Some(contract_deployments),
        }))
    }
}