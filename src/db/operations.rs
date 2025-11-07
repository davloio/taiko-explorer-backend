use anyhow::Result;
use diesel::prelude::*;
use tracing::info;

use crate::db::connection::DbPool;
use crate::models::block::{Block, NewBlock};
use crate::models::transaction::{Transaction, NewTransaction};
use crate::schema::{blocks, transactions};

#[derive(Clone)]
pub struct BlockRepository {
    pool: DbPool,
}

impl BlockRepository {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    pub fn insert_block(&self, new_block: NewBlock) -> Result<Block> {
        let mut conn = self.pool.get()?;
        
        let block = diesel::insert_into(blocks::table)
            .values(&new_block)
            .on_conflict(blocks::number)
            .do_nothing()
            .get_result::<Block>(&mut conn)
            .optional()?;
        
        match block {
            Some(b) => Ok(b),
            None => {
                // Block already exists, fetch it
                self.get_block_by_number(new_block.number)?.ok_or_else(|| {
                    anyhow::anyhow!("Failed to insert or fetch block {}", new_block.number)
                })
            }
        }
    }

    pub fn insert_blocks_batch(&self, new_blocks: Vec<NewBlock>) -> Result<usize> {
        let mut conn = self.pool.get()?;
        
        let inserted = diesel::insert_into(blocks::table)
            .values(&new_blocks)
            .on_conflict(blocks::number)
            .do_nothing()
            .execute(&mut conn)?;
        
        Ok(inserted)
    }

    pub fn get_latest_block_number(&self) -> Result<Option<i64>> {
        let mut conn = self.pool.get()?;
        
        let latest_number = blocks::table
            .select(blocks::number)
            .order(blocks::number.desc())
            .first::<i64>(&mut conn)
            .optional()?;
        
        Ok(latest_number)
    }

    pub fn block_exists(&self, block_number: i64) -> Result<bool> {
        let mut conn = self.pool.get()?;
        
        let count = blocks::table
            .filter(blocks::number.eq(block_number))
            .count()
            .get_result::<i64>(&mut conn)?;
        
        Ok(count > 0)
    }

    pub fn get_block_by_number(&self, block_number: i64) -> Result<Option<Block>> {
        let mut conn = self.pool.get()?;
        
        let block = blocks::table
            .filter(blocks::number.eq(block_number))
            .first::<Block>(&mut conn)
            .optional()?;
        
        Ok(block)
    }

    pub fn get_block_by_hash(&self, block_hash: &str) -> Result<Option<Block>> {
        let mut conn = self.pool.get()?;
        
        let block = blocks::table
            .filter(blocks::hash.eq(block_hash))
            .first::<Block>(&mut conn)
            .optional()?;
        
        Ok(block)
    }

    pub fn get_block_count(&self) -> Result<i64> {
        let mut conn = self.pool.get()?;
        
        let count = blocks::table
            .count()
            .get_result::<i64>(&mut conn)?;
        
        Ok(count)
    }

    // Transaction operations
    pub fn insert_transaction(&self, new_transaction: NewTransaction) -> Result<Transaction> {
        let mut conn = self.pool.get()?;
        
        let transaction = diesel::insert_into(transactions::table)
            .values(&new_transaction)
            .on_conflict(transactions::hash)
            .do_nothing()
            .get_result::<Transaction>(&mut conn)
            .optional()?;
        
        match transaction {
            Some(tx) => Ok(tx),
            None => {
                // Transaction already exists, fetch it
                self.get_transaction_by_hash(&new_transaction.hash)?.ok_or_else(|| {
                    anyhow::anyhow!("Failed to insert or fetch transaction {}", new_transaction.hash)
                })
            }
        }
    }

    pub fn insert_transactions_batch(&self, new_transactions: Vec<NewTransaction>) -> Result<usize> {
        let mut conn = self.pool.get()?;
        
        let inserted = diesel::insert_into(transactions::table)
            .values(&new_transactions)
            .on_conflict(transactions::hash)
            .do_nothing()
            .execute(&mut conn)?;
        
        Ok(inserted)
    }

    pub fn get_transaction_by_hash(&self, tx_hash: &str) -> Result<Option<Transaction>> {
        let mut conn = self.pool.get()?;
        
        let transaction = transactions::table
            .filter(transactions::hash.eq(tx_hash))
            .first::<Transaction>(&mut conn)
            .optional()?;
        
        Ok(transaction)
    }

    pub fn get_transactions_by_block(&self, block_number: i64) -> Result<Vec<Transaction>> {
        let mut conn = self.pool.get()?;
        
        let transactions_list = transactions::table
            .filter(transactions::block_number.eq(block_number))
            .order(transactions::transaction_index.asc())
            .load::<Transaction>(&mut conn)?;
        
        Ok(transactions_list)
    }

    pub fn get_transactions_by_address(&self, address: &str, limit: i64, offset: i64) -> Result<Vec<Transaction>> {
        let mut conn = self.pool.get()?;
        
        let transactions_list = transactions::table
            .filter(
                transactions::from_address.eq(address)
                    .or(transactions::to_address.eq(address))
            )
            .order(transactions::block_number.desc())
            .limit(limit)
            .offset(offset)
            .load::<Transaction>(&mut conn)?;
        
        Ok(transactions_list)
    }

    pub fn get_transactions_paginated(&self, limit: i64, offset: i64, order_desc: bool) -> Result<Vec<Transaction>> {
        let mut conn = self.pool.get()?;
        
        let mut query = transactions::table.into_boxed();
        
        if order_desc {
            query = query.order(transactions::block_number.desc());
        } else {
            query = query.order(transactions::block_number.asc());
        }
        
        let transactions_list = query
            .limit(limit)
            .offset(offset)
            .load::<Transaction>(&mut conn)?;
        
        Ok(transactions_list)
    }

    pub fn get_transaction_count(&self) -> Result<i64> {
        let mut conn = self.pool.get()?;
        
        let count = transactions::table
            .count()
            .get_result::<i64>(&mut conn)?;
        
        Ok(count)
    }

    pub fn get_transactions_count_by_address(&self, address: &str) -> Result<i64> {
        let mut conn = self.pool.get()?;
        
        let count = transactions::table
            .filter(
                transactions::from_address.eq(address)
                    .or(transactions::to_address.eq(address))
            )
            .count()
            .get_result::<i64>(&mut conn)?;
        
        Ok(count)
    }

    pub fn get_failed_transactions(&self, limit: i64) -> Result<Vec<Transaction>> {
        let mut conn = self.pool.get()?;
        
        let transactions_list = transactions::table
            .filter(transactions::status.eq(0))
            .order(transactions::block_number.desc())
            .limit(limit)
            .load::<Transaction>(&mut conn)?;
        
        Ok(transactions_list)
    }

    pub fn get_contract_creation_transactions(&self, limit: i64) -> Result<Vec<Transaction>> {
        let mut conn = self.pool.get()?;
        
        let transactions_list = transactions::table
            .filter(
                transactions::to_address.is_null()
                    .and(transactions::contract_address.is_not_null())
            )
            .order(transactions::block_number.desc())
            .limit(limit)
            .load::<Transaction>(&mut conn)?;
        
        Ok(transactions_list)
    }
}