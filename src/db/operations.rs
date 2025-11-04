use anyhow::Result;
use diesel::prelude::*;
use tracing::info;

use crate::db::connection::DbPool;
use crate::models::block::{Block, NewBlock};
use crate::schema::blocks;

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
            .get_result::<Block>(&mut conn)?;
        
        info!("Inserted block #{}", block.number);
        Ok(block)
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
}