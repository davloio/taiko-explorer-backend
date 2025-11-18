-- Rollback Auto-refresh materialized view triggers

-- Drop triggers
DROP TRIGGER IF EXISTS trigger_refresh_stats_on_transaction_insert ON transactions;
DROP TRIGGER IF EXISTS trigger_refresh_stats_on_block_insert ON blocks;

-- Drop function
DROP FUNCTION IF EXISTS refresh_explorer_stats();

-- Drop pagination indexes
DROP INDEX IF EXISTS idx_transactions_contract_creation_pagination;
DROP INDEX IF EXISTS idx_transactions_failed_pagination;
DROP INDEX IF EXISTS idx_transactions_to_address_block_desc;
DROP INDEX IF EXISTS idx_transactions_address_block_desc;
DROP INDEX IF EXISTS idx_blocks_pagination_asc;
DROP INDEX IF EXISTS idx_blocks_pagination_desc;
DROP INDEX IF EXISTS idx_transactions_pagination_asc;
DROP INDEX IF EXISTS idx_transactions_pagination_desc;

-- Drop unique index
DROP INDEX IF EXISTS mv_explorer_stats_unique_idx;