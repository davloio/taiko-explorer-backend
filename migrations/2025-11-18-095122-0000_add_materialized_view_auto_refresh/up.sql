-- Auto-refresh materialized view triggers

-- First, add a unique index to enable CONCURRENTLY refresh (if not exists)
CREATE UNIQUE INDEX IF NOT EXISTS mv_explorer_stats_unique_idx ON mv_explorer_stats ((1));

-- Create function to refresh the materialized view
CREATE OR REPLACE FUNCTION refresh_explorer_stats() 
RETURNS TRIGGER AS $$
BEGIN
  -- Use REFRESH MATERIALIZED VIEW CONCURRENTLY to avoid blocking reads
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_explorer_stats;
  RETURN NULL;
EXCEPTION
  -- If concurrent refresh fails, fall back to regular refresh
  WHEN OTHERS THEN
    REFRESH MATERIALIZED VIEW mv_explorer_stats;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create triggers to auto-refresh on data changes
-- Trigger for new blocks
CREATE TRIGGER trigger_refresh_stats_on_block_insert
  AFTER INSERT ON blocks
  FOR EACH STATEMENT
  EXECUTE FUNCTION refresh_explorer_stats();

-- Trigger for new transactions (most important for stats)
CREATE TRIGGER trigger_refresh_stats_on_transaction_insert
  AFTER INSERT ON transactions
  FOR EACH STATEMENT
  EXECUTE FUNCTION refresh_explorer_stats();

-- Add critical pagination indexes for lightning-fast queries
-- Index for transactions pagination (ORDER BY block_number DESC LIMIT/OFFSET)
CREATE INDEX IF NOT EXISTS idx_transactions_pagination_desc ON transactions (block_number DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_pagination_asc ON transactions (block_number ASC, id ASC);

-- Index for blocks pagination (ORDER BY number DESC LIMIT/OFFSET) 
CREATE INDEX IF NOT EXISTS idx_blocks_pagination_desc ON blocks (number DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_blocks_pagination_asc ON blocks (number ASC, id ASC);

-- Index for latest transactions by address (most common query)
CREATE INDEX IF NOT EXISTS idx_transactions_address_block_desc ON transactions (from_address, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_to_address_block_desc ON transactions (to_address, block_number DESC) WHERE to_address IS NOT NULL;

-- Index for failed transactions pagination
CREATE INDEX IF NOT EXISTS idx_transactions_failed_pagination ON transactions (status, block_number DESC) WHERE status = 0;

-- Index for contract creation transactions
CREATE INDEX IF NOT EXISTS idx_transactions_contract_creation_pagination ON transactions (contract_address, block_number DESC) WHERE contract_address IS NOT NULL;

-- Optional: Refresh immediately to ensure view is current
REFRESH MATERIALIZED VIEW mv_explorer_stats;