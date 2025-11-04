CREATE TABLE blocks (
    id SERIAL PRIMARY KEY,
    number BIGINT NOT NULL UNIQUE,
    hash VARCHAR(66) NOT NULL UNIQUE,
    parent_hash VARCHAR(66) NOT NULL,
    timestamp BIGINT NOT NULL,
    gas_limit BIGINT NOT NULL,
    gas_used BIGINT NOT NULL,
    miner VARCHAR(42) NOT NULL,
    difficulty NUMERIC NOT NULL,
    total_difficulty NUMERIC,
    size BIGINT,
    transaction_count INTEGER NOT NULL DEFAULT 0,
    extra_data TEXT,
    logs_bloom TEXT,
    mix_hash VARCHAR(66),
    nonce VARCHAR(18),
    base_fee_per_gas BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_blocks_number ON blocks(number);
CREATE INDEX idx_blocks_hash ON blocks(hash);
CREATE INDEX idx_blocks_timestamp ON blocks(timestamp);
CREATE INDEX idx_blocks_miner ON blocks(miner);