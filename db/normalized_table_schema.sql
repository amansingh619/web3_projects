-- decoded_blocks
CREATE TABLE IF NOT EXISTS decoded_blocks (
    block_number BIGINT PRIMARY KEY,
    block_timestamp BIGINT,
    miner TEXT,
    gas_used BIGINT,
    gas_limit BIGINT,
    base_fee NUMERIC
);

-- decoded_transactions
CREATE TABLE IF NOT EXISTS decoded_transactions (
    tx_hash TEXT PRIMARY KEY,
    block_number BIGINT,
    from_address TEXT,
    to_address TEXT,
    value_eth NUMERIC,
    gas_price NUMERIC,
    gas_used BIGINT,
    input TEXT,
    method_id TEXT
);

-- decoded_events: generic event rows
CREATE TABLE IF NOT EXISTS decoded_events (
    tx_hash TEXT,
    block_number BIGINT,
    log_index INT,
    contract_address TEXT,
    event_topic TEXT,
    topics JSONB,
    data TEXT,
    PRIMARY KEY (tx_hash, log_index)
);

-- decoded_erc20_transfers
CREATE TABLE IF NOT EXISTS decoded_erc20_transfers (
    tx_hash TEXT,
    block_number BIGINT,
    log_index INT,
    token_address TEXT,
    token_symbol TEXT,
    token_decimals INT,
    from_address TEXT,
    to_address TEXT,
    amount_raw NUMERIC,
    amount NUMERIC,
    PRIMARY KEY (tx_hash, log_index)
);

-- token metadata (if not present)
CREATE TABLE IF NOT EXISTS tokens (
    address TEXT PRIMARY KEY,
    symbol TEXT,
    decimals INT,
    name TEXT,
    first_seen TIMESTAMPTZ DEFAULT now()
);

-- Indices for analytics
CREATE INDEX IF NOT EXISTS idx_dec_tx_block ON decoded_transactions (block_number);

CREATE INDEX IF NOT EXISTS idx_erc20_token ON decoded_erc20_transfers (token_address);

CREATE INDEX IF NOT EXISTS idx_erc20_from ON decoded_erc20_transfers (from_address);

CREATE INDEX IF NOT EXISTS idx_erc20_to ON decoded_erc20_transfers (to_address);