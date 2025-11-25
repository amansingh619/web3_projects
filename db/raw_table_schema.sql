-- raw block
CREATE TABLE IF NOT EXISTS raw_blocks (
    block_number     BIGINT PRIMARY KEY,
    block_timestamp  BIGINT,
    raw_json         JSONB
);

-- raw transaction
CREATE TABLE IF NOT EXISTS raw_transactions (
    tx_hash        TEXT PRIMARY KEY,
    block_number   BIGINT,
    raw_json       JSONB
);
-- raw receipts
CREATE TABLE IF NOT EXISTS raw_receipts (
    tx_hash        TEXT PRIMARY KEY,
    block_number   BIGINT,
    raw_json       JSONB
);

-- raw logs
CREATE TABLE IF NOT EXISTS raw_logs (
    tx_hash      TEXT,
    block_number BIGINT,
    log_index    INT,
    raw_json     JSONB,
    PRIMARY KEY (tx_hash, log_index)
);
