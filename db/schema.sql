-- blocks
CREATE TABLE
IF NOT EXISTS blocks
(
block_number BIGINT PRIMARY KEY,
block_hash TEXT UNIQUE,
parent_hash TEXT,
timestamp TIMESTAMPTZ,
miner TEXT,
gas_used NUMERIC,
gas_limit NUMERIC
);


-- wallets (tracked whales and discovered counterparties)
CREATE TABLE
IF NOT EXISTS wallets
(
address TEXT PRIMARY KEY,
label TEXT,
first_seen TIMESTAMPTZ DEFAULT now
()
);


-- tokens metadata
CREATE TABLE
IF NOT EXISTS tokens
(
address TEXT PRIMARY KEY,
symbol TEXT,
decimals INT,
first_seen TIMESTAMPTZ DEFAULT now
()
);


-- transactions
CREATE TABLE
IF NOT EXISTS transactions
(
tx_hash TEXT PRIMARY KEY,
block_number BIGINT REFERENCES blocks
(block_number),
from_address TEXT,
to_address TEXT,
value NUMERIC,
gas_used BIGINT,
gas_price NUMERIC,
timestamp TIMESTAMPTZ
);


-- ERC20 transfers (normalized)
CREATE TABLE
IF NOT EXISTS token_transfers
(
id BIGSERIAL PRIMARY KEY,
tx_hash TEXT REFERENCES transactions
(tx_hash),
token_address TEXT REFERENCES tokens
(address),
wallet_address TEXT,
direction TEXT,
amount NUMERIC,
symbol TEXT,
timestamp TIMESTAMPTZ
);


-- index for fast queries
CREATE INDEX
IF NOT EXISTS idx_token_transfers_wallet ON token_transfers
(wallet_address);
CREATE INDEX
IF NOT EXISTS idx_transactions_timestamp ON transactions
(timestamp);