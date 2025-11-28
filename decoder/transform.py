# decoder/transform.py
import json
from psycopg2.extras import execute_values
from web3 import Web3

from db.connection import get_conn, release_conn
from decoder.util import json_from_raw, normalize_value, ERC20_TRANSFER_TOPIC, extract_address_from_topic, parse_uint_from_data
from decoder.token_utils import get_or_create_token

# ---------------------------------------------------------
# Bulk insert helpers (use psycopg2 connection pool functions)
# ---------------------------------------------------------
def bulk_insert_rows(query, rows):
    if not rows:
        return
    conn = get_conn()
    cur = conn.cursor()
    try:
        # normalize JSON fields if present inside rows (we expect final arguments are primitives or JSON strings)
        safe_rows = []
        for r in rows:
            new = []
            for col in r:
                # If column is dict -> normalize -> json.dumps
                if isinstance(col, dict):
                    new.append(json.dumps(normalize_value(col)))
                else:
                    new.append(col)
            safe_rows.append(tuple(new))

        execute_values(cur, query, safe_rows)
        conn.commit()
    finally:
        try:
            cur.close()
        except:
            pass
        release_conn(conn)


# ---------------------------------------------------------
# Decode block row
# ---------------------------------------------------------
def decode_blocks(block_rows):
    """
    block_rows: list of dicts from raw_blocks query: each with
      { 'block_number':..., 'raw_json': <dict> }
    returns rows ready to insert into decoded_blocks
    """
    out = []
    for r in block_rows:
        raw = json_from_raw(r.get("raw_json"))
        bn = r.get("block_number")
        out.append((
            bn,
            raw.get("timestamp"),
            raw.get("miner"),
            raw.get("gasUsed"),
            raw.get("gasLimit"),
            raw.get("baseFeePerGas")
        ))
    # insert
    bulk_insert_rows("""
        INSERT INTO decoded_blocks (block_number, block_timestamp, miner, gas_used, gas_limit, base_fee)
        VALUES %s ON CONFLICT DO NOTHING
    """, out)


# ---------------------------------------------------------
# Decode transactions (join raw_transactions + raw_receipts)
# Expect input: list of tuples/dicts where you have both raw tx and its receipt
# ---------------------------------------------------------
def decode_transactions(tx_pairs):
    """
    tx_pairs: iterable of dicts { 'tx': raw_tx_row, 'receipt': raw_receipt_row }
    """
    out = []
    for pair in tx_pairs:
        raw_tx = json_from_raw(pair["tx"].get("raw_json"))
        raw_receipt = json_from_raw(pair["receipt"].get("raw_json"))
        tx_hash = pair["tx"].get("tx_hash")
        bn = pair["tx"].get("block_number")

        value_wei = raw_tx.get("value", 0)
        # web3 Web3.from_wei accepts ints; ensure int
        try:
            value_eth = Web3.from_wei(int(value_wei), "ether")
        except Exception:
            try:
                value_eth = float(value_wei)
            except Exception:
                value_eth = 0.0

        out.append((
            tx_hash,
            bn,
            raw_tx.get("from"),
            raw_tx.get("to"),
            value_eth,
            raw_tx.get("gasPrice"),
            raw_receipt.get("gasUsed"),
            raw_tx.get("input"),
            (raw_tx.get("input")[:10] if raw_tx.get("input") else None)
        ))

    bulk_insert_rows("""
        INSERT INTO decoded_transactions
        (tx_hash, block_number, from_address, to_address, value_eth, gas_price, gas_used, input, method_id)
        VALUES %s ON CONFLICT DO NOTHING
    """, out)


# ---------------------------------------------------------
# Decode generic logs and also produce ERC20 transfer rows
# ---------------------------------------------------------
def decode_logs(log_rows, w3):
    """
    log_rows: list of rows from raw_logs query, each dict { 'tx_hash', 'block_number', 'log_index', 'raw_json' }
    w3: Web3 instance for calls
    """
    events_out = []
    erc20_out = []

    for lr in log_rows:
        tx_hash = lr.get("tx_hash")
        bn = lr.get("block_number")
        log_index = lr.get("log_index")
        raw = json_from_raw(lr.get("raw_json"))

        # normalize topics: may be list of hex strings or bytes
        topics = raw.get("topics") or []
        # topics in DB might already be list of hex strings - leave as-is for storage
        events_out.append((
            tx_hash,
            bn,
            log_index,
            raw.get("address"),
            (topics[0] if len(topics) > 0 else None),
            json.dumps([t if isinstance(t, str) else (t.hex() if hasattr(t,'hex') else str(t)) for t in topics]),
            raw.get("data")
        ))

        # Check ERC20 transfer by topic equality
        t0 = None
        if len(topics) > 0:
            t0 = topics[0]
            if not isinstance(t0, str) and hasattr(t0, "hex"):
                t0 = t0.hex()
            if isinstance(t0, str):
                t0 = t0.lower()

        if t0 == ERC20_TRANSFER_TOPIC:
            # need topics[1], topics[2], data
            if len(topics) >= 3:
                from_addr = extract_address_from_topic(topics[1])
                to_addr = extract_address_from_topic(topics[2])
                amount_raw = parse_uint_from_data(raw.get("data"))
                token_addr = raw.get("address")
                # fetch token metadata (symbol, decimals)
                symbol, decimals = get_or_create_token(w3, token_addr)
                amount = None
                try:
                    amount = amount_raw / (10 ** decimals)
                except Exception:
                    amount = amount_raw
                erc20_out.append((
                    tx_hash,
                    bn,
                    log_index,
                    token_addr.lower(),
                    symbol,
                    decimals,
                    from_addr,
                    to_addr,
                    amount_raw,
                    amount
                ))

    # bulk insert events & erc20 transfers
    bulk_insert_rows("""
        INSERT INTO decoded_events
        (tx_hash, block_number, log_index, contract_address, event_topic, topics, data)
        VALUES %s ON CONFLICT DO NOTHING
    """, events_out)

    bulk_insert_rows("""
        INSERT INTO decoded_erc20_transfers
        (tx_hash, block_number, log_index, token_address, token_symbol, token_decimals, from_address, to_address, amount_raw, amount)
        VALUES %s ON CONFLICT DO NOTHING
    """, erc20_out)
