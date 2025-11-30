# decoder/transform.py
import json
from psycopg2.extras import execute_values
from web3 import Web3

from db.db_operations import Database_Operations
from decoder.util import json_from_raw, ERC20_TRANSFER_TOPIC, extract_address_from_topic, parse_uint_from_data
from decoder.token_utils import get_or_create_token
from utils.logger import logger

db_help = Database_Operations()

# Decode block row
def decode_blocks(block_rows):
    """
    block_rows: list of dicts from raw_blocks query: each with
      { 'block_number':..., 'raw_json': <dict> }
    returns rows ready to insert into decoded_blocks
    """
    try:
        results = []
        for r in block_rows:
            raw = json_from_raw(r.get("raw_json"))
            block_number = r.get("block_number")
            results.append((
                block_number,
                raw.get("timestamp"),
                raw.get("miner"),
                raw.get("gasUsed"),
                raw.get("gasLimit"),
                raw.get("baseFeePerGas")
            ))
        # insert
        db_help.decoder_bulk_insertion("""
            INSERT INTO decoded_blocks (block_number, block_timestamp, miner, gas_used, gas_limit, base_fee)
            VALUES %s ON CONFLICT DO NOTHING
        """, results)
    except Exception as e:
        logger.error("error occured while decoding blocks -> %s", e)


# Decode transactions (join raw_transactions + raw_receipts)
# Expect input: list of tuples/dicts where you have both raw tx and its receipt
def decode_transactions(tx_pairs):
    """
    tx_pairs: iterable of dicts { 'tx': raw_tx_row, 'receipt': raw_receipt_row }
    """
    results = []
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

        results.append((
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

    db_help.decoder_bulk_insertion("""
        INSERT INTO decoded_transactions
        (tx_hash, block_number, from_address, to_address, value_eth, gas_price, gas_used, input, method_id)
        VALUES %s ON CONFLICT DO NOTHING
    """, results)


# Decode generic logs and also produce ERC20 transfer rows
def decode_logs(log_rows, w3):
    """
    log_rows: list of rows from raw_logs query, each dict { 'tx_hash', 'block_number', 'log_index', 'raw_json' }
    w3: Web3 instance for calls
    """
    events_out = []
    erc20_out = []
    
    try:
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
        db_help.decoder_bulk_insertion("""
            INSERT INTO decoded_events
            (tx_hash, block_number, log_index, contract_address, event_topic, topics, data)
            VALUES %s ON CONFLICT DO NOTHING
        """, events_out)

        db_help.decoder_bulk_insertion("""
            INSERT INTO decoded_erc20_transfers
            (tx_hash, block_number, log_index, token_address, token_symbol, token_decimals, from_address, to_address, amount_raw, amount)
            VALUES %s ON CONFLICT DO NOTHING
        """, erc20_out)
    except Exception as e:
        logger.error("error occured while decoding logs -> %s", e)
