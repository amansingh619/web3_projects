import time
import json
from hexbytes import HexBytes
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.extras import execute_values
from web3.datastructures import AttributeDict
from db.connection import get_conn, release_conn
from utils.web3_client import make_web3
from utils.logger import logger


BATCH_SIZE = 20
MAX_WORKERS = 1   # safe value


# -----------------------------------------
# 2. Binary search block by timestamp
# -----------------------------------------
def find_block_for_timestamp(w3, target_ts):
    low = 0
    high = w3.eth.block_number
    chosen = None

    while low <= high:
        mid = (low + high) // 2
        block = w3.eth.get_block(mid)

        if block.timestamp < target_ts:
            low = mid + 1
        else:
            chosen = mid
            high = mid - 1

    return chosen


# -----------------------------------------
# 3. Bulk Insert Helpers
# -----------------------------------------
# ---------------------------------------------------------
# JSON Normalizer (Fix HexBytes, bytes, nested dicts, lists)
# ---------------------------------------------------------
def normalize(value):
    """Recursively convert Web3 objects → JSON-serializable."""
    if isinstance(value, HexBytes):
        return value.hex()

    if isinstance(value, bytes):
        return value.hex()

    # Handle both AttributeDict and regular dict
    if isinstance(value, (dict, AttributeDict)):
        return {k: normalize(v) for k, v in value.items()}

    if isinstance(value, list):
        return [normalize(v) for v in value]

    return value


def safe_json(row):
    """Convert row (list) → JSON-safe tuple for DB insertion."""
    new_r = []
    for col in row:
        # Convert nested dict into JSON
        if isinstance(col, dict):
            col = normalize(col)
            new_r.append(json.dumps(col))
        else:
            new_r.append(col)
    return tuple(new_r)


def bulk_insert(query, rows):
    if not rows:
        return

    conn = get_conn()
    cur = conn.cursor()

    # Convert all rows to JSON-safe form
    safe_rows = [safe_json(r) for r in rows]

    execute_values(cur, query, safe_rows)
    conn.commit()
    release_conn(conn)


def insert_blocks(rows):
    bulk_insert("""
        INSERT INTO raw_blocks (block_number, block_timestamp, raw_json)
        VALUES %s ON CONFLICT DO NOTHING;
    """, rows)


def insert_txs(rows):
    bulk_insert("""
        INSERT INTO raw_transactions (tx_hash, block_number, raw_json)
        VALUES %s ON CONFLICT DO NOTHING;
    """, rows)


def insert_receipts(rows):
    bulk_insert("""
        INSERT INTO raw_receipts (tx_hash, block_number, raw_json)
        VALUES %s ON CONFLICT DO NOTHING;
    """, rows)


def insert_logs(rows):
    bulk_insert("""
        INSERT INTO raw_logs (tx_hash, block_number, log_index, raw_json)
        VALUES %s ON CONFLICT DO NOTHING;
    """, rows)


# -----------------------------------------
# 4. Fetch a single block (full raw ingestion)
# -----------------------------------------
def fetch_block(block_number):
    try:
        block = w3.eth.get_block(block_number, full_transactions=True)

        block_row = [
            block_number,
            block.timestamp,
            dict(block)
        ]

        tx_rows = []
        receipt_rows = []
        log_rows = []

        for tx in block.transactions:
            tx_hash = tx.hash.hex()
            tx_rows.append([tx_hash, block_number, dict(tx)])

            receipt = w3.eth.get_transaction_receipt(tx_hash)
            receipt_rows.append([tx_hash, block_number, dict(receipt)])

            for lg in receipt.logs:
                log_rows.append([
                    tx_hash,
                    block_number,
                    lg["logIndex"],
                    dict(lg)
                ])

        return {
            "block": block_row,
            "tx": tx_rows,
            "receipt": receipt_rows,
            "logs": log_rows
        }

    except Exception as e:
        print(f"❌ Error fetching block {block_number}: {e}")
        return None


# -----------------------------------------
# 5. Worker Batch Processor
# -----------------------------------------
def process_batch(start_block, end_block):
    all_blocks, all_txs, all_receipts, all_logs = [], [], [], []

    for blk in range(start_block, end_block + 1):
        res = fetch_block(blk)
        if res:
            all_blocks.append(res["block"])
            all_txs.extend(res["tx"])
            all_receipts.extend(res["receipt"])
            all_logs.extend(res["logs"])

    insert_blocks(all_blocks)
    insert_txs(all_txs)
    insert_receipts(all_receipts)
    insert_logs(all_logs)

    print(f"✅ Finished {start_block} → {end_block}")
    return True


if __name__ == "__main__":
    w3 = make_web3()        # creating an connection with RPC
    DATE = '2025-11-01'
    START_BLOCK, END_BLOCK = None, None
    if DATE:
        logger.info("Starting raw block ingestion for %s", DATE)
        # 2025-11-01 to 2025-11-02 (UTC)
        start_date = datetime.datetime(2025, 11, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        end_date = datetime.datetime(2025, 11, 2, 0, 0, 0, tzinfo=datetime.timezone.utc)
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())
        
        logger.info("Finding start block…")
        START_BLOCK = find_block_for_timestamp(w3, start_ts)

        logger.info("Finding end block…")
        END_BLOCK = find_block_for_timestamp(w3, end_ts)

        logger.info("Block range to process for %s : \t %s to %s", DATE, START_BLOCK, END_BLOCK)
    else:
        # by default the ingestion module will work o the basis of input start & end block
        START_BLOCK = 23700768
        END_BLOCK = 23700770
        logger.info("Block range to process: \t %s to %s", START_BLOCK, END_BLOCK)


    t0 = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        blk = START_BLOCK

        while blk <= END_BLOCK:
            batch_end = min(blk + BATCH_SIZE - 1, END_BLOCK)
            futures.append(executor.submit(process_batch, blk, batch_end))
            blk += BATCH_SIZE

        for f in as_completed(futures):
            f.result()

    logger.info("Completed ingestion in %ss!", (time.time() - t0:.2f))
