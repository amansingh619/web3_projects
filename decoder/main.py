import sys
import os

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import time
from db.connection import get_conn, release_conn
from decoder.transform import decode_blocks, decode_transactions, decode_logs
from utils.helpers import connect_to_rpc

"""
Runner orchestrates decoding for a block range.
It:
- queries raw_blocks / raw_transactions / raw_receipts / raw_logs for a block range
- passes batches to the transform functions
"""

BATCH_BLOCKS = 5 # number of blocks to decode per loop


def fetch_raw_block_rows(start_block, end_block):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT block_number, raw_json FROM raw_blocks WHERE block_number BETWEEN %s AND %s ORDER BY block_number", (start_block, end_block))
        rows = [{"block_number": r[0], "raw_json": r[1]} for r in cur.fetchall()]
        return rows
    finally:
        release_conn(conn)


def fetch_raw_tx_receipt_pairs(start_block, end_block):
    """
    returns list of dicts {'tx': {'tx_hash','block_number','raw_json'}, 'receipt': {...}}
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        # join raw_transactions and raw_receipts on tx_hash
        cur.execute("""
            SELECT t.tx_hash, t.block_number, t.raw_json, r.raw_json
            FROM raw_transactions t
            JOIN raw_receipts r ON t.tx_hash = r.tx_hash
            WHERE t.block_number BETWEEN %s AND %s
            ORDER BY t.block_number
        """, (start_block, end_block))
        out = []
        for tx_hash, block_number, tx_raw, receipt_raw in cur.fetchall():
            out.append({
                "tx": {"tx_hash": tx_hash, "block_number": block_number, "raw_json": tx_raw},
                "receipt": {"tx_hash": tx_hash, "block_number": block_number, "raw_json": receipt_raw}
            })
        return out
    finally:
        release_conn(conn)


def fetch_raw_logs(start_block, end_block):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT tx_hash, block_number, log_index, raw_json
            FROM raw_logs
            WHERE block_number BETWEEN %s AND %s
            ORDER BY block_number, log_index
        """, (start_block, end_block))
        rows = [{"tx_hash": r[0], "block_number": r[1], "log_index": r[2], "raw_json": r[3]} for r in cur.fetchall()]
        return rows
    finally:
        release_conn(conn)


def run_decode_range(start_block, end_block, w3=None):
    if w3 is None:
        w3 = connect_to_rpc()

    t0 = time.time()
    print(f"Decoding blocks {start_block} → {end_block}")

    # decode blocks
    block_rows = fetch_raw_block_rows(start_block, end_block)
    decode_blocks(block_rows)

    # decode txs (join receipts)
    tx_pairs = fetch_raw_tx_receipt_pairs(start_block, end_block)
    decode_transactions(tx_pairs)

    # decode logs (events + erc20)
    log_rows = fetch_raw_logs(start_block, end_block)
    decode_logs(log_rows, w3)

    print(f"Decoded {start_block} → {end_block} in {time.time() - t0:.2f}s")


if __name__ == "__main__":

    START_BLOCK = 23700769
    END_BLOCK = 23707800

    # run in batches to avoid huge queries
    blk = START_BLOCK
    while blk <= END_BLOCK:
        end = min(blk + BATCH_BLOCKS - 1, END_BLOCK)
        run_decode_range(blk, end)
        blk = end + 1
