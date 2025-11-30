import sys
import os

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import time
from db.db_operations import Database_Operations
from utils.logger import logger
from decoder.transform import decode_blocks, decode_transactions, decode_logs
from utils.helpers import connect_to_rpc

"""
Runner orchestrates decoding for a block range.
It:
- queries raw_blocks / raw_transactions / raw_receipts / raw_logs for a block range
- passes batches to the transform functions
"""

BATCH_BLOCKS = 2 # number of blocks to decode per loop
db_help = Database_Operations()


def run_decoder_for_range(start_block, end_block):
    """Function to decode all the data from the raw table"""
    w3 = connect_to_rpc()       # connecting to alchemy 
    t0 = time.time()
    logger.info("Decoding blocks %s → %s", start_block, end_block)

    try:
        # decode blocks
        block_rows = db_help.fetch_raw_block_rows(start_block, end_block)
        if not block_rows:
            logger.warning("Empty blocks found while decoding")
            return None
        decode_blocks(block_rows)

        # decode txs (& joining receipts)
        trans_receipt_data = db_help.fetch_raw_tx_receipt_pairs(start_block, end_block)
        if not trans_receipt_data:
            logger.warning("Tranx and receipt data not found for recoding")
            return None
        decode_transactions(trans_receipt_data)

        # decode logs (events + erc20)
        log_rows = db_help.fetch_raw_logs(start_block, end_block)
        if not log_rows:
            logger.warning("Empty logs found from DB")
            return None
        decode_logs(log_rows, w3)

        logger.info("Decoded %s → %s in %s s", start_block, end_block, (time.time() - t0))
    except Exception as e:
        logger.error("Error occured while decoding : %s to %s -> %s", start_block, end_block, e)

if __name__ == "__main__":

    START_BLOCK = 23700776
    END_BLOCK = 23700780

    # running the decoding block in batches 
    bulk = START_BLOCK
    while bulk <= END_BLOCK:
        end = min(bulk + BATCH_BLOCKS - 1, END_BLOCK)
        run_decoder_for_range(bulk, end)
        bulk = end + 1
