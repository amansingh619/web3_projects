import time
import json
from hexbytes import HexBytes
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.helpers import connect_to_rpc
from utils.logger import logger
from db.db_operations import Database_Operations

db_inst = Database_Operations()
w3 = connect_to_rpc()    # creating an connection with RPC


def find_block_for_timestamp(w3, target_ts):
    """function to block number for the targetted timestamp"""
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


def get_block_data(block_number):
    """Fucntion to get raw block data 

    Args:
        block_number (int): block number 
    Returns:
        dict: list of values
    """
    try:
        block = w3.eth.get_block(block_number, full_transactions=True)
        block_row = [
            block_number,
            block.timestamp,
            dict(block)
        ]

        tx_rows, receipt_rows, log_rows = [], [], []

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
        logger.error("Error fetching block %s: %s", block_number, e)
        return {}


def process_batch(start_block, end_block):
    """Function to fetch & insert the data for given block range

    Args:
        start_block (int): start block number
        end_block (int): end block number

    Returns:
        bool: returns True if it's succesful else False
    """
    all_blocks, all_txs, all_receipts, all_logs = [], [], [], []

    try:
        for block_number in range(start_block, end_block + 1):
            data = get_block_data(block_number)
            if data:
                all_blocks.append(data["block"])
                all_txs.extend(data["tx"])
                all_receipts.extend(data["receipt"])
                all_logs.extend(data["logs"])

        db_inst.insert_blocks_data(all_blocks)
        db_inst.insert_txs_data(all_txs)
        db_inst.insert_receipts_data(all_receipts)
        db_inst.insert_logs_data(all_logs)

        logger.info("Data for %s → %s stored in DB!!", start_block, end_block)
        return True
    except Exception as e:
        logger.error("Error occured during batch processing -> %s", e)
        return False


def get_block_number_for_date(date):
    """
        Function to generate start & 
        and block number based on date value
    """
    try:
        logger.info("Starting raw block ingestion for %s", date)
        # 2025-11-01 to 2025-11-02 (UTC)
        start_date = datetime.datetime(2025, 11, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        end_date = datetime.datetime(2025, 11, 2, 0, 0, 0, tzinfo=datetime.timezone.utc)
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())
        
        logger.info("Finding start block…")
        start_block = find_block_for_timestamp(w3, start_ts)

        logger.info("Finding end block…")
        end_block = find_block_for_timestamp(w3, end_ts)

        logger.info("Block to process for %s : %s to %s", date, start_block,end_block)
        return start_block, end_block
    except Exception  as e:
        logger.error("Error occured for date to block functions -> %s",e)
        return None, None
    

if __name__ == "__main__":
    DATE = None
    BATCH_SIZE = 20     # TODO: batch size for bulk insertion can be increased
    MAX_WORKERS = 1     # TODO: we can increas in future
    START_BLOCK, END_BLOCK = None, None
    if DATE:
        START_BLOCK, END_BLOCK = get_block_number_for_date(date=DATE)
    else:
        # by default the ingestion module will work on the basis
        # of input start & end block
        START_BLOCK = 23700831
        END_BLOCK = 23700860
        logger.info("Block to process: %s to %s", START_BLOCK, END_BLOCK)

    initial_time = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        blk = START_BLOCK

        while blk <= END_BLOCK:
            batch_end = min(blk + BATCH_SIZE - 1, END_BLOCK)
            futures.append(executor.submit(process_batch, blk, batch_end))
            blk += BATCH_SIZE

        for f in as_completed(futures):
            f.result()
            
    logger.info("Completed ingestion in %s s!", (time.time() - initial_time))
