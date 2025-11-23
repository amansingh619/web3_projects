# main_backfill_single_day.py

import datetime
from utils.web3_client import make_web3
from utils.helpers import load_whale_wallets
from db.connection import SessionLocal
from ingestion.process_eth import handle_eth_transfers
from ingestion.process_erc20 import handle_erc20_transfers
from ingestion.save_data import check_data


# -------------------------------------------------------
# 1. Binary search block by timestamp
# -------------------------------------------------------
def find_block_for_timestamp(w3, target_ts):
    low = 0
    high = w3.eth.block_number
    chosen = None

    while low <= high:
        mid = (low + high) // 2
        blk = w3.eth.get_block(mid)

        if blk.timestamp < target_ts:
            low = mid + 1
        else:
            chosen = mid
            high = mid - 1

    return chosen


# -------------------------------------------------------
# 2. MAIN: backfill data for specific date
# -------------------------------------------------------
def main():
    print("🚀 Starting backfill for 2025-11-01")

    w3 = make_web3()
    whale_wallets = load_whale_wallets()

    # define date window
    start_dt = datetime.datetime(2025, 11, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    end_dt   = datetime.datetime(2025, 11, 2, 0, 0, 0, tzinfo=datetime.timezone.utc)

    start_ts = int(start_dt.timestamp())
    end_ts   = int(end_dt.timestamp())

    # find block range
    print("Finding start block…")
    start_block = find_block_for_timestamp(w3, start_ts)

    print("Finding end block…")
    end_block = find_block_for_timestamp(w3, end_ts)

    print(f"📌 Block range: {start_block} → {end_block}")

    # process blocks
    for block_number in range(start_block, end_block):
        block = w3.eth.get_block(block_number, full_transactions=True)
        session = SessionLocal()

        try:
            handle_eth_transfers(w3, block, session, whale_wallets)
            handle_erc20_transfers(w3, block, session, whale_wallets)
            session.commit()

        except Exception as e:
            print(f"❌ Error in block {block_number}: {e}")
            session.rollback()

        finally:
            session.close()

        if block_number % 25 == 0:
            print(f"Processed block {block_number}")

    print("🎉 Backfill completed!")


if __name__ == "__main__":
    # main()
    check_data()