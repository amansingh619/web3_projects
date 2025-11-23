import asyncio
from utils.logger import logger
from ingestion.process_eth import process_eth_transfers
from ingestion.process_erc20 import process_erc20_logs
from ingestion.save_data import save_block

POLL_INTERVAL = 4  # seconds


async def poll_blocks(w3, wallet_set):
    """
    Polls Ethereum blocks in real-time and streams them to processors.
    """
    latest = w3.eth.block_number
    logger.info(f"[BLOCK LISTENER] Starting from block: {latest}")

    while True:
        try:
            new_block = w3.eth.block_number

            if new_block > latest:
                for bn in range(latest + 1, new_block + 1):
                    block = w3.eth.get_block(bn, full_transactions=True)
                    timestamp = block.timestamp

                    save_block(block)

                    for tx in block.transactions:
                        await process_eth_transfers(w3, tx, timestamp, wallet_set)
                        await process_erc20_logs(w3, tx.hash.hex(), timestamp, wallet_set)

                    logger.info(f"[BLOCK] processed {bn}")

                latest = new_block

            await asyncio.sleep(POLL_INTERVAL)

        except Exception as e:
            logger.error(f"[BLOCK ERROR] {e}")
            await asyncio.sleep(5)
