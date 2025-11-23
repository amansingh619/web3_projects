from web3 import Web3
from utils.helpers import is_wallet_involved
from ingestion.save_data import upsert_tx as save_eth_transfer
from utils.logger import logger


def handle_eth_transfers(w3, block, session, whale_set):
    """
    Process ETH transfers inside a block involving whale wallets.
    """

    timestamp = block.timestamp

    for tx in block.transactions:
        involved = is_wallet_involved(tx, whale_set)
        if not involved:
            continue

        tx_hash = tx.hash.hex()
        value_eth = float(w3.from_wei(tx.value, "ether"))

        for direction, wallet_addr in involved:
            session.execute("""
                INSERT INTO eth_transfers (timestamp, tx_hash, wallet_address, direction, value_eth)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                timestamp,
                tx_hash,
                wallet_addr.lower(),
                direction,
                value_eth
            ))

