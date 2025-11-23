from utils.decode import decode_transfer_log
from ingestion.save_data import save_token_transfer
from ingestion.save_data import get_or_create_token
from utils.logger import logger


def handle_erc20_transfers(w3, block, session, whale_set):
    """
    Process ERC-20 transfer logs inside every transaction of the block.
    """

    timestamp = block.timestamp

    for tx in block.transactions:
        tx_hash = tx.hash.hex()

        try:
            receipt = w3.eth.get_transaction_receipt(tx_hash)
        except Exception:
            continue

        for log in receipt.logs:
            decoded = decode_transfer_log(w3, log)
            if not decoded:
                continue

            from_addr = decoded["from"]
            to_addr = decoded["to"]

            if from_addr not in whale_set and to_addr not in whale_set:
                continue

            direction = "inflow" if to_addr in whale_set else "outflow"
            whale_addr = to_addr if direction == "inflow" else from_addr

            # metadata from token registry
            symbol, decimals = get_or_create_token(w3, decoded["token"], session)

            amount = decoded["amount_raw"] / (10 ** decimals)

            session.execute("""
                INSERT INTO token_transfers
                (timestamp, tx_hash, wallet_address, direction, token_address, symbol, amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                timestamp,
                tx_hash,
                whale_addr.lower(),
                direction,
                decoded["token"].lower(),
                symbol,
                amount
            ))
