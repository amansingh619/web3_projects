import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # root directory
from utils.logger import logger
from db.connection import get_conn, release_conn
from utils.decode import decode_erc20_metadata



def save_block(block):
    """Function to save block value in DB"""
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
                INSERT INTO blocks (block_number, block_hash, parent_hash, timestamp, miner, gas_used, gas_limit)
                VALUES (%s,%s,%s,to_timestamp(%s),%s,%s,%s)
                ON CONFLICT (block_number) DO NOTHING
            """,
            (
                block.number,
                block.hash.hex(),
                block.parentHash.hex(),
                block.timestamp,
                block.miner,
                block.gasUsed,
                block.gasLimit,
            ),
        )
        conn.commit()
    except Exception as e:
        logger.exception("save_block failed: %s", e)
    finally:
        cur.close()
        release_conn(conn)

def upsert_wallet(address, label=None):
    """Functionn to save wallet data"""
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
        """
        INSERT INTO wallets (address, label) VALUES (%s,%s)
        ON CONFLICT (address) DO UPDATE SET label = COALESCE(wallets.label, EXCLUDED.label)
        """,
        (address.lower(), label),
        )
        conn.commit()
    except Exception as e:
        logger.exception("upsert_wallet failed: %s", e)
    finally:
        cur.close()
        release_conn(conn)


def upsert_tx(tx_hash, block_number, from_addr, to_addr, value, gas_used, gas_price, timestamp):
    """Function to save transaction"""
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO transactions (tx_hash, block_number, from_address, to_address, value, gas_used, gas_price, timestamp)
            VALUES (%s,%s,%s,%s,%s,%s,%s,to_timestamp(%s))
            ON CONFLICT (tx_hash) DO NOTHING
            """,
            (tx_hash, block_number, from_addr, to_addr, value, gas_used, gas_price, timestamp),
        )
        conn.commit()
    except Exception as e:
        logger.exception("upsert_tx failed: %s", e)
    finally:
        cur.close()
        release_conn(conn)

def save_token_transfer(record):
    """Function to save token transfer details"""
    # record: {tx_hash, token_address, wallet_address, direction, amount, symbol, timestamp}
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO token_transfers (tx_hash, token_address, wallet_address, direction, amount, symbol, timestamp)
            VALUES (%s,%s,%s,%s,%s,%s,to_timestamp(%s))
            """,
            (
            record.get("tx_hash"),
            record.get("token_address").lower(),
            record.get("wallet_address").lower(),
            record.get("direction"),
            record.get("amount"),
            record.get("symbol"),
            record.get("timestamp"),
            ),
        )
        conn.commit()
    except Exception as e:
        logger.exception("save_token_transfer failed: %s", e)
    finally:
        cur.close()
        release_conn(conn)

def get_or_create_token(w3, token_address: str):
    """
    Returns (symbol, decimals) for a token.
    - Uses lowercase address for DB
    - Uses checksum address for Web3
    """

    token_db_addr = token_address.lower()
    token_chain_addr = Web3.to_checksum_address(token_address)

    conn = get_conn()
    cursor = conn.cursor()

    try:
        # 1️⃣ Check DB
        cursor.execute("""
            SELECT symbol, decimals
            FROM tokens
            WHERE address = %s
        """, (token_db_addr,))

        row = cursor.fetchone()
        if row:
            return row[0], row[1]

        # 2️⃣ Read metadata from chain
        metadata = decode_erc20_metadata(w3, token_chain_addr)

        symbol = metadata["symbol"]
        decimals = metadata["decimals"]

        # 3️⃣ Insert into DB
        cursor.execute("""
            INSERT INTO tokens (address, symbol, decimals)
            VALUES (%s, %s, %s)
        """, (token_db_addr, symbol, decimals))

        conn.commit()

        return symbol, decimals

    finally:
        release_conn(conn)


    