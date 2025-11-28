# decoder/token_utils.py
from web3 import Web3
from db.connection import get_conn, release_conn
from decoder.decode import decode_erc20_metadata  # your existing function
from datetime import datetime

def get_token_from_db(session_cursor, token_address):
    session_cursor.execute("SELECT symbol, decimals FROM tokens WHERE address = %s", (token_address.lower(),))
    row = session_cursor.fetchone()
    return row if row else None

def insert_token_to_db(session_cursor, token_address, symbol, decimals, name=None):
    session_cursor.execute(
        "INSERT INTO tokens(address, symbol, decimals, name, first_seen) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (address) DO NOTHING",
        (token_address.lower(), symbol, decimals, name, datetime())
    )

def get_or_create_token(w3, token_address):
    """
    Returns (symbol, decimals).
    Uses DB tokens table as cache, falls back to on-chain calls (decode_erc20_metadata),
    and inserts metadata into tokens table.
    """
    addr_chain = Web3.to_checksum_address(token_address)
    db_addr = token_address.lower()

    conn = get_conn()
    cur = conn.cursor()
    try:
        row = get_token_from_db(cur, db_addr)
        if row:
            return row[0], int(row[1])

        # fetch metadata from chain (best-effort)
        meta = decode_erc20_metadata(w3, addr_chain)
        symbol = meta.get("symbol") or "UNK"
        decimals = int(meta.get("decimals") or 18)
        name = meta.get("name")
        insert_token_to_db(cur, db_addr, symbol, decimals, name)
        conn.commit()
        return symbol, decimals
    except Exception:
        # on any failure fallback
        try:
            conn.rollback()
        except Exception:
            pass
        return "UNK", 18
    finally:
        release_conn(conn)
