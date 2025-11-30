import json
from psycopg2.extras import execute_values
from utils.helpers import safe_json
from db.connection import get_conn, release_conn
from utils.logger import logger
from decoder.util import normalize_value


class Database_Operations():
    """Class to handle all the DB related operations"""

    def __init__(self):
        logger.info("DB instance initialized")

    def __del__(self):
        logger.info("DB instance exited")

    def bulk_insert(self, query, rows):
        """Function for bulk insertion"""
        try:
            if not rows:
                return

            conn = get_conn()
            cur = conn.cursor()
            # Convert all rows to JSON-safe form
            safe_rows = [safe_json(r) for r in rows]

            execute_values(cur, query, safe_rows)
            conn.commit()
            release_conn(conn)
        except Exception as e:
            logger.error("Error happened while bulk insertion-> %s", e)

    def insert_blocks_data(self, rows):
        """Function to insert block data into DB"""
        self.bulk_insert("""
            INSERT INTO raw_blocks (block_number, block_timestamp, raw_json)
            VALUES %s ON CONFLICT DO NOTHING;
        """, rows)

    def insert_txs_data(self, rows):
        """Function to insert transaction data in DB"""

        self.bulk_insert("""
            INSERT INTO raw_transactions (tx_hash, block_number, raw_json)
            VALUES %s ON CONFLICT DO NOTHING;
        """, rows)

    def insert_receipts_data(self, rows):
        """Function to insert receipts data in DB"""

        self.bulk_insert("""
            INSERT INTO raw_receipts (tx_hash, block_number, raw_json)
            VALUES %s ON CONFLICT DO NOTHING;
        """, rows)

    def insert_logs_data(self, rows):
        """Function to insert logs data in DB"""
        
        self.bulk_insert("""
            INSERT INTO raw_logs (tx_hash, block_number, log_index, raw_json)
            VALUES %s ON CONFLICT DO NOTHING;
        """, rows)


    def fetch_raw_block_rows(self, start_block, end_block):
        """Function to fetch raw block data from DB"""
        conn = get_conn()
        cur = conn.cursor()
        try:
            query = """
                SELECT block_number, raw_json 
                FROM raw_blocks 
                WHERE block_number BETWEEN %s AND %s 
                ORDER BY block_number
            """
            cur.execute(query, (start_block, end_block))
            rows = [{"block_number": r[0], "raw_json": r[1]} for r in cur.fetchall()]
            return rows
        finally:
            release_conn(conn)

    
    
    def fetch_raw_tx_receipt_pairs(self, start_block, end_block):
        """
        returns list of dicts 
        {
        'tx': {'tx_hash','block_number','raw_json'}, 
        'receipt': {...}
        }
        """
        results = []
        conn = get_conn()
        cur = conn.cursor()
        try:
            query = """
                SELECT tranx.tx_hash, tranx.block_number, tranx.raw_json, receipt.raw_json
                FROM raw_transactions tranx
                JOIN raw_receipts receipt
                ON tranx.tx_hash = receipt.tx_hash
                WHERE tranx.block_number BETWEEN %s AND %s
                ORDER BY tranx.block_number
            """
            cur.execute(query, (start_block, end_block))
            for tx_hash, block_number, tx_raw, receipt_raw in cur.fetchall():
                results.append({
                    "tx": {
                        "tx_hash": tx_hash, 
                        "block_number": block_number, 
                        "raw_json": tx_raw
                    },
                    "receipt": {
                        "tx_hash": tx_hash, 
                        "block_number": block_number, 
                        "raw_json": receipt_raw
                    }
                })
            return results
        finally:
            release_conn(conn)


    def fetch_raw_logs(self, start_block, end_block):
        """Fcuntion to redturn the logs data from DB"""
        conn = get_conn()
        cur = conn.cursor()
        try:
            query = """
                SELECT tx_hash, block_number, log_index, raw_json
                FROM raw_logs
                WHERE block_number BETWEEN %s AND %s
                ORDER BY block_number, log_index
            """
            cur.execute(query, (start_block, end_block))
            rows = [{
                "tx_hash": r[0], 
                "block_number": r[1], 
                "log_index": r[2], 
                "raw_json": r[3
            ]} for r in cur.fetchall()]
            return rows
        finally:
            release_conn(conn)

    
    def decoder_bulk_insertion(self, query, rows):
        """Function to insert transformed data in DB"""
        if not rows:
            return
        conn = get_conn()
        cur = conn.cursor()
        try:
            # normalize JSON fields if present inside rows 
            safe_rows = []
            for r in rows:
                new = []
                for col in r:
                    # If column is dict -> normalize -> json.dumps
                    if isinstance(col, dict):
                        new.append(json.dumps(normalize_value(col)))
                    else:
                        new.append(col)
                safe_rows.append(tuple(new))

            execute_values(cur, query, safe_rows)
            conn.commit()
        finally:
            try:
                cur.close()
            except:
                pass
            release_conn(conn)