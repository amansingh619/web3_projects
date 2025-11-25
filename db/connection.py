# db/connection.py

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    """Return raw psycopg2 connection for fast bulk inserts."""
    return psycopg2.connect(DATABASE_URL)

def release_conn(conn):
    """Close psycopg2 connection."""
    conn.close()
