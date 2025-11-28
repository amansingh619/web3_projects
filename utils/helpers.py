import os
import json
from hexbytes import HexBytes
from web3 import Web3
from utils.logger import logger
from web3.datastructures import AttributeDict
from pathlib import Path

def to_eth(wei):
    """function to conert wei to eth"""
    return float(Web3.from_wei(wei, "ether"))

def checksum(address):
    """Function to make sure address is correct else raise exception"""
    try:
        return Web3.to_checksum_address(address)
    except Exception:
        logger.error("Invalid address detected!")
        return address

def load_whale_wallets():
    """Load whale wallet addresses from JSON file."""
    data = {}
    # Get project root directory
    project_root = Path(__file__).parent
    
    # Build absolute path
    file_path = project_root / "whale_wallets.json"
    
    with open(file_path, "r", encoding='utf-8') as f:
        data = json.load(f)
        return {addr.lower() for addr in data.values()}

def is_wallet_involved(tx, wallet_set):
    """
    Detect whether a tracked whale wallet is involved in an ETH transaction.

    Args:
        tx (dict or AttributeDict): full transaction object returned by web3
        wallet_set (set): set of checksum addresses being tracked

    Returns:
        list of tuples: [
            ("inflow" / "outflow", wallet_address)
        ]

        Example:
            [("inflow", "0xabc..."), ("outflow", "0xdef...")]
    """

    involved = []
    
    try:
        from_addr = tx.get("from")
        to_addr = tx.get("to")

        # Normalize
        if from_addr:
            from_addr = Web3.to_checksum_address(from_addr)
        if to_addr:
            to_addr = Web3.to_checksum_address(to_addr)

        # Check outflow (wallet sending ETH)
        if from_addr in wallet_set:
            involved.append(("outflow", from_addr))

        # Check inflow (wallet receiving ETH)
        if to_addr in wallet_set:
            involved.append(("inflow", to_addr))

        return involved

    except Exception as e:
        print(f"[WARN] Failed to process transaction {tx.get('hash')}: {e}")
        return []


def connect_to_rpc():
    """ Create and return a Web3 client 
        using the Alchemy RPC URL.
    """
    rpc_url = os.getenv("ALCHEMY_RPC_URL")
    if not rpc_url:
        raise ValueError("ALCHEMY_RPC_URL missing in environment")

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    logger.info("Connected to Alchemy server")
    if not w3.is_connected():
        raise ConnectionError("Unable to connect to Ethereum mainnet")
    return w3


def safe_json(row):
    """Convert row (list) → JSON-safe tuple for DB insertion."""
    new_r = []
    for col in row:
        # Convert nested dict into JSON
        if isinstance(col, dict):
            col = normalize(col)
            new_r.append(json.dumps(col))
        else:
            new_r.append(col)
    return tuple(new_r)


def normalize(value):
    """Recursively convert Web3 objects → JSON-serializable."""
    if isinstance(value, HexBytes):
        return value.hex()

    if isinstance(value, bytes):
        return value.hex()

    # Handle both AttributeDict and regular dict
    if isinstance(value, (dict, AttributeDict)):
        return {k: normalize(v) for k, v in value.items()}

    if isinstance(value, list):
        return [normalize(v) for v in value]

    return value
