# decoder/utils.py
import json
from hexbytes import HexBytes
from web3 import Web3

ERC20_TRANSFER_TOPIC = Web3.keccak(text="Transfer(address,address,uint256)").hex().lower()

def normalize_value(v):
    """Recursively normalize Web3 types to JSON-serializable."""
    if isinstance(v, HexBytes):
        return v.hex()
    if isinstance(v, bytes):
        return v.hex()
    if isinstance(v, dict):
        return {k: normalize_value(val) for k, val in v.items()}
    if isinstance(v, list):
        return [normalize_value(x) for x in v]
    return v

def json_from_raw(raw):
    """Accept raw_json coming from DB (dict or string) and return a dict."""
    if raw is None:
        return {}
    if isinstance(raw, str):
        return json.loads(raw)
    # if it's already a dict (psycopg2 JSONB -> dict)
    return raw

def extract_address_from_topic(topic_hex):
    """Get address (0x...) from topic hex string or bytes (topic contains 32-byte padded address)."""
    if topic_hex is None:
        return None
    if isinstance(topic_hex, bytes) or isinstance(topic_hex, HexBytes):
        hexstr = topic_hex.hex()
    else:
        hexstr = topic_hex if isinstance(topic_hex, str) else str(topic_hex)
        if hexstr.startswith("0x"):
            hexstr = hexstr[2:]
    # last 40 hex chars are address
    addr = "0x" + hexstr[-40:]
    try:
        return Web3.to_checksum_address(addr)
    except Exception:
        # fallback return lowercase
        return addr.lower()

def parse_uint_from_data(data: str):
    if data is None:
        return None
    if data.startswith("0x"):
        return int(data, 16)
    return int(data, 16)
