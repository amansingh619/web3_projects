from web3 import Web3
from utils.logger import logger

ERC20_TRANSFER_SIG = Web3.keccak(text="Transfer(address,address,uint256)").hex()

def decode_transfer_log(w3, log):
    """Safely decode an ERC20 Transfer event log."""
    
    ERC20_TRANSFER_SIG = Web3.keccak(text="Transfer(address,address,uint256)").hex().lower()

    # 1️⃣ Check event signature
    if len(log["topics"]) == 0:
        return None

    if log["topics"][0].hex().lower() != ERC20_TRANSFER_SIG:
        return None

    # 2️⃣ Must have at least 3 topics (event + indexed from + to)
    if len(log["topics"]) < 3:
        return None

    # 3️⃣ Extract addresses
    from_addr = "0x" + log["topics"][1].hex()[-40:]
    to_addr   = "0x" + log["topics"][2].hex()[-40:]

    # 4️⃣ Value can be hex-string or bytes
    raw_data = log["data"]

    if isinstance(raw_data, bytes):
        value = int.from_bytes(raw_data, byteorder="big")
    else:
        value = int(raw_data, 16)

    # 5️⃣ Token address
    token_addr = log["address"].lower()

    return {
        "from": from_addr,
        "to": to_addr,
        "value": value,
        "token": token_addr,
    }


# Minimal ABI required for reading ERC20 metadata
ERC20_METADATA_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    },
]


def decode_erc20_metadata(w3: Web3, token_address: str):
    """
    Fetches ERC20 token metadata: name, symbol, decimals.
    Returns dict {name, symbol, decimals}
    Falls back to safe defaults if call fails.
    """

    token_address = Web3.to_checksum_address(token_address)

    contract = w3.eth.contract(address=token_address, abi=ERC20_METADATA_ABI)

    metadata = {}

    # 1️⃣ Name
    try:
        metadata["name"] = contract.functions.name().call()
    except Exception:
        metadata["name"] = "UNKNOWN"

    # 2️⃣ Symbol
    try:
        metadata["symbol"] = contract.functions.symbol().call()
    except Exception:
        metadata["symbol"] = "UNK"

    # 3️⃣ Decimals
    try:
        metadata["decimals"] = contract.functions.decimals().call()
    except Exception:
        metadata["decimals"] = 18  # safest fallback

    return metadata
