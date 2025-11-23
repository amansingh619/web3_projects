
from web3 import Web3
import os

def make_web3():
    """Create and return a Web3 client using the Alchemy RPC URL."""
    rpc_url = os.getenv("ALCHEMY_RPC_URL")
    if not rpc_url:
        raise ValueError("ALCHEMY_RPC_URL missing in environment")

    w3 = Web3(Web3.HTTPProvider(rpc_url))

    if not w3.is_connected():
        raise ConnectionError("Unable to connect to Ethereum RPC")

    return w3
