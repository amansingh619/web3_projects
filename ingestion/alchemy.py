
from web3 import Web3
import json
from dotenv import load_dotenv
import os
load_dotenv()

alchemy_url = os.getenv('ALCHEMY_RPC_URL') 
web3 = Web3(Web3.HTTPProvider(alchemy_url))

print("Connected:", web3.is_connected())

# latest_block = web3.eth.get_block('latest')
# with open("raw_block.json", "w") as f:
#     json.dump(latest_block, f, indent=2)
# wallet = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e"
# tx_count = web3.eth.get_transaction_count(wallet)
# print(tx_count)

