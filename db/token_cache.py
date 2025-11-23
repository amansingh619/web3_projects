# avoiding repeated RPC calls by token_cache dict
token_cache = {}

def get_token(address):
    """function to return address in lower case"""
    
    return token_cache.get(address.lower())

def set_token(address, symbol, decimals):
    """function to map address with symbol & decimal value"""

    token_cache[address.lower()] = {"symbol": symbol, "decimals": decimals}