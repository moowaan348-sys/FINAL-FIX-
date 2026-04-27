"""Crypto price helper — fetches from CoinGecko free API, with fallback."""
import httpx
import time
from typing import Dict

_CACHE: Dict[str, float] = {}
_CACHE_TS: float = 0.0
_TTL = 300  # 5 min

_COIN_IDS = {
    'USDT_TRC20': 'tether',
    'LTC': 'litecoin',
    'BTC': 'bitcoin',
    'ETH': 'ethereum',
}


async def get_rates_usd(fallback: Dict[str, float]) -> Dict[str, float]:
    global _CACHE, _CACHE_TS
    now = time.time()
    if _CACHE and (now - _CACHE_TS) < _TTL:
        return _CACHE
    ids = ','.join(_COIN_IDS.values())
    url = f'https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd'
    try:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.get(url)
            if r.status_code == 200:
                data = r.json()
                rates = {}
                for k, coin_id in _COIN_IDS.items():
                    if coin_id in data and 'usd' in data[coin_id]:
                        rates[k] = float(data[coin_id]['usd'])
                    else:
                        rates[k] = fallback.get(k, 1.0)
                _CACHE = rates
                _CACHE_TS = now
                return rates
    except Exception as e:
        print(f'[rates] coingecko fetch failed: {e}')
    return fallback


async def usd_to_crypto(amount_usd: float, crypto: str, fallback: Dict[str, float]) -> float:
    rates = await get_rates_usd(fallback)
    rate = rates.get(crypto, fallback.get(crypto, 1.0))
    if rate <= 0:
        rate = fallback.get(crypto, 1.0)
    # Round to 6 decimals for USDT, 8 for BTC/LTC/ETH
    decimals = 6 if crypto == 'USDT_TRC20' else 8
    return round(amount_usd / rate, decimals)
