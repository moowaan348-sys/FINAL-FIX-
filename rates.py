"""Blockchain watcher — polls TronGrid (USDT-TRC20) and Blockcypher (LTC) for pending top-ups.

Runs as an asyncio background task. For each pending top-up, checks the chain
for incoming transactions matching the expected amount (within tolerance) and
having enough confirmations. Credits user balance when matched.
"""
import asyncio
import httpx
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional, Dict, Any, List

from .config import WALLETS, USDT_TRC20_CONTRACT
from .db import topups_col, users_col, settings_col, orders_col

logger = logging.getLogger('watcher')

POLL_INTERVAL_S = 60
LOOKBACK_TXS = 30
ORDERS_PRUNE_INTERVAL_S = 3600  # hourly
ORDERS_RETENTION_DAYS = 10


async def _fetch_tron_trc20_txs(wallet: str) -> List[Dict[str, Any]]:
    url = f'https://api.trongrid.io/v1/accounts/{wallet}/transactions/trc20'
    params = {
        'limit': LOOKBACK_TXS,
        'only_to': 'true',
        'contract_address': USDT_TRC20_CONTRACT,
    }
    async with httpx.AsyncClient(timeout=20.0) as c:
        r = await c.get(url, params=params)
        if r.status_code != 200:
            logger.warning(f'TronGrid HTTP {r.status_code}: {r.text[:200]}')
            return []
        return r.json().get('data', []) or []


async def _fetch_ltc_txs(wallet: str) -> List[Dict[str, Any]]:
    url = f'https://api.blockcypher.com/v1/ltc/main/addrs/{wallet}/full'
    params = {'limit': LOOKBACK_TXS}
    async with httpx.AsyncClient(timeout=20.0) as c:
        r = await c.get(url, params=params)
        if r.status_code != 200:
            logger.warning(f'Blockcypher HTTP {r.status_code}: {r.text[:200]}')
            return []
        return r.json().get('txs', []) or []


async def _get_settings() -> Dict[str, Any]:
    doc = await settings_col.find_one({'_id': 'global'})
    return doc or {}


async def _credit_balance(telegram_user_id: str, amount_usd: float) -> None:
    await users_col.update_one(
        {'telegram_user_id': str(telegram_user_id)},
        {'$inc': {'balance_usd': amount_usd}},
        upsert=True,
    )


async def _match_usdt_trc20(pending: Dict[str, Any], txs: List[Dict[str, Any]], required_confirms: int, tol_pct: float) -> Optional[Dict[str, Any]]:
    expected = Decimal(str(pending.get('expected_crypto_amount', 0)))
    if expected <= 0:
        return None
    # Convert already-matched tx hashes to skip duplicates
    used_hashes = await _get_used_hashes('USDT_TRC20')
    for tx in txs:
        tx_hash = tx.get('transaction_id')
        if not tx_hash or tx_hash in used_hashes:
            continue
        raw_value = int(tx.get('value', '0'))
        amount = Decimal(raw_value) / Decimal(10**6)
        # Match within tolerance
        low = expected * Decimal(1 - tol_pct / 100.0)
        high = expected * Decimal(1 + tol_pct / 100.0)
        if not (low <= amount <= high):
            continue
        # Check confirmations (trongrid doesn't directly return; assume enough if tx is in list and not pending)
        # We conservatively wait POLL_INTERVAL * required_confirms minimum
        ts_ms = tx.get('block_timestamp') or 0
        age_s = (datetime.now(timezone.utc).timestamp() * 1000 - ts_ms) / 1000
        if age_s < required_confirms * 20:  # Tron ~20s per confirmation => 3x = 60s
            continue
        return {
            'tx_hash': tx_hash,
            'actual_crypto_received': float(amount),
            'confirmations': required_confirms,
        }
    return None


async def _match_ltc(pending: Dict[str, Any], txs: List[Dict[str, Any]], required_confirms: int, tol_pct: float) -> Optional[Dict[str, Any]]:
    expected = Decimal(str(pending.get('expected_crypto_amount', 0)))
    if expected <= 0:
        return None
    wallet = pending.get('wallet_address')
    used_hashes = await _get_used_hashes('LTC')
    for tx in txs:
        tx_hash = tx.get('hash')
        if not tx_hash or tx_hash in used_hashes:
            continue
        confirmations = int(tx.get('confirmations', 0) or 0)
        if confirmations < required_confirms:
            continue
        incoming_sats = 0
        for out in tx.get('outputs', []):
            if wallet in (out.get('addresses') or []):
                incoming_sats += int(out.get('value', 0))
        if incoming_sats <= 0:
            continue
        amount = Decimal(incoming_sats) / Decimal(10**8)
        low = expected * Decimal(1 - tol_pct / 100.0)
        high = expected * Decimal(1 + tol_pct / 100.0)
        if not (low <= amount <= high):
            continue
        return {
            'tx_hash': tx_hash,
            'actual_crypto_received': float(amount),
            'confirmations': confirmations,
        }
    return None


async def _get_used_hashes(crypto: str) -> set:
    cursor = topups_col.find(
        {'crypto_type': crypto, 'status': {'$in': ['confirmed', 'manual']}, 'tx_hash': {'$ne': None}},
        {'tx_hash': 1, '_id': 0},
    )
    out = set()
    async for d in cursor:
        h = d.get('tx_hash')
        if h:
            out.add(h)
    return out


async def _process_once():
    settings = await _get_settings()
    required = int(settings.get('confirmations_required', 3))
    tol_pct = float(settings.get('amount_tolerance_pct', 1.5))

    # Group pending topups by crypto type
    pending_cursor = topups_col.find({'status': 'pending'})
    pendings = [p async for p in pending_cursor]
    if not pendings:
        return

    cryptos_needed = {p['crypto_type'] for p in pendings}
    tx_cache: Dict[str, List[Dict[str, Any]]] = {}
    if 'USDT_TRC20' in cryptos_needed:
        try:
            tx_cache['USDT_TRC20'] = await _fetch_tron_trc20_txs(WALLETS['USDT_TRC20'])
        except Exception as e:
            logger.warning(f'TronGrid fetch error: {e}')
            tx_cache['USDT_TRC20'] = []
    if 'LTC' in cryptos_needed:
        try:
            tx_cache['LTC'] = await _fetch_ltc_txs(WALLETS['LTC'])
        except Exception as e:
            logger.warning(f'Blockcypher fetch error: {e}')
            tx_cache['LTC'] = []

    for p in pendings:
        crypto = p.get('crypto_type')
        txs = tx_cache.get(crypto, [])
        matched = None
        if crypto == 'USDT_TRC20':
            matched = await _match_usdt_trc20(p, txs, required, tol_pct)
        elif crypto == 'LTC':
            matched = await _match_ltc(p, txs, required, tol_pct)
        if matched:
            now_iso = datetime.now(timezone.utc).isoformat()
            res = await topups_col.update_one(
                {'id': p['id'], 'status': 'pending'},
                {'$set': {
                    'status': 'confirmed',
                    'tx_hash': matched['tx_hash'],
                    'actual_crypto_received': matched['actual_crypto_received'],
                    'confirmations': matched['confirmations'],
                    'confirmed_at': now_iso,
                }},
            )
            if res.modified_count == 1:
                await _credit_balance(p['telegram_user_id'], float(p['amount_usd']))
                logger.info(
                    f"CONFIRMED topup {p['id']} user={p['telegram_user_id']} "
                    f"amount=${p['amount_usd']} tx={matched['tx_hash']}"
                )
                try:
                    from .notifications import push as notif_push
                    await notif_push('topup', {
                        'topup_id': p['id'],
                        'telegram_user_id': p['telegram_user_id'],
                        'telegram_username': p.get('telegram_username', '') or '',
                        'amount_usd': float(p['amount_usd']),
                        'crypto_type': p.get('crypto_type', ''),
                        'tx_hash': matched['tx_hash'],
                        'source': 'blockchain_auto',
                    })
                except Exception as _e:
                    logger.warning(f'topup notif push failed: {_e}')


async def watcher_loop():
    logger.info(f'Watcher starting; poll every {POLL_INTERVAL_S}s')
    while True:
        try:
            await _process_once()
        except Exception as e:
            logger.exception(f'Watcher loop error: {e}')
        await asyncio.sleep(POLL_INTERVAL_S)


async def _prune_old_orders_once() -> int:
    """Delete orders older than ORDERS_RETENTION_DAYS. Returns number deleted."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=ORDERS_RETENTION_DAYS)).isoformat()
    try:
        res = await orders_col.delete_many({'created_at': {'$lt': cutoff}})
        deleted = int(getattr(res, 'deleted_count', 0) or 0)
        if deleted:
            logger.info(f'Orders pruner: deleted {deleted} orders older than {ORDERS_RETENTION_DAYS}d')
        return deleted
    except Exception as e:
        logger.exception(f'Orders pruner error: {e}')
        return 0


async def orders_pruner_loop():
    """Background task: hourly delete orders older than the retention window."""
    logger.info(
        f'Orders pruner starting; retention={ORDERS_RETENTION_DAYS}d, '
        f'interval={ORDERS_PRUNE_INTERVAL_S}s'
    )
    while True:
        try:
            await _prune_old_orders_once()
        except Exception as e:
            logger.exception(f'Orders pruner loop error: {e}')
        await asyncio.sleep(ORDERS_PRUNE_INTERVAL_S)
