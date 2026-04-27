"""Storm.gift integration + auto-refund watcher.

Flow:
  1. When a user buys a line, we set order.check_status='pending' and
     order.scheduled_check_at = now + CHECK_DELAY_S.
  2. Every REFUND_POLL_S, this watcher:
       - submits any 'pending' orders whose scheduled_check_at has passed
         (status -> 'checking', stores batch_id)
       - polls any 'checking' orders' batches; once done, applies refund logic:
           live   -> check_status='live'     (no refund)
           dead   -> check_status='refunded' + credit user back
           error  -> check_status='error'    (admin review, no auto-refund)
           timeout-> check_status='timeout'  (admin review)
"""
import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

import httpx

from .db import orders_col, users_col, settings_col

logger = logging.getLogger('refund')

STORM_API_BASE = os.environ.get('STORM_API_BASE', 'https://api.storm.gift/api/v1')
STORM_API_KEY = os.environ.get('STORM_API_KEY', '')

CHECK_DELAY_S = 120         # How long to wait after purchase before submitting
REFUND_POLL_S = 20          # How often the watcher runs
CHECK_MAX_WAIT_S = 300      # Consider a 'checking' order timed out after this


def _headers() -> Dict[str, str]:
    return {
        'Authorization': f'Bearer {STORM_API_KEY}',
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    }


def _card_string(order: Dict[str, Any]) -> Optional[str]:
    """Build 'number|mm|yy|cvv' for Storm from the order's raw_line / line data.

    Auto-detects both upload formats:
      • combined  → number|mm/yy|cvv|name|…
      • split     → number|mm|yy|cvv|name|…
    """
    raw = order.get('raw_line') or ''
    parts = [p.strip() for p in raw.split('|')]
    if len(parts) < 3 or not parts[0].isdigit():
        return None
    num = parts[0]
    p1 = parts[1]
    # combined format: parts[1] contains slash/dash/space OR is too long for a month
    if any(ch in p1 for ch in ('/', '-', ' ')) or len(p1) > 2:
        import re as _re
        m = _re.match(r'^\s*(\d{1,2})\s*[/\-\. ]\s*(\d{2,4})\s*$', p1)
        if not m:
            return None
        mm = m.group(1).zfill(2)
        yy_raw = m.group(2)
        yy = yy_raw[-2:] if len(yy_raw) >= 2 else yy_raw
        cvv = parts[2] if len(parts) > 2 else ''
    else:
        # split format: parts = num, mm, yy, cvv, …
        if len(parts) < 4:
            return None
        mm = p1.zfill(2)
        yy_raw = parts[2]
        yy = yy_raw[-2:] if len(yy_raw) >= 2 else yy_raw
        cvv = parts[3]
    cvv = (cvv or '').strip()
    if not (cvv.isdigit() and 3 <= len(cvv) <= 4):
        return None
    if not (mm.isdigit() and 1 <= int(mm) <= 12):
        return None
    if not (yy.isdigit() and len(yy) == 2):
        return None
    return f'{num}|{mm}|{yy}|{cvv}'


async def storm_submit(cards: List[str]) -> Optional[Dict[str, Any]]:
    """Submit cards to Storm.gift's /check endpoint.

    Returns:
      • dict with the `data` block (batch_id + accepted_count) on 2xx success.
      • dict with key `_rejected` listing per-card rejections when Storm
        returns 4xx (typically HTTP 422 `no_valid_cards`). The caller can
        inspect this to give the user an accurate reason instead of the
        generic "API unreachable".
      • `None` only on real network/connection/5xx failures.
    """
    if not STORM_API_KEY:
        logger.warning('STORM_API_KEY not set; skipping submission')
        return None
    try:
        async with httpx.AsyncClient(timeout=30.0) as c:
            r = await c.post(f'{STORM_API_BASE}/check', headers=_headers(), json={'cards': cards})
            logger.info(
                'Storm submit → HTTP %s (cards=%d bytes_out=%d bytes_in=%d)',
                r.status_code, len(cards), sum(len(x) for x in cards), len(r.content),
            )
            if r.status_code == 200 or r.status_code == 201:
                return r.json().get('data')
            # 4xx → Storm rejected the payload but is reachable. Surface
            # the reason so the caller can refund the fee AND tell the user why.
            if 400 <= r.status_code < 500:
                try:
                    body = r.json()
                except Exception:
                    body = {'raw': r.text[:300]}
                logger.warning(
                    'Storm submit rejected HTTP %s: %s',
                    r.status_code,
                    str(body)[:400],
                )
                return {
                    '_rejected': True,
                    'status_code': r.status_code,
                    'error_code': (body.get('error') or {}).get('code') if isinstance(body.get('error'), dict) else None,
                    'error_message': (body.get('error') or {}).get('message') if isinstance(body.get('error'), dict) else None,
                    'rejected_details': body.get('rejected') or [],
                }
            # 5xx → treat as network-style failure
            logger.warning(f'Storm submit HTTP {r.status_code}: {r.text[:300]}')
            return None
    except Exception as e:
        logger.warning(f'Storm submit error: {e}')
        return None


async def storm_batch(batch_id: str) -> Optional[Dict[str, Any]]:
    if not STORM_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=20.0) as c:
            r = await c.get(f'{STORM_API_BASE}/check/{batch_id}', headers=_headers())
            if r.status_code >= 400:
                logger.warning(f'Storm batch HTTP {r.status_code}: {r.text[:200]}')
                return None
            return r.json().get('data')
    except Exception as e:
        logger.warning(f'Storm batch error: {e}')
        return None


async def storm_user() -> Optional[Dict[str, Any]]:
    """Fetch account info (credits)."""
    if not STORM_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=15.0) as c:
            r = await c.get(f'{STORM_API_BASE}/user', headers=_headers())
            if r.status_code >= 400:
                return None
            return r.json().get('data')
    except Exception:
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


async def _settings() -> Dict[str, Any]:
    doc = await settings_col.find_one({'_id': 'global'})
    return doc or {}


async def _submit_due_orders():
    """Find orders whose scheduled_check_at has passed and submit them to Storm."""
    now = datetime.now(timezone.utc).isoformat()
    cursor = orders_col.find({
        'check_status': 'pending',
        'scheduled_check_at': {'$lte': now},
    }).limit(50)
    pending = [o async for o in cursor]
    for o in pending:
        card = _card_string(o)
        if not card:
            await orders_col.update_one(
                {'id': o['id']},
                {'$set': {'check_status': 'error', 'check_status_detail': 'Could not build card string'}},
            )
            continue
        data = await storm_submit([card])
        if not data or not data.get('batch_id'):
            # Mark failed submit; will stay 'pending', we'll retry next cycle after a short cooldown
            await orders_col.update_one(
                {'id': o['id']},
                {'$set': {
                    'scheduled_check_at': (datetime.now(timezone.utc) + timedelta(minutes=2)).isoformat(),
                    'check_status_detail': 'Submit failed; will retry',
                }},
            )
            continue
        batch_id = data['batch_id']
        rejected = data.get('rejected', []) or []
        if rejected:
            # Storm refused the card format — skip check
            await orders_col.update_one(
                {'id': o['id']},
                {'$set': {
                    'check_status': 'error',
                    'check_status_detail': f'Rejected: {rejected}',
                    'check_batch_id': batch_id,
                }},
            )
            continue
        await orders_col.update_one(
            {'id': o['id']},
            {'$set': {
                'check_status': 'checking',
                'check_batch_id': batch_id,
                'check_submitted_at': _now_iso(),
            }},
        )
        logger.info(f"Submitted order {o['id']} to Storm (batch {batch_id})")


async def _refund_order(order: Dict[str, Any], reason: str, refund_fee_too: bool = False, fee: float = 0.0):
    price = float(order.get('price_usd', 0))
    refund_amount = price + (fee if refund_fee_too else 0.0)
    tg_id = order['telegram_user_id']
    # Guard against double-refund
    res = await orders_col.update_one(
        {'id': order['id'], 'check_status': {'$ne': 'refunded'}},
        {'$set': {
            'check_status': 'refunded',
            'refunded_at': _now_iso(),
            'refund_amount_usd': refund_amount,
            'check_status_detail': reason,
        }},
    )
    if res.modified_count != 1:
        return False
    await users_col.update_one(
        {'telegram_user_id': tg_id},
        {'$inc': {'balance_usd': refund_amount, 'total_spent_usd': -price}},
    )
    logger.info(f"REFUNDED order {order['id']} user={tg_id} +${refund_amount} ({reason})")
    # Auto-delete duplicates of this dead card (so it can't be resold accidentally)
    deleted_dup = 0
    try:
        # Find the dedupe_key from the matching line doc
        from .db import lines_col
        sold_line = await lines_col.find_one({'id': order.get('line_id')}, {'dedupe_key': 1, 'number': 1})
        if sold_line:
            dk = sold_line.get('dedupe_key')
            if dk:
                dup_res = await lines_col.delete_many({'dedupe_key': dk, 'status': 'available'})
                deleted_dup = dup_res.deleted_count
                if deleted_dup:
                    logger.info(f"Deleted {deleted_dup} duplicate AVAILABLE lines (dedupe_key={dk}) because card came back DEAD")
    except Exception as e:
        logger.warning(f'dead-card dup cleanup failed: {e}')
    # Admin notification
    try:
        from .notifications import push as notif_push
        await notif_push('refund', {
            'order_id': order['id'],
            'telegram_user_id': tg_id,
            'price_usd': price,
            'refund_amount_usd': refund_amount,
            'bin': order.get('bin', ''),
            'reason': reason,
            'duplicates_deleted': deleted_dup,
        })
    except Exception:
        pass
    return True


async def _refund_fee_only(order: Dict[str, Any], fee: float, reason: str):
    """Refund just the checker fee (e.g. when Storm returns error)."""
    tg_id = order['telegram_user_id']
    await orders_col.update_one(
        {'id': order['id']},
        {'$set': {
            'check_status_detail': reason,
            'refund_amount_usd': fee,
        }},
    )
    await users_col.update_one(
        {'telegram_user_id': tg_id},
        {'$inc': {'balance_usd': fee}},
    )
    logger.info(f"FEE REFUND order {order['id']} user={tg_id} +${fee} ({reason})")


async def _process_checking():
    cursor = orders_col.find({'check_status': 'checking'}).limit(100)
    orders = [o async for o in cursor]
    settings = await _settings()
    fee = float(settings.get('refund_checker_fee_usd', 1.0))
    for o in orders:
        batch_id = o.get('check_batch_id')
        if not batch_id:
            await orders_col.update_one({'id': o['id']}, {'$set': {'check_status': 'error', 'check_status_detail': 'missing batch_id'}})
            continue
        data = await storm_batch(batch_id)
        if data is None:
            continue
        batch = data.get('batch', {})
        is_checking = batch.get('is_checking', True)
        items = data.get('items', []) or []
        submitted_iso = o.get('check_submitted_at')
        submitted_dt = _parse_iso(submitted_iso) if submitted_iso else None
        fee_paid = bool(o.get('checker_fee_paid', False))
        if submitted_dt and (datetime.now(timezone.utc) - submitted_dt).total_seconds() > CHECK_MAX_WAIT_S and is_checking:
            await orders_col.update_one(
                {'id': o['id']},
                {'$set': {'check_status': 'timeout', 'check_status_detail': 'Storm did not return a result in time'}},
            )
            if fee_paid:
                await _refund_fee_only(o, fee, 'Checker timeout — $1 fee refunded')
            continue
        if is_checking:
            continue
        item = items[0] if items else None
        status = (item or {}).get('status', 'error')
        detail = (item or {}).get('status_detail') or ''
        if status == 'live':
            # Try to extract approval / response code from detail (Storm returns various formats).
            approval = ''
            if detail:
                import re
                # Try explicit labels first
                m = re.search(r'(?:approval(?:\s*code)?|response(?:\s*code)?|resp|code)\s*[:=\s]*([A-Z0-9\-]{2,10})', detail, re.I)
                if m:
                    approval = m.group(1).upper()
                # Fallback: bare 2-3 digit numeric code in parentheses or surrounded by non-word
                if not approval:
                    m2 = re.search(r'\b(\d{2,3})\b', detail)
                    if m2:
                        approval = m2.group(1)
            await orders_col.update_one(
                {'id': o['id']},
                {'$set': {
                    'check_status': 'live',
                    'check_status_detail': detail or 'Card valid',
                    'check_approval_code': approval,
                }},
            )
            logger.info(f"Order {o['id']} LIVE ({detail}) code={approval} fee_paid={fee_paid}")
        elif status == 'dead':
            # If fee was paid (user-initiated), refund price + fee; else just price.
            await _refund_order(o, detail or 'Dead on Storm check', refund_fee_too=fee_paid, fee=fee)
        else:
            await orders_col.update_one(
                {'id': o['id']},
                {'$set': {
                    'check_status': 'error',
                    'check_status_detail': detail or f'Unknown status: {status}',
                }},
            )
            if fee_paid:
                await _refund_fee_only(o, fee, f'Checker error ({detail or status}) — $1 fee refunded')
            logger.info(f"Order {o['id']} ERROR ({status}): {detail}; fee_paid={fee_paid}")


async def refund_loop():
    logger.info(f'Refund watcher starting; poll every {REFUND_POLL_S}s; key={"set" if STORM_API_KEY else "MISSING"}')
    while True:
        try:
            s = await _settings()
            if s.get('auto_refund_enabled', True):
                await _submit_due_orders()
                await _process_checking()
        except Exception as e:
            logger.exception(f'refund loop error: {e}')
        await asyncio.sleep(REFUND_POLL_S)
