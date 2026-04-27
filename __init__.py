"""Admin REST API — JWT-protected. The admin UI talks to these."""
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone, timedelta
import logging

from ..auth import require_admin, verify_password, create_admin_token
from ..db import users_col, lines_col, topups_col, orders_col, admins_col, settings_col
from ..models import Line
from ..parser import parse_bulk_lines
from ..config import WALLETS
from ..refund import storm_user, storm_submit, storm_batch, _refund_order

router = APIRouter(prefix='/admin', tags=['admin'])
logger = logging.getLogger('admin_api')


# ---- Auth ----
class LoginIn(BaseModel):
    username: str
    password: str


@router.post('/login')
async def admin_login(body: LoginIn):
    doc = await admins_col.find_one({'username': body.username})
    if not doc or not verify_password(body.password, doc['password_hash']):
        raise HTTPException(status_code=401, detail='Invalid credentials')
    token = create_admin_token(body.username)
    return {'token': token, 'username': body.username}


@router.get('/me')
async def me(user: str = Depends(require_admin)):
    return {'username': user}


# ---- Dashboard ----
@router.get('/dashboard')
async def dashboard(_: str = Depends(require_admin)):
    total_users = await users_col.count_documents({})
    available_stock = await lines_col.count_documents({'status': 'available'})
    sold_lines = await lines_col.count_documents({'status': 'sold'})
    pending_topups = await topups_col.count_documents({'status': 'pending'})
    # Revenue = sum of confirmed + manual topups amount_usd (what users actually paid in)
    rev_agg = topups_col.aggregate([
        {'$match': {'status': {'$in': ['confirmed', 'manual']}}},
        {'$group': {'_id': None, 'sum': {'$sum': '$amount_usd'}}},
    ])
    rev = 0.0
    async for r in rev_agg:
        rev = float(r.get('sum', 0) or 0)
    # Sold revenue from orders
    orders_rev_agg = orders_col.aggregate([
        {'$group': {'_id': None, 'sum': {'$sum': '$price_usd'}}},
    ])
    orders_rev = 0.0
    async for r in orders_rev_agg:
        orders_rev = float(r.get('sum', 0) or 0)
    # Sold today
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    sold_today = await orders_col.count_documents({'created_at': {'$gte': today_start}})
    # Latest
    latest_orders = [d async for d in orders_col.find({}, {'_id': 0}).sort('created_at', -1).limit(10)]
    latest_topups = [d async for d in topups_col.find({}, {'_id': 0}).sort('created_at', -1).limit(10)]
    return {
        'total_users': total_users,
        'available_stock': available_stock,
        'sold_lines': sold_lines,
        'pending_topups': pending_topups,
        'topup_revenue_usd': rev,
        'sales_revenue_usd': orders_rev,
        'sold_today': sold_today,
        'latest_orders': latest_orders,
        'latest_topups': latest_topups,
    }


# ---- Users ----
@router.get('/users')
async def list_users(
    _: str = Depends(require_admin),
    q: Optional[str] = None,
    skip: int = 0,
    limit: int = 50,
):
    filt: Dict[str, Any] = {}
    if q:
        filt = {'$or': [
            {'telegram_user_id': {'$regex': q, '$options': 'i'}},
            {'telegram_username': {'$regex': q, '$options': 'i'}},
            {'first_name': {'$regex': q, '$options': 'i'}},
        ]}
    total = await users_col.count_documents(filt)
    cursor = users_col.find(filt, {'_id': 0}).sort('created_at', -1).skip(skip).limit(min(limit, 200))
    items = [d async for d in cursor]
    return {'items': items, 'total': total}


@router.get('/users/{telegram_user_id}')
async def user_detail(telegram_user_id: str, _: str = Depends(require_admin)):
    user = await users_col.find_one({'telegram_user_id': telegram_user_id}, {'_id': 0})
    if not user:
        raise HTTPException(404, 'user not found')
    orders = [d async for d in orders_col.find({'telegram_user_id': telegram_user_id}, {'_id': 0}).sort('created_at', -1).limit(50)]
    topups = [d async for d in topups_col.find({'telegram_user_id': telegram_user_id}, {'_id': 0}).sort('created_at', -1).limit(50)]
    return {'user': user, 'orders': orders, 'topups': topups}


class UserPatch(BaseModel):
    banned: Optional[bool] = None
    balance_adjustment_usd: Optional[float] = None
    note: Optional[str] = None


@router.patch('/users/{telegram_user_id}')
async def patch_user(telegram_user_id: str, body: UserPatch, _: str = Depends(require_admin)):
    updates: Dict[str, Any] = {}
    if body.banned is not None:
        updates['banned'] = body.banned
    if updates:
        await users_col.update_one({'telegram_user_id': telegram_user_id}, {'$set': updates})
    if body.balance_adjustment_usd:
        await users_col.update_one(
            {'telegram_user_id': telegram_user_id},
            {'$inc': {'balance_usd': float(body.balance_adjustment_usd)}},
        )
    u = await users_col.find_one({'telegram_user_id': telegram_user_id}, {'_id': 0})
    return u


# ---- Topups ----
@router.get('/topups')
async def list_topups(
    _: str = Depends(require_admin),
    status: Optional[str] = None,
    q: Optional[str] = None,
    skip: int = 0,
    limit: int = 50,
):
    filt: Dict[str, Any] = {}
    if status and status != 'all':
        filt['status'] = status
    if q:
        filt['$or'] = [
            {'telegram_user_id': {'$regex': q, '$options': 'i'}},
            {'tx_hash': {'$regex': q, '$options': 'i'}},
            {'wallet_address': {'$regex': q, '$options': 'i'}},
            {'id': {'$regex': q, '$options': 'i'}},
        ]
    total = await topups_col.count_documents(filt)
    cursor = topups_col.find(filt, {'_id': 0}).sort('created_at', -1).skip(skip).limit(min(limit, 200))
    items = [d async for d in cursor]
    return {'items': items, 'total': total}


class ManualConfirmIn(BaseModel):
    tx_hash: Optional[str] = None
    note: Optional[str] = None


@router.post('/topups/{topup_id}/confirm')
async def manual_confirm(topup_id: str, body: ManualConfirmIn, _: str = Depends(require_admin)):
    topup = await topups_col.find_one({'id': topup_id})
    if not topup:
        raise HTTPException(404, 'topup not found')
    if topup.get('status') not in ('pending', 'failed'):
        raise HTTPException(400, f"cannot confirm topup in status {topup.get('status')}")
    now_iso = datetime.now(timezone.utc).isoformat()
    await topups_col.update_one(
        {'id': topup_id},
        {'$set': {
            'status': 'manual',
            'tx_hash': body.tx_hash or topup.get('tx_hash'),
            'confirmed_at': now_iso,
            'note': body.note or 'Manually confirmed by admin',
        }},
    )
    await users_col.update_one(
        {'telegram_user_id': topup['telegram_user_id']},
        {'$inc': {'balance_usd': float(topup.get('amount_usd', 0))}},
        upsert=True,
    )
    try:
        from ..notifications import push as notif_push
        await notif_push('topup', {
            'topup_id': topup_id,
            'telegram_user_id': topup['telegram_user_id'],
            'telegram_username': topup.get('telegram_username', '') or '',
            'amount_usd': float(topup.get('amount_usd', 0)),
            'crypto_type': topup.get('crypto_type', ''),
            'source': 'manual_admin',
        })
    except Exception:
        pass
    return {'ok': True}


@router.post('/topups/{topup_id}/fail')
async def mark_failed(topup_id: str, _: str = Depends(require_admin)):
    res = await topups_col.update_one(
        {'id': topup_id, 'status': 'pending'},
        {'$set': {'status': 'failed'}},
    )
    return {'ok': res.modified_count == 1}


# ---- Orders ----
@router.get('/orders')
async def list_orders(
    _: str = Depends(require_admin),
    q: Optional[str] = None,
    check_status: Optional[str] = None,
    skip: int = 0,
    limit: int = 50,
):
    filt: Dict[str, Any] = {}
    if check_status and check_status != 'all':
        filt['check_status'] = check_status
    if q:
        filt['$or'] = [
            {'telegram_user_id': {'$regex': q, '$options': 'i'}},
            {'bin': {'$regex': q}},
            {'id': {'$regex': q, '$options': 'i'}},
        ]
    total = await orders_col.count_documents(filt)
    cursor = orders_col.find(filt, {'_id': 0}).sort('created_at', -1).skip(skip).limit(min(limit, 200))
    items = [d async for d in cursor]
    return {'items': items, 'total': total}


# ---- Stock (lines) ----
@router.get('/lines')
async def list_lines(
    _: str = Depends(require_admin),
    status: Optional[str] = None,
    base: Optional[str] = None,
    country: Optional[str] = None,
    bin_prefix: Optional[str] = None,
    skip: int = 0,
    limit: int = 50,
):
    filt: Dict[str, Any] = {}
    if status and status != 'all':
        filt['status'] = status
    if base:
        filt['base_name'] = base
    if country:
        filt['country'] = country.upper()
    if bin_prefix:
        filt['bin'] = {'$regex': f'^{bin_prefix}'}
    total = await lines_col.count_documents(filt)
    cursor = lines_col.find(filt, {'_id': 0}).sort('created_at', -1).skip(skip).limit(min(limit, 500))
    items = [d async for d in cursor]
    return {'items': items, 'total': total}


@router.delete('/lines/{line_id}')
async def delete_line(line_id: str, _: str = Depends(require_admin)):
    res = await lines_col.delete_one({'id': line_id, 'status': 'available'})
    return {'ok': res.deleted_count == 1}


class BulkUploadIn(BaseModel):
    base_name: str
    price: float
    text: str
    skip_duplicates: bool = True


@router.post('/lines/bulk-upload')
async def bulk_upload(body: BulkUploadIn, _: str = Depends(require_admin)):
    base_name = (body.base_name or 'default').strip()
    price = float(body.price)
    if price <= 0:
        raise HTTPException(400, 'price must be > 0')
    records, errors = parse_bulk_lines(body.text)
    known_dead = await _known_dead_dedupe_keys()
    inserted = 0
    duplicates = 0
    dead_blocked = 0
    sample: List[Dict[str, Any]] = []
    for rec in records:
        if rec['dedupe_key'] in known_dead:
            dead_blocked += 1
            continue
        if body.skip_duplicates:
            existing = await lines_col.find_one({'dedupe_key': rec['dedupe_key']}, {'_id': 1})
            if existing:
                duplicates += 1
                continue
        line = Line(
            **rec,
            base_name=base_name,
            price=price,
        ).model_dump()
        try:
            await lines_col.insert_one(line)
            inserted += 1
            if len(sample) < 5:
                sample.append({k: line[k] for k in ('id', 'bin', 'exp_month', 'exp_year', 'country', 'state', 'price')})
        except Exception as e:
            errors.append({'reason': str(e), 'raw': rec.get('raw_line', '')})
    # Ensure base doc exists
    from ..db import bases_col
    await bases_col.update_one(
        {'name': base_name},
        {'$setOnInsert': {'name': base_name, 'created_at': datetime.now(timezone.utc).isoformat()}},
        upsert=True,
    )
    return {
        'parsed': len(records),
        'inserted': inserted,
        'duplicates': duplicates,
        'dead_blocked': dead_blocked,
        'errors': errors,
        'sample_inserted': sample,
    }


async def _known_dead_dedupe_keys() -> set:
    """dedupe_keys of cards known to be dead (from past refunded orders)."""
    keys = set()
    refunded_line_ids = []
    async for d in orders_col.find({'check_status': 'refunded'}, {'line_id': 1, '_id': 0}):
        if d.get('line_id'):
            refunded_line_ids.append(d['line_id'])
    if refunded_line_ids:
        async for d in lines_col.find({'id': {'$in': refunded_line_ids}}, {'dedupe_key': 1, '_id': 0}):
            if d.get('dedupe_key'):
                keys.add(d['dedupe_key'])
    return keys


class ParsePreviewIn(BaseModel):
    text: str


@router.post('/lines/parse-preview')
async def parse_preview(body: ParsePreviewIn, _: str = Depends(require_admin)):
    records, errors = parse_bulk_lines(body.text)
    # Strip the CVV and full number for safer preview display — still leave available for admin
    preview = []
    for r in records[:200]:
        preview.append({
            'bin': r['bin'],
            'exp_month': r['exp_month'],
            'exp_year': r['exp_year'],
            'country': r['country'],
            'state': r['state'],
            'name': r['name'],
            'email': r['email'],
            'zip': r['zip'],
            'number_masked': r['number'][:6] + 'X' * max(0, len(r['number']) - 10) + r['number'][-4:] if len(r['number']) >= 10 else r['number'],
        })
    return {'parsed': len(records), 'errors': errors, 'preview': preview}


# ---- Bases ----
@router.get('/bases')
async def list_bases(_: str = Depends(require_admin)):
    pipeline = [
        {'$group': {
            '_id': '$base_name',
            'total': {'$sum': 1},
            'available': {'$sum': {'$cond': [{'$eq': ['$status', 'available']}, 1, 0]}},
            'sold': {'$sum': {'$cond': [{'$eq': ['$status', 'sold']}, 1, 0]}},
        }},
        {'$sort': {'_id': 1}},
    ]
    out = []
    async for d in lines_col.aggregate(pipeline):
        out.append({
            'name': d['_id'] or 'default',
            'total': d['total'],
            'available': d['available'],
            'sold': d['sold'],
        })
    return out


# ---- Settings ----
@router.get('/settings')
async def get_settings(_: str = Depends(require_admin)):
    doc = await settings_col.find_one({'_id': 'global'}) or {}
    doc.pop('_id', None)
    doc['wallets'] = WALLETS
    return doc


class SettingsIn(BaseModel):
    min_topup_usd: Optional[float] = None
    default_price_usd: Optional[float] = None
    confirmations_required: Optional[int] = None
    amount_tolerance_pct: Optional[float] = None
    auto_refund_enabled: Optional[bool] = None
    auto_refund_delay_s: Optional[int] = None


@router.put('/settings')
async def update_settings(body: SettingsIn, _: str = Depends(require_admin)):
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if updates:
        await settings_col.update_one({'_id': 'global'}, {'$set': updates}, upsert=True)
    doc = await settings_col.find_one({'_id': 'global'}) or {}
    doc.pop('_id', None)
    doc['wallets'] = WALLETS
    return doc


# ---- Storm / Auto-refund ----
@router.get('/storm/credits')
async def storm_credits(_: str = Depends(require_admin)):
    """Surface remaining Storm credits so admin knows when to top up."""
    info = await storm_user()
    if not info:
        return {'ok': False, 'credits': None, 'detail': 'Unable to reach Storm API'}
    return {
        'ok': True,
        'credits': info.get('credits'),
        'thread_count': info.get('thread_count'),
        'plan': (info.get('current_plan') or {}).get('name'),
        'remaining_plan_credits': (info.get('current_plan') or {}).get('remaining_credits'),
    }


@router.post('/orders/{order_id}/recheck')
async def admin_recheck(order_id: str, _: str = Depends(require_admin)):
    """Force-submit this order's card to Storm for (re)checking."""
    order = await orders_col.find_one({'id': order_id}, {'_id': 0})
    if not order:
        raise HTTPException(404, 'order not found')
    raw = order.get('raw_line', '')
    parts = raw.split('|') if raw else []
    if len(parts) < 4 or not parts[0].isdigit():
        raise HTTPException(400, 'cannot build card string from raw_line')
    yy = parts[2][-2:] if len(parts[2]) >= 2 else parts[2]
    card = f'{parts[0]}|{parts[1].zfill(2)}|{yy}|{parts[3]}'
    data = await storm_submit([card])
    if not data or not data.get('batch_id'):
        raise HTTPException(502, 'Storm submit failed')
    batch_id = data['batch_id']
    await orders_col.update_one(
        {'id': order_id},
        {'$set': {
            'check_status': 'checking',
            'check_batch_id': batch_id,
            'check_submitted_at': datetime.now(timezone.utc).isoformat(),
            'check_status_detail': 'Manually re-submitted by admin',
        }},
    )
    return {'ok': True, 'batch_id': batch_id}


class ManualRefundIn(BaseModel):
    reason: Optional[str] = 'Manually refunded by admin'


@router.post('/orders/{order_id}/refund')
async def admin_refund(order_id: str, body: ManualRefundIn, _: str = Depends(require_admin)):
    order = await orders_col.find_one({'id': order_id}, {'_id': 0})
    if not order:
        raise HTTPException(404, 'order not found')
    if order.get('check_status') == 'refunded':
        raise HTTPException(400, 'already refunded')
    ok = await _refund_order(order, body.reason or 'Manually refunded by admin')
    if not ok:
        raise HTTPException(409, 'refund conflict (already refunded?)')
    return {'ok': True}

