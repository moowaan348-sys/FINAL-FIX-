"""Bot-facing API. Matches the action-based shape used by the original Base44 backend
so the user's existing bot script can drop in with minimal changes.

All requests POST /api/bot/action with a JSON body containing:
  { secret: 'ANDRO', action: '<action_name>', ...params }
"""
from fastapi import APIRouter, HTTPException, Request
from typing import Any, Dict
from datetime import datetime, timezone, timedelta
import logging

from ..config import BOT_SECRET, WALLETS, ADMIN_TG_IDS
from ..db import users_col, lines_col, topups_col, orders_col, settings_col
from ..models import User, Topup, Order
from ..rates import usd_to_crypto

router = APIRouter(prefix='/bot', tags=['bot'])
logger = logging.getLogger('bot_api')


def _check_secret(payload: Dict[str, Any]):
    if payload.get('secret') != BOT_SECRET:
        raise HTTPException(status_code=403, detail='Invalid secret')


async def _get_settings() -> Dict[str, Any]:
    doc = await settings_col.find_one({'_id': 'global'})
    return doc or {}


async def _ensure_user(tg_id: str, username: str = '', first_name: str = '') -> Dict[str, Any]:
    existing = await users_col.find_one({'telegram_user_id': str(tg_id)}, {'_id': 0})
    is_new = False
    if not existing:
        u = User(
            telegram_user_id=str(tg_id),
            telegram_username=username or '',
            first_name=first_name or '',
        ).model_dump()
        await users_col.insert_one(u)
        existing = u
        existing.pop('_id', None)
        is_new = True
    elif username and existing.get('telegram_username') != username:
        await users_col.update_one(
            {'telegram_user_id': str(tg_id)},
            {'$set': {'telegram_username': username}},
        )
    existing['is_new'] = is_new
    return existing


def _strip(doc: Dict[str, Any]) -> Dict[str, Any]:
    if not doc:
        return doc
    doc.pop('_id', None)
    return doc


async def _list_lines(filt: Dict[str, Any], limit: int = 20) -> list:
    filt = {**filt, 'status': 'available'}
    cursor = lines_col.find(filt, {'_id': 0}).sort('created_at', 1).limit(limit)
    out = []
    async for d in cursor:
        out.append(d)
    return out


@router.post('/action')
async def bot_action(req: Request):
    payload = await req.json()
    _check_secret(payload)
    return await handle_action(payload)


async def handle_action(payload: Dict[str, Any]) -> Any:
    """Core bot action dispatcher.

    Pure-async entry point that both the HTTP endpoint above AND the
    standalone bot process (bot/standalone.py) call directly. All
    business logic for the bot lives here.
    """
    action = payload.get('action')

    if action == 'get_balance':
        tg_id = payload.get('telegram_user_id')
        if not tg_id:
            raise HTTPException(400, 'missing telegram_user_id')
        user = await _ensure_user(str(tg_id), payload.get('telegram_username', ''))
        if user.get('banned'):
            return {'balance_usd': 0.0, 'is_new': False, 'banned': True}
        return {
            'balance_usd': float(user.get('balance_usd', 0)),
            'is_new': user.get('is_new', False),
            'banned': False,
        }

    if action == 'notify_new_user':
        tg_id = payload.get('telegram_user_id')
        username = payload.get('telegram_username', '')
        first = payload.get('first_name', '')
        await _ensure_user(str(tg_id), username, first)
        return {'ok': True}

    if action == 'get_available':
        q = payload.get('query') or {}
        limit = int(q.get('limit', 20))
        return await _list_lines({}, limit)

    if action == 'search_bin':
        q = payload.get('query') or {}
        prefix = str(q.get('bin_prefix', '')).strip()
        limit = int(q.get('limit', 20))
        if not prefix:
            return []
        return await _list_lines({'bin': {'$regex': f'^{prefix}'}}, limit)

    if action == 'get_line_preview':
        """Return masked preview of a single line for the pre-purchase
        confirmation screen. Shows bank/scheme/type/level + billing teaser
        (address truncated) + phone/email presence only."""
        line_id = str(payload.get('line_id', '')).strip()
        if not line_id:
            return {'error': 'missing line_id'}
        line = await lines_col.find_one({'id': line_id, 'status': 'available'})
        if not line:
            return {'error': 'not found'}
        from ..country_alias import iso2_to_name
        # Prefer BIN-provider country (card_country), fall back to shipping country
        iso2 = (line.get('card_country') or line.get('country') or '').strip().upper()
        if not (len(iso2) == 2 and iso2.isalpha()):
            iso2 = ''
        country_name = iso2_to_name(iso2) if iso2 else (line.get('country') or '')
        # Truncated address (show first 12 chars)
        addr = (line.get('address') or '').strip()
        addr_preview = (addr[:12] + '...') if len(addr) > 12 else addr
        phone = (line.get('phone') or '').strip()
        email = (line.get('email') or '').strip()
        return {
            'ok': True,
            'id': line.get('id'),
            'bin': line.get('bin', ''),
            'exp_month': line.get('exp_month', ''),
            'exp_year': line.get('exp_year', ''),
            'card_type': (line.get('card_type') or '').upper(),
            'card_level': (line.get('card_level') or '').upper(),
            'card_scheme': (line.get('card_scheme') or '').upper(),
            'bank_name': line.get('bank_name') or '',
            'country_iso2': iso2,
            'country_name': country_name.title() if country_name and country_name.isupper() else country_name,
            'address_preview': addr_preview,
            'city': line.get('city', ''),
            'state': line.get('state', ''),
            'zip': line.get('zip', ''),
            'has_phone': bool(phone),
            'has_email': bool(email),
            'price': float(line.get('price', 5.0)),
            'base_name': line.get('base_name', ''),
        }

    if action == 'bin_search_full':
        """
        Smarter BIN search for the bot: returns matching lines AND the
        issuer/bank info from HandyAPI for BIN prefixes ≥ 6 digits.
        Every search is logged in `bin_search_log` so admins can see what
        BINs people are looking for (demand signal for restocking).
        """
        q = payload.get('query') or {}
        prefix = str(q.get('bin_prefix', '')).strip()
        limit = int(q.get('limit', 20))
        if not prefix:
            return {'lines': [], 'bin_info': {}, 'logged': False}
        caller_id = str(payload.get('caller_telegram_user_id', '')).strip()
        caller_username = str(payload.get('caller_username', '')).strip()
        # Lines
        lines = await _list_lines({'bin': {'$regex': f'^{prefix}'}}, limit)
        # BIN info — only do a lookup if we have ≥6 digits
        bin_info = {}
        if prefix.isdigit() and len(prefix) >= 6:
            try:
                from ..bin_lookup import lookup_bin
                bin_info = await lookup_bin(prefix[:8])  # prefer 8-digit accuracy
            except Exception as _e:
                logger.warning(f'bin_search_full lookup failed: {_e}')
        # Log the search
        try:
            from ..db import bin_search_log_col
            now_iso = datetime.now(timezone.utc).isoformat()
            log_doc = {
                'bin': prefix,
                'telegram_user_id': caller_id,
                'telegram_username': caller_username,
                'found_count': len(lines),
                'bank_name': bin_info.get('bank_name', ''),
                'card_type': bin_info.get('card_type', ''),
                'card_level': bin_info.get('card_level', ''),
                'card_scheme': bin_info.get('card_scheme', ''),
                'card_country': bin_info.get('card_country', ''),
                'searched_at': now_iso,
            }
            await bin_search_log_col.insert_one(log_doc)
            logger.info(
                f"BIN search: {prefix!r} user={caller_username or caller_id} "
                f"found={len(lines)} bank={bin_info.get('bank_name','-')!r}"
            )
            logged = True
        except Exception as _e:
            logger.warning(f'bin_search_full log write failed: {_e}')
            logged = False
        return {'lines': lines, 'bin_info': bin_info, 'logged': logged}

    if action == 'search_country':
        q = payload.get('query') or {}
        country = str(q.get('country', '')).strip()
        limit = int(q.get('limit', 20))
        if not country:
            return []
        from ..country_alias import expand_country_query
        import re as _re
        codes, free_patterns = expand_country_query(country)
        # Match either:
        #   • ISO-2 code on the raw-line country OR the HandyAPI-derived card_country
        #   • free-text partial match against whatever the user typed
        or_clauses = []
        if codes:
            or_clauses.append({'country': {'$in': list(codes)}})
            or_clauses.append({'card_country': {'$in': list(codes)}})
        for pat in free_patterns:
            rx = _re.compile(_re.escape(pat), _re.IGNORECASE)
            or_clauses.append({'country': rx})
        if not or_clauses:
            return []
        return await _list_lines({'$or': or_clauses}, limit)

    if action == 'search_base':
        q = payload.get('query') or {}
        base = str(q.get('base_name', '')).strip()
        limit = int(q.get('limit', 20))
        if not base:
            return []
        return await _list_lines({'base_name': base}, limit)

    if action == 'get_bases':
        # Distinct base names where at least one available line exists
        pipeline = [
            {'$match': {'status': 'available'}},
            {'$group': {'_id': '$base_name', 'count': {'$sum': 1}}},
            {'$sort': {'_id': 1}},
        ]
        bases = []
        async for d in lines_col.aggregate(pipeline):
            if d['_id']:
                bases.append(d['_id'])
        return bases

    if action == 'create_topup':
        tg_id = str(payload.get('telegram_user_id'))
        username = payload.get('telegram_username', '')
        crypto = payload.get('crypto_type')
        amount = float(payload.get('amount_usd', 0))
        await _ensure_user(tg_id, username)
        if crypto not in WALLETS or WALLETS[crypto] in ('0', ''):
            return {'error': f'{crypto} not supported'}
        settings = await _get_settings()
        min_topup = float(settings.get('min_topup_usd', 15.0))
        if amount < min_topup:
            return {'error': f'Minimum top-up is ${min_topup}'}
        fallback_rates = settings.get('crypto_rates', {'USDT_TRC20': 1.0, 'LTC': 70.0})
        crypto_amount = await usd_to_crypto(amount, crypto, fallback_rates)
        topup = Topup(
            telegram_user_id=tg_id,
            telegram_username=username or '',
            crypto_type=crypto,
            amount_usd=amount,
            expected_crypto_amount=crypto_amount,
            wallet_address=WALLETS[crypto],
        ).model_dump()
        await topups_col.insert_one(topup)
        return {
            'id': topup['id'],
            'wallet_address': topup['wallet_address'],
            'expected_crypto_amount': topup['expected_crypto_amount'],
            'amount_usd': topup['amount_usd'],
            'crypto_type': topup['crypto_type'],
            'status': topup['status'],
        }

    if action == 'buy_with_balance':
        tg_id = str(payload.get('telegram_user_id'))
        line_id = payload.get('line_id')
        if not line_id:
            return {'error': 'missing line_id'}
        user = await users_col.find_one({'telegram_user_id': tg_id})
        if not user:
            return {'error': 'user not found'}
        if user.get('banned'):
            return {'error': 'user banned'}
        line = await lines_col.find_one({'id': line_id})
        if not line:
            return {'error': 'line not found'}
        if line.get('status') != 'available':
            return {'error': 'line already sold'}
        price = float(line.get('price', 5.0))
        balance = float(user.get('balance_usd', 0))
        if balance < price:
            return {'error': 'Insufficient balance', 'balance': balance, 'price': price}
        # Atomic sold mark first to prevent double-buy
        now_iso = datetime.now(timezone.utc).isoformat()
        # Set refund-button window (user-initiated refund); no auto-scheduling.
        settings = await _get_settings()
        window_s = int(settings.get('refund_button_window_s', 60))
        window_end = (datetime.now(timezone.utc) + timedelta(seconds=window_s)).isoformat()
        order = Order(
            telegram_user_id=tg_id,
            telegram_username=user.get('telegram_username', '') or '',
            line_id=line_id,
            bin=line.get('bin', ''),
            raw_line=line.get('raw_line', ''),
            price_usd=price,
            check_status='none',
            refund_window_end=window_end,
        ).model_dump()
        res = await lines_col.update_one(
            {'id': line_id, 'status': 'available'},
            {'$set': {
                'status': 'sold',
                'buyer_telegram_user_id': tg_id,
                'order_id': order['id'],
                'sold_at': now_iso,
            }},
        )
        if res.modified_count != 1:
            return {'error': 'line already sold'}
        await orders_col.insert_one(order)
        await users_col.update_one(
            {'telegram_user_id': tg_id},
            {'$inc': {'balance_usd': -price, 'total_spent_usd': price, 'orders_count': 1}},
        )
        new_balance = balance - price
        # Admin notification
        try:
            from ..notifications import push as notif_push
            await notif_push('purchase', {
                'order_id': order['id'],
                'telegram_user_id': tg_id,
                'telegram_username': user.get('telegram_username', '') or '',
                'bin': line.get('bin', ''),
                'base_name': line.get('base_name', ''),
                'price_usd': price,
                'new_balance': new_balance,
                'raw_line': line.get('raw_line', ''),
            })
        except Exception as _e:
            logger.warning(f'purchase notif push failed: {_e}')
        return {
            'order_id': order['id'],
            'raw_line': line.get('raw_line', ''),
            'new_balance': new_balance,
            'price': price,
            'refund_window_end': window_end,
            'refund_window_s': window_s,
            'refund_checker_fee_usd': float(settings.get('refund_checker_fee_usd', 1.0)),
        }

    if action == 'admin_credit_user':
        # Bot-initiated balance credit from any admin (multi-admin).
        # Secured by: BOT_SECRET + caller must be in ADMIN_TG_IDS.
        caller_id = str(payload.get('caller_telegram_user_id', ''))
        if not caller_id.isdigit() or int(caller_id) not in ADMIN_TG_IDS:
            return {'error': 'not admin'}
        target = str(payload.get('target_telegram_user_id', ''))
        try:
            amount = float(payload.get('amount_usd', 0))
        except Exception:
            return {'error': 'bad amount'}
        if not target or amount == 0:
            return {'error': 'missing target or amount'}
        user = await users_col.find_one({'telegram_user_id': target})
        if not user:
            return {'error': 'user not found'}
        await users_col.update_one(
            {'telegram_user_id': target},
            {'$inc': {'balance_usd': amount}},
        )
        updated = await users_col.find_one({'telegram_user_id': target}, {'_id': 0})
        return {
            'ok': True,
            'new_balance': float(updated.get('balance_usd', 0)),
            'target_telegram_user_id': target,
            'telegram_username': updated.get('telegram_username', ''),
            'amount_credited': amount,
        }

    # ---- Admin bot actions (BOT_SECRET + caller must be in ADMIN_TG_IDS) ----
    def _is_admin() -> bool:
        cid = str(payload.get('caller_telegram_user_id', ''))
        return cid.isdigit() and int(cid) in ADMIN_TG_IDS

    if action == 'admin_bulk_upload':
        if not _is_admin():
            return {'error': 'not admin'}
        from ..parser import parse_bulk_lines
        from ..bin_lookup import enrich_records
        from ..models import Line
        from ..db import bases_col
        base_name = str(payload.get('base_name', 'default')).strip() or 'default'
        try:
            price = float(payload.get('price', 5.0))
        except Exception:
            return {'error': 'bad price'}
        if price <= 0:
            return {'error': 'price must be > 0'}
        text = payload.get('text', '') or ''
        skip_dup = bool(payload.get('skip_duplicates', True))
        records, errors = parse_bulk_lines(text)
        # Auto-enrich BIN metadata via HandyAPI before insert. Each BIN
        # is looked up at most once per process thanks to Mongo caching.
        try:
            enriched_count = await enrich_records(records)
        except Exception as _e:
            logger.warning(f'bin enrichment failed: {_e}')
            enriched_count = 0
        # Block dedupe_keys that are known-dead (prevent resale)
        known_dead = set()
        refunded_line_ids = []
        async for d in orders_col.find({'check_status': 'refunded'}, {'line_id': 1, '_id': 0}):
            if d.get('line_id'):
                refunded_line_ids.append(d['line_id'])
        if refunded_line_ids:
            async for d in lines_col.find({'id': {'$in': refunded_line_ids}}, {'dedupe_key': 1, '_id': 0}):
                if d.get('dedupe_key'):
                    known_dead.add(d['dedupe_key'])
        inserted = 0
        duplicates = 0
        dead_blocked = 0
        for rec in records:
            if rec['dedupe_key'] in known_dead:
                dead_blocked += 1
                continue
            if skip_dup:
                existing = await lines_col.find_one({'dedupe_key': rec['dedupe_key']}, {'_id': 1})
                if existing:
                    duplicates += 1
                    continue
            line = Line(**rec, base_name=base_name, price=price).model_dump()
            try:
                await lines_col.insert_one(line)
                inserted += 1
            except Exception as e:
                errors.append({'reason': str(e), 'raw': rec.get('raw_line', '')})
        await bases_col.update_one(
            {'name': base_name},
            {'$setOnInsert': {'name': base_name, 'created_at': datetime.now(timezone.utc).isoformat()}},
            upsert=True,
        )
        return {
            'ok': True,
            'parsed': len(records),
            'inserted': inserted,
            'duplicates': duplicates,
            'dead_blocked': dead_blocked,
            'errors_count': len(errors),
        }

    if action == 'admin_list_bases':
        if not _is_admin():
            return {'error': 'not admin'}
        pipeline = [
            {'$group': {
                '_id': '$base_name',
                'total': {'$sum': 1},
                'available': {'$sum': {'$cond': [{'$eq': ['$status', 'available']}, 1, 0]}},
                'sold': {'$sum': {'$cond': [{'$eq': ['$status', 'sold']}, 1, 0]}},
                'price': {'$first': '$price'},
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
                'price': float(d.get('price') or 0),
            })
        return out

    if action == 'admin_update_base':
        if not _is_admin():
            return {'error': 'not admin'}
        base_name = str(payload.get('base_name', '')).strip()
        if not base_name:
            return {'error': 'missing base_name'}
        updates = {}
        new_name = payload.get('new_name')
        new_price = payload.get('new_price')
        if new_name is not None:
            nn = str(new_name).strip()
            if not nn:
                return {'error': 'empty new_name'}
            updates['base_name'] = nn
        if new_price is not None:
            try:
                np = float(new_price)
            except Exception:
                return {'error': 'bad new_price'}
            if np <= 0:
                return {'error': 'new_price must be > 0'}
            updates['price'] = np
        if not updates:
            return {'error': 'nothing to update'}
        # Only update available lines' price? Update ALL lines so historical data shows updated base name, but only available lines get new price.
        if 'base_name' in updates:
            await lines_col.update_many({'base_name': base_name}, {'$set': {'base_name': updates['base_name']}})
        if 'price' in updates:
            target_name = updates.get('base_name', base_name)
            await lines_col.update_many(
                {'base_name': target_name, 'status': 'available'},
                {'$set': {'price': updates['price']}},
            )
        return {'ok': True, 'applied': updates}

    if action == 'admin_delete_base':
        if not _is_admin():
            return {'error': 'not admin'}
        base_name = str(payload.get('base_name', '')).strip()
        if not base_name:
            return {'error': 'missing base_name'}
        # Only delete AVAILABLE lines in the base (never touch sold/order history)
        res = await lines_col.delete_many({'base_name': base_name, 'status': 'available'})
        return {'ok': True, 'deleted': res.deleted_count}

    if action == 'admin_export_base_unsold':
        """Return every AVAILABLE line in a base as raw pipe strings for .txt export."""
        if not _is_admin():
            return {'error': 'not admin'}
        base_name = str(payload.get('base_name', '')).strip()
        if not base_name:
            return {'error': 'missing base_name'}
        items = []
        cursor = lines_col.find(
            {'base_name': base_name, 'status': 'available'},
            {'_id': 0, 'raw_line': 1, 'bin': 1, 'country': 1,
             'card_type': 1, 'card_level': 1, 'card_scheme': 1,
             'card_country': 1, 'price': 1, 'created_at': 1}
        ).sort('created_at', 1)
        async for d in cursor:
            items.append(d)
        return {
            'ok': True,
            'base_name': base_name,
            'count': len(items),
            'items': items,
        }

    if action == 'admin_destroy_base':
        """COMPLETELY delete a base: every line (available + sold) AND the base record.
        Order history is preserved (orders have their own copy of raw_line)."""
        if not _is_admin():
            return {'error': 'not admin'}
        from ..db import bases_col
        base_name = str(payload.get('base_name', '')).strip()
        if not base_name:
            return {'error': 'missing base_name'}
        confirm = str(payload.get('confirm', '')).strip()
        if confirm != base_name:
            return {'error': 'confirm-name mismatch'}
        avail = await lines_col.count_documents({'base_name': base_name, 'status': 'available'})
        sold = await lines_col.count_documents({'base_name': base_name, 'status': {'$ne': 'available'}})
        res = await lines_col.delete_many({'base_name': base_name})
        base_res = await bases_col.delete_one({'name': base_name})
        logger.info(
            f'admin_destroy_base: base={base_name!r} lines_deleted={res.deleted_count} '
            f'available={avail} sold={sold} base_record_removed={base_res.deleted_count}'
        )
        return {
            'ok': True,
            'base_name': base_name,
            'lines_deleted': res.deleted_count,
            'available_deleted': avail,
            'sold_deleted': sold,
            'base_record_removed': base_res.deleted_count,
        }

    if action == 'admin_enrich_bins':
        """Back-fill BIN info on existing lines that don't yet have card_type."""
        if not _is_admin():
            return {'error': 'not admin'}
        from ..bin_lookup import lookup_bin
        max_lines = int(payload.get('max_lines', 5000))
        pipeline = [
            {'$match': {
                'status': 'available',
                '$or': [
                    {'card_type': {'$exists': False}},
                    {'card_type': ''},
                ],
            }},
            {'$group': {'_id': '$bin', 'count': {'$sum': 1}}},
            {'$limit': 1000},
        ]
        unique_bins = []
        async for d in lines_col.aggregate(pipeline):
            if d.get('_id'):
                unique_bins.append(d['_id'])
        if not unique_bins:
            return {'ok': True, 'enriched_bins': 0, 'updated_lines': 0,
                    'message': 'nothing to enrich'}
        updated_lines_total = 0
        enriched_bins = 0
        failed_bins = 0
        for bin6 in unique_bins:
            info = await lookup_bin(bin6)
            if not info:
                failed_bins += 1
                continue
            res = await lines_col.update_many(
                {
                    'bin': bin6,
                    'status': 'available',
                    '$or': [{'card_type': {'$exists': False}}, {'card_type': ''}],
                },
                {'$set': info},
            )
            updated_lines_total += res.modified_count
            enriched_bins += 1
            if updated_lines_total >= max_lines:
                break
        return {
            'ok': True,
            'enriched_bins': enriched_bins,
            'failed_bins': failed_bins,
            'updated_lines': updated_lines_total,
            'total_unique_bins_pending': len(unique_bins),
        }

    if action == 'admin_export_bin_searches':
        """Return the BIN search log (most recent first) for the admin to download."""
        if not _is_admin():
            return {'error': 'not admin'}
        from ..db import bin_search_log_col
        limit = int(payload.get('limit', 2000))
        items = []
        cursor = bin_search_log_col.find({}, {'_id': 0}).sort('searched_at', -1).limit(limit)
        async for d in cursor:
            items.append(d)
        total = await bin_search_log_col.count_documents({})
        pipe = [
            {'$group': {'_id': '$bin', 'count': {'$sum': 1},
                        'bank': {'$first': '$bank_name'},
                        'country': {'$first': '$card_country'},
                        'type': {'$first': '$card_type'},
                        'level': {'$first': '$card_level'}}},
            {'$sort': {'count': -1}},
            {'$limit': 50},
        ]
        top = []
        async for d in bin_search_log_col.aggregate(pipe):
            top.append(d)
        return {'total': total, 'items': items, 'top_bins': top}

    if action == 'admin_get_welcome':
        if not _is_admin():
            return {'error': 'not admin'}
        doc = await settings_col.find_one({'_id': 'global'}) or {}
        return {'welcome_message': doc.get('welcome_message', '')}

    if action == 'admin_set_welcome':
        if not _is_admin():
            return {'error': 'not admin'}
        msg = payload.get('welcome_message', '')
        if not isinstance(msg, str):
            return {'error': 'welcome_message must be string'}
        await settings_col.update_one(
            {'_id': 'global'},
            {'$set': {'welcome_message': msg}},
            upsert=True,
        )
        return {'ok': True}

    if action == 'admin_get_all_user_ids':
        if not _is_admin():
            return {'error': 'not admin'}
        cursor = users_col.find({'banned': {'$ne': True}}, {'telegram_user_id': 1, '_id': 0})
        ids = []
        async for d in cursor:
            if d.get('telegram_user_id'):
                ids.append(str(d['telegram_user_id']))
        return {'ok': True, 'user_ids': ids}

    if action == 'admin_list_users':
        if not _is_admin():
            return {'error': 'not admin'}
        q = str(payload.get('q', '')).strip()
        skip = int(payload.get('skip', 0))
        limit = min(int(payload.get('limit', 15)), 50)
        filt: Dict[str, Any] = {}
        if q:
            import re as _re
            rg = _re.escape(q)
            filt['$or'] = [
                {'telegram_user_id': {'$regex': rg, '$options': 'i'}},
                {'telegram_username': {'$regex': rg, '$options': 'i'}},
                {'first_name': {'$regex': rg, '$options': 'i'}},
            ]
        total = await users_col.count_documents(filt)
        cursor = users_col.find(filt, {'_id': 0}).sort('created_at', -1).skip(skip).limit(limit)
        items = [d async for d in cursor]
        return {'ok': True, 'items': items, 'total': total}

    if action == 'admin_user_detail':
        if not _is_admin():
            return {'error': 'not admin'}
        tg_id = str(payload.get('target_telegram_user_id', ''))
        user = await users_col.find_one({'telegram_user_id': tg_id}, {'_id': 0})
        if not user:
            return {'error': 'user not found'}
        orders = [d async for d in orders_col.find({'telegram_user_id': tg_id}, {'_id': 0}).sort('created_at', -1).limit(10)]
        topups = [d async for d in topups_col.find({'telegram_user_id': tg_id}, {'_id': 0}).sort('created_at', -1).limit(10)]
        return {'ok': True, 'user': user, 'orders': orders, 'topups': topups}

    if action == 'admin_toggle_ban':
        if not _is_admin():
            return {'error': 'not admin'}
        tg_id = str(payload.get('target_telegram_user_id', ''))
        user = await users_col.find_one({'telegram_user_id': tg_id}, {'_id': 0})
        if not user:
            return {'error': 'user not found'}
        new_val = not bool(user.get('banned', False))
        await users_col.update_one({'telegram_user_id': tg_id}, {'$set': {'banned': new_val}})
        return {'ok': True, 'banned': new_val}

    if action == 'admin_refund_order':
        if not _is_admin():
            return {'error': 'not admin'}
        from ..refund import _refund_order
        order_id = payload.get('order_id')
        reason = payload.get('reason') or 'Manually refunded by admin (bot)'
        order = await orders_col.find_one({'id': order_id}, {'_id': 0})
        if not order:
            return {'error': 'order not found'}
        if order.get('check_status') == 'refunded':
            return {'error': 'already refunded'}
        ok = await _refund_order(order, reason, refund_fee_too=False, fee=0.0)
        if not ok:
            return {'error': 'refund conflict'}
        user = await users_col.find_one({'telegram_user_id': order['telegram_user_id']}, {'_id': 0})
        return {
            'ok': True,
            'refund_amount_usd': float(order.get('price_usd', 0)),
            'new_balance': float((user or {}).get('balance_usd', 0)),
            'target_telegram_user_id': order['telegram_user_id'],
        }

    if action == 'admin_fetch_notifications':
        if not _is_admin():
            return {'error': 'not admin'}
        from ..notifications import fetch_undelivered, mark_delivered, cleanup_old
        caller = int(payload.get('caller_telegram_user_id', 0))
        items = await fetch_undelivered(caller, limit=20)
        # Don't auto-mark here; bot explicitly marks after successfully sending.
        return {'ok': True, 'items': items}

    if action == 'admin_mark_notification_delivered':
        if not _is_admin():
            return {'error': 'not admin'}
        from ..notifications import mark_delivered
        caller = int(payload.get('caller_telegram_user_id', 0))
        notif_id = payload.get('notification_id')
        if not notif_id:
            return {'error': 'missing notification_id'}
        await mark_delivered(notif_id, caller)
        return {'ok': True}

    if action == 'get_welcome':
        # Public (bot uses this on /start). Only returns template text, no user data.
        doc = await settings_col.find_one({'_id': 'global'}) or {}
        return {'welcome_message': doc.get('welcome_message', '')}

    if action == 'my_orders':
        tg_id = str(payload.get('telegram_user_id'))
        # Retention: only return orders from the last 10 days.
        cutoff = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
        cursor = orders_col.find(
            {'telegram_user_id': tg_id, 'created_at': {'$gte': cutoff}},
            {'_id': 0},
        ).sort('created_at', -1).limit(500)
        out = []
        async for d in cursor:
            d['created_date'] = d.get('created_at', '')
            out.append(d)
        return out

    if action == 'request_refund_check':
        from ..refund import storm_submit, _card_string
        tg_id = str(payload.get('telegram_user_id'))
        order_id = payload.get('order_id')
        order = await orders_col.find_one({'id': order_id})
        if not order:
            return {'error': 'order not found'}
        if order.get('telegram_user_id') != tg_id:
            return {'error': 'not your order'}
        # Enforce 60s window
        window_end = order.get('refund_window_end')
        now = datetime.now(timezone.utc)
        if window_end:
            we = datetime.fromisoformat(window_end)
            if now > we:
                return {'error': 'refund window expired', 'expired': True}
        # Can't double-check
        if order.get('check_status') in ('checking', 'live', 'dead', 'refunded', 'error', 'timeout'):
            return {'error': 'check already performed', 'already': True, 'status': order.get('check_status')}
        # Charge $1 fee
        settings = await _get_settings()
        fee = float(settings.get('refund_checker_fee_usd', 1.0))
        user = await users_col.find_one({'telegram_user_id': tg_id})
        if not user or float(user.get('balance_usd', 0)) < fee:
            return {'error': 'insufficient balance for checker fee', 'required': fee}
        # Deduct fee
        await users_col.update_one(
            {'telegram_user_id': tg_id},
            {'$inc': {'balance_usd': -fee}},
        )
        # Build card string (auto-detects combined 'mm/yy' or split 'mm|yy' formats).
        card = _card_string(order)
        if not card:
            # refund the fee, fail
            await users_col.update_one({'telegram_user_id': tg_id}, {'$inc': {'balance_usd': fee}})
            return {'error': 'Could not parse card fields from this order (expiry/CVV). $1 fee refunded.'}
        # Submit
        data = await storm_submit([card])
        # Storm reachable but rejected the card (bad format, expired, etc.)
        if isinstance(data, dict) and data.get('_rejected'):
            # Refund the fee — user shouldn't be charged if Storm refused to check
            await users_col.update_one({'telegram_user_id': tg_id}, {'$inc': {'balance_usd': fee}})
            # Pull a human-readable rejection reason
            reasons: list[str] = []
            for item in (data.get('rejected_details') or []):
                errs = item.get('errors') or []
                if isinstance(errs, list):
                    reasons.extend(str(e) for e in errs)
            if not reasons and data.get('error_message'):
                reasons.append(str(data['error_message']))
            reason_txt = '; '.join(reasons[:3]) or 'Card rejected by checker'
            return {
                'error': f'Checker rejected this card: {reason_txt}. $1 fee refunded.',
                'rejected': True,
                'reasons': reasons,
            }
        # Real network / 5xx failure
        if not data or not data.get('batch_id'):
            # Fee refund on submit failure
            await users_col.update_one({'telegram_user_id': tg_id}, {'$inc': {'balance_usd': fee}})
            return {'error': 'Storm API unreachable; $1 fee refunded'}
        batch_id = data['batch_id']
        await orders_col.update_one(
            {'id': order_id},
            {'$set': {
                'check_status': 'checking',
                'check_batch_id': batch_id,
                'check_submitted_at': now.isoformat(),
                'checker_fee_paid': True,
                'check_status_detail': 'User-initiated check',
            }},
        )
        # Return immediately; bot polls via get_refund_status
        new_bal = await users_col.find_one({'telegram_user_id': tg_id})
        return {
            'ok': True,
            'batch_id': batch_id,
            'status': 'checking',
            'new_balance': float(new_bal.get('balance_usd', 0)) if new_bal else 0.0,
            'fee_charged': fee,
        }

    if action == 'get_refund_status':
        tg_id = str(payload.get('telegram_user_id'))
        order_id = payload.get('order_id')
        order = await orders_col.find_one({'id': order_id}, {'_id': 0})
        if not order or order.get('telegram_user_id') != tg_id:
            return {'error': 'order not found'}
        user = await users_col.find_one({'telegram_user_id': tg_id})
        return {
            'check_status': order.get('check_status', 'none'),
            'check_status_detail': order.get('check_status_detail', ''),
            'check_approval_code': order.get('check_approval_code', ''),
            'refund_amount_usd': order.get('refund_amount_usd', 0),
            'balance_usd': float((user or {}).get('balance_usd', 0)),
            'price_usd': order.get('price_usd', 0),
        }

    raise HTTPException(status_code=400, detail=f'Unknown action: {action}')
