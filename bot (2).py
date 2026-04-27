"""
BIN (Bank Identification Number) lookup — HandyAPI edition.

Primary provider: HandyAPI (https://www.handyapi.com/) — 1000 req/day free,
                  weekly database updates, 3M+ BINs, accurate CardTier field.
Fallback       : iinlist.com public JSON (no key, best-effort).

Caching: every BIN is stored forever in the `bin_cache` Mongo collection.
Negative lookups are cached for 24h so we don't retry flaky BINs repeatedly.

Usage::

    from app.bin_lookup import lookup_bin, enrich_records
    info = await lookup_bin('530079')
    # -> {'card_type': 'DEBIT', 'card_level': 'ENHANCED',
    #     'card_scheme': 'MASTERCARD', 'card_brand': 'DEBIT MASTERCARD (ENHANCED)',
    #     'bank_name': 'MORGAN STANLEY PRIVATE BANK NA',
    #     'card_country': 'US'}
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional

import httpx

from .db import db

logger = logging.getLogger('bin_lookup')

bin_cache_col = db['bin_cache']

# Keep the negative TTL short-ish — provider data improves weekly.
_NEGATIVE_TTL = timedelta(hours=24)

HANDYAPI_KEY = (os.environ.get('HANDYAPI_KEY') or '').strip()
# Alternate env names people might use
if not HANDYAPI_KEY:
    HANDYAPI_KEY = (os.environ.get('HANDY_API_KEY') or '').strip()

# Levels we look for inside free-text brand/tier strings as a fallback
# when the provider doesn't return a separate "CardTier" level.
_LEVEL_KEYWORDS = (
    'WORLD ELITE', 'INFINITE', 'SIGNATURE', 'PLATINUM',
    'BUSINESS', 'CORPORATE', 'PURCHASING',
    'WORLD', 'BLACK', 'TITANIUM', 'GOLD',
    'SILVER', 'CLASSIC', 'STANDARD', 'ELECTRON',
    'MAESTRO', 'PREPAID', 'REWARDS', 'CASHBACK',
    'ENHANCED', 'BASIC',
)


def _extract_level(*candidates: str) -> str:
    """Pick the first readable level keyword found in any of the strings."""
    for raw in candidates:
        if not raw:
            continue
        up = raw.upper()
        for kw in _LEVEL_KEYWORDS:
            if kw in up:
                return kw
    return ''


def _shape_from_handyapi(data: Dict) -> Dict[str, str]:
    """Convert HandyAPI response → our normalized shape."""
    if (data.get('Status') or '').upper() != 'SUCCESS':
        return {}
    ctype = (data.get('Type') or '').upper()
    if ctype == 'DEFERRED DEBIT' or ctype == 'DEFERRED_DEBIT':
        ctype = 'DEBIT'
    scheme = (data.get('Scheme') or '').upper()
    tier = (data.get('CardTier') or '').strip()
    issuer = (data.get('Issuer') or '').strip()
    country = ((data.get('Country') or {}).get('A2') or '').upper()
    level = _extract_level(tier, scheme)
    return {
        'card_type': ctype,
        'card_level': level,
        'card_scheme': scheme,
        'card_brand': tier or f'{scheme} {ctype}'.strip(),
        'bank_name': issuer.title() if issuer else '',
        'card_country': country,
    }


def _shape_from_iinlist(data: Dict) -> Dict[str, str]:
    """Convert iinlist.com response → our normalized shape."""
    ctype = (data.get('type') or '').upper()
    scheme = (data.get('scheme') or '').upper()
    brand = (data.get('brand') or '').strip()
    bank = (data.get('bank') or {}).get('name') if isinstance(data.get('bank'), dict) else (data.get('bank') or '')
    country = (data.get('country') or {}).get('alpha2') if isinstance(data.get('country'), dict) else ''
    if not (ctype or scheme):
        return {}
    return {
        'card_type': ctype,
        'card_level': _extract_level(brand),
        'card_scheme': scheme,
        'card_brand': brand,
        'bank_name': (bank or '').title() if isinstance(bank, str) else '',
        'card_country': (country or '').upper(),
    }


async def _fetch_handyapi(bin_input: str) -> Optional[Dict[str, str]]:
    if not HANDYAPI_KEY:
        return None
    url = f'https://data.handyapi.com/bin/{bin_input}'
    try:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.get(url, headers={'x-api-key': HANDYAPI_KEY})
        if r.status_code == 200:
            shaped = _shape_from_handyapi(r.json())
            return shaped or None
        if r.status_code in (401, 403):
            logger.warning('HandyAPI auth failed (%s): %s', r.status_code, r.text[:120])
        elif r.status_code == 429:
            logger.warning('HandyAPI rate-limited — falling back')
        else:
            logger.debug('HandyAPI %s for %s: %s', r.status_code, bin_input, r.text[:120])
        return None
    except Exception as e:  # noqa: BLE001
        logger.debug('HandyAPI error for %s: %s', bin_input, e)
        return None


async def _fetch_iinlist(bin_input: str) -> Optional[Dict[str, str]]:
    url = f'https://iinlist.com/api/v2/iin/{bin_input}'
    try:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.get(url)
        if r.status_code == 200:
            shaped = _shape_from_iinlist(r.json())
            return shaped or None
        return None
    except Exception as e:  # noqa: BLE001
        logger.debug('iinlist error for %s: %s', bin_input, e)
        return None


async def _provider_chain(bin_input: str) -> Optional[Dict[str, str]]:
    # Primary: HandyAPI (with API key)
    data = await _fetch_handyapi(bin_input)
    if data:
        return data
    # Fallback: public iinlist (no key required)
    data = await _fetch_iinlist(bin_input)
    return data


_BIN_RE = re.compile(r'^\d{6,8}$')


async def lookup_bin(bin_input: str) -> Dict[str, str]:
    """Return enrichment dict for a BIN (6-8 digits). Never raises."""
    bin_input = str(bin_input or '').strip()
    # Prefer an 8-digit BIN when available, but accept 6.
    if not _BIN_RE.match(bin_input):
        # Try trimming to the longest valid prefix
        candidate = bin_input[:8] if len(bin_input) >= 8 else bin_input[:6]
        if not _BIN_RE.match(candidate):
            return {}
        bin_input = candidate

    now = datetime.now(timezone.utc)
    cached = await bin_cache_col.find_one({'_id': bin_input})
    if cached:
        if cached.get('found'):
            return {k: v for k, v in cached.items()
                    if k in ('card_type', 'card_level', 'card_scheme',
                             'card_brand', 'bank_name', 'card_country')}
        # Stale negative — retry if older than TTL
        checked_at = cached.get('checked_at')
        if checked_at:
            try:
                ts = datetime.fromisoformat(checked_at)
                if now - ts < _NEGATIVE_TTL:
                    return {}
            except Exception:
                pass

    data = await _provider_chain(bin_input)
    doc = {
        '_id': bin_input,
        'checked_at': now.isoformat(),
        'found': bool(data),
    }
    if data:
        doc.update(data)
    try:
        await bin_cache_col.replace_one({'_id': bin_input}, doc, upsert=True)
    except Exception as e:  # noqa: BLE001
        logger.debug('bin_cache write failed for %s: %s', bin_input, e)
    return data or {}


async def enrich_records(records, throttle: float = 0.05) -> int:
    """Enrich a list of parsed-line dicts (mutates in place). Returns count enriched."""
    seen_cache: Dict[str, Dict[str, str]] = {}
    enriched = 0
    for rec in records:
        bin6 = rec.get('bin') or (rec.get('number') or '')[:6]
        if not bin6:
            continue
        if bin6 in seen_cache:
            info = seen_cache[bin6]
        else:
            info = await lookup_bin(bin6)
            seen_cache[bin6] = info
            # Small stagger only when we hit the network (DB hits are instant)
            await asyncio.sleep(throttle)
        if info:
            rec.update(info)
            enriched += 1
    return enriched


__all__ = ['lookup_bin', 'enrich_records', 'bin_cache_col', 'HANDYAPI_KEY']
