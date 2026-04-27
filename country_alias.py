"""Notifications queue for admin DMs from the Telegram bot.

Backend code inserts a notification here. The bot polls periodically and
delivers each notification to each admin Telegram ID, tracking delivery
state on the record so it never double-sends.
"""
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List

from .db import notifications_col


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def push(event_type: str, payload: Dict[str, Any]) -> str:
    doc = {
        'id': str(uuid.uuid4()),
        'event_type': event_type,
        'payload': payload,
        'created_at': _now_iso(),
        'delivered_to': [],   # list of admin_id ints
    }
    await notifications_col.insert_one(doc)
    return doc['id']


async def fetch_undelivered(admin_id: int, limit: int = 20) -> List[Dict[str, Any]]:
    cursor = notifications_col.find(
        {'delivered_to': {'$ne': int(admin_id)}},
        {'_id': 0},
    ).sort('created_at', 1).limit(limit)
    return [d async for d in cursor]


async def mark_delivered(notif_id: str, admin_id: int) -> None:
    await notifications_col.update_one(
        {'id': notif_id},
        {'$addToSet': {'delivered_to': int(admin_id)}},
    )


async def cleanup_old(days: int = 7) -> int:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    res = await notifications_col.delete_many({'created_at': {'$lt': cutoff}})
    return res.deleted_count
