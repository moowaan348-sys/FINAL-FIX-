from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Any, Dict
from datetime import datetime, timezone
import uuid


def _uuid() -> str:
    return str(uuid.uuid4())


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class User(BaseModel):
    model_config = ConfigDict(extra='ignore')
    id: str = Field(default_factory=_uuid)
    telegram_user_id: str
    telegram_username: Optional[str] = ''
    first_name: Optional[str] = ''
    balance_usd: float = 0.0
    total_spent_usd: float = 0.0
    orders_count: int = 0
    banned: bool = False
    created_at: str = Field(default_factory=_now_iso)


class Line(BaseModel):
    model_config = ConfigDict(extra='ignore')
    id: str = Field(default_factory=_uuid)
    number: str
    bin: str
    exp_month: str
    exp_year: str
    cvv: str
    name: str = ''
    address: str = ''
    city: str = ''
    state: str = ''
    zip: str = ''
    country: str = ''
    phone: str = ''
    email: str = ''
    raw_line: str = ''
    base_name: str = 'default'
    price: float = 5.0
    status: str = 'available'  # available | sold
    buyer_telegram_user_id: Optional[str] = None
    order_id: Optional[str] = None
    sold_at: Optional[str] = None
    dedupe_key: Optional[str] = None
    # BIN-lookup enrichment (populated by bin_lookup.py, HandyAPI primary)
    card_type: str = ''        # CREDIT | DEBIT | PREPAID | ''
    card_level: str = ''       # PLATINUM | GOLD | CLASSIC | BUSINESS | ''
    card_scheme: str = ''      # VISA | MASTERCARD | AMEX | ...
    card_brand: str = ''       # provider's CardTier / brand string
    bank_name: str = ''
    card_country: str = ''     # ISO-2 from BIN provider (may differ from shipping country)
    created_at: str = Field(default_factory=_now_iso)


class Topup(BaseModel):
    model_config = ConfigDict(extra='ignore')
    id: str = Field(default_factory=_uuid)
    telegram_user_id: str
    telegram_username: Optional[str] = ''
    crypto_type: str  # USDT_TRC20 | LTC | BTC | ETH
    amount_usd: float
    expected_crypto_amount: float
    wallet_address: str
    status: str = 'pending'  # pending | confirmed | failed | manual
    tx_hash: Optional[str] = None
    confirmations: int = 0
    actual_crypto_received: Optional[float] = None
    confirmed_at: Optional[str] = None
    created_at: str = Field(default_factory=_now_iso)
    note: Optional[str] = ''


class Order(BaseModel):
    model_config = ConfigDict(extra='ignore')
    id: str = Field(default_factory=_uuid)
    telegram_user_id: str
    telegram_username: Optional[str] = ''
    line_id: str
    bin: str
    raw_line: str
    price_usd: float
    created_at: str = Field(default_factory=_now_iso)
    # Storm auto-refund fields
    check_status: str = 'none'  # none|pending|checking|live|dead|refunded|error|timeout|skipped
    scheduled_check_at: Optional[str] = None
    check_submitted_at: Optional[str] = None
    check_batch_id: Optional[str] = None
    check_status_detail: Optional[str] = None
    check_approval_code: Optional[str] = None
    refunded_at: Optional[str] = None
    refund_amount_usd: Optional[float] = None
    checker_fee_paid: bool = False  # True if user/admin paid the $1 fee (user-initiated refund)
    refund_window_end: Optional[str] = None


class AdminUser(BaseModel):
    model_config = ConfigDict(extra='ignore')
    id: str = Field(default_factory=_uuid)
    username: str
    password_hash: str
    created_at: str = Field(default_factory=_now_iso)
