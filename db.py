import os

# Wallets (set via env for safety, with defaults from user).
WALLETS = {
    'USDT_TRC20': os.environ.get('USDT_TRC20_WALLET', 'TCGjtfZnsWt3JDccm3Y1uk2QvLmvM3Yt2x'),
    'LTC': os.environ.get('LTC_WALLET', 'Lak56Y1JhwiW26YwcnXdgMSEMDjSUgp7PB'),
    'BTC': os.environ.get('BTC_WALLET', '0'),
    'ETH': os.environ.get('ETH_WALLET', '0'),
}

USDT_TRC20_CONTRACT = 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t'

BOT_SECRET = os.environ.get('BOT_SECRET', 'ANDRO')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '8655880432:AAGHBgdEXkgWlBwneWseXjhtDiJZvrfutjk')

# Comma-separated list of Telegram user IDs that have admin privileges in the bot.
_admin_raw = os.environ.get('ADMIN_TG_IDS') or os.environ.get('ADMIN_TG_ID') or '8295276273,8798542436'
ADMIN_TG_IDS = [int(x.strip()) for x in _admin_raw.split(',') if x.strip().isdigit()]
# Backwards-compat alias — first admin is the "primary"
ADMIN_TG_ID = ADMIN_TG_IDS[0] if ADMIN_TG_IDS else 0

DEFAULT_ADMIN_USERNAME = os.environ.get('ADMIN_USERNAME', 'admin')
DEFAULT_ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'ChangeMe123!')

DEFAULT_SETTINGS = {
    'min_topup_usd': 15.0,
    'default_price_usd': 5.0,
    'confirmations_required': 3,
    'amount_tolerance_pct': 1.5,  # % tolerance when matching crypto amount
    'auto_refund_enabled': False,  # Off — we use user-initiated refund button now
    'auto_refund_delay_s': 120,
    'refund_button_window_s': 60,   # Button visible/valid for this long after purchase
    'refund_checker_fee_usd': 1.0,  # Charged when user clicks refund
    'crypto_rates': {
        # rough fallback rates; watcher will refresh from coingecko
        'USDT_TRC20': 1.0,
        'LTC': 70.0,
        'BTC': 60000.0,
        'ETH': 2500.0,
    },
}
