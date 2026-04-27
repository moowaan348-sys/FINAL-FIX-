# ═══════════════════════════════════════════════════════════════
#   DataLine Bot — environment template
# ═══════════════════════════════════════════════════════════════
#
# Copy this file to `.env` for local runs. On Railway, set these
# as **Variables** in the service settings (Railway ignores .env
# files, it uses its own dashboard variables).
#
# Values marked ⚠ REQUIRED — the bot won't start without them.
# Everything else is optional.
# ═══════════════════════════════════════════════════════════════

# ─── Required ─────────────────────────────────────────────────

# ⚠ Tells the boot script to launch the bot (not the FastAPI web server).
MODE=bot
# ⚠ Tells the bot to talk to MongoDB directly (no separate API server).
BOT_STANDALONE=1

# ⚠ Your Telegram bot token from @BotFather.
BOT_TOKEN=123456789:AA-YourTokenHere

# ⚠ Comma-separated Telegram user IDs of your admins.
# Tip: DM @userinfobot to find your own ID.
ADMIN_TG_IDS=123456789

# ⚠ MongoDB connection string.
#    Cloud (recommended):  mongodb+srv://user:pass@cluster0.xxx.mongodb.net
#    Railway plugin auto-injects MONGO_URL — leave as-is if you added it.
MONGO_URL=mongodb+srv://USER:PASS@cluster0.xxx.mongodb.net
DB_NAME=dataline_store


# ─── Checker / BIN API (optional but recommended) ─────────────

# Storm.gift API key — powers the user-initiated 60-second refund checker.
# Leave blank to disable the refund-check feature.
STORM_API_KEY=

# HandyAPI key — enriches stock uploads with bank / level / scheme data.
# Leave blank to skip BIN enrichment.
HANDYAPI_KEY=


# ─── Crypto wallets (your top-up destinations) ────────────────

USDT_TRC20_WALLET=TCGjtfZnsWt3JDccm3Y1uk2QvLmvM3Yt2x
LTC_WALLET=Lak56Y1JhwiW26YwcnXdgMSEMDjSUgp7PB


# ─── Advanced (rarely need changing) ──────────────────────────

# Shared secret between bot and (optional) remote FastAPI backend.
BOT_SECRET=ANDRO

# Shows up in logs — useful when running two bots with two tokens.
BOT_INSTANCE_LABEL=railway

# Minimum top-up amount in USD.
MIN_TOPUP_USD=15

# Storm.gift base URL — only change if they migrate.
STORM_API_BASE=https://api.storm.gift/api/v1

# Safety: when the bot runs on a host that ALSO runs the FastAPI
# backend (e.g. both on Emergent), set this to 1 to avoid running
# watchers twice. On a dedicated Railway bot service, keep it 0.
STANDALONE_SKIP_WATCHERS=0
