#!/usr/bin/env sh
# ═══════════════════════════════════════════════════════════════
#  DataLine Bot — unified boot script
# ═══════════════════════════════════════════════════════════════
#  MODE picks what this container runs:
#     bot   → standalone Telegram bot (default for this repo)
#     web   → FastAPI admin API on $PORT
#     both  → API + bot in same container
# ═══════════════════════════════════════════════════════════════
set -e

MODE="${MODE:-bot}"        # default to bot for this repo
PORT="${PORT:-8001}"

echo "[boot] MODE=$MODE PORT=$PORT BOT_STANDALONE=${BOT_STANDALONE:-1}"

case "$MODE" in
  bot)
    export BOT_STANDALONE="${BOT_STANDALONE:-1}"
    exec python -u -m bot.bot
    ;;
  both)
    uvicorn server:app --host 0.0.0.0 --port "$PORT" &
    API_PID=$!
    : "${BACKEND_API_URL:=http://localhost:$PORT/api/bot/action}"
    export BACKEND_API_URL
    exec python -u -m bot.bot
    ;;
  web)
    exec uvicorn server:app --host 0.0.0.0 --port "$PORT"
    ;;
  *)
    echo "[boot] Unknown MODE='$MODE' — defaulting to bot"
    export BOT_STANDALONE="${BOT_STANDALONE:-1}"
    exec python -u -m bot.bot
    ;;
esac
