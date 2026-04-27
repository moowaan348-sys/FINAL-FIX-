# 🚂 Deploy the Standalone Bot on Railway (Plug-and-Play)

This is the **one-service, bot-only** deployment path. Use it when you want a
dedicated Railway service that runs **nothing but the Telegram bot** (talking
directly to MongoDB — no separate FastAPI backend needed).

Perfect as a **hot backup** alongside your primary bot, or a clean solo deploy.

---

## 1‑Click Deploy

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/new/template?template=https://github.com/YOUR_GITHUB_USERNAME/YOUR_REPO_NAME)

> ⚠️ **Edit this link after you push to GitHub.** Replace
> `YOUR_GITHUB_USERNAME/YOUR_REPO_NAME` with your actual repo path, e.g.
> `https://github.com/alice/dataline-bot`. Then the button creates a new
> Railway project pre-wired to your repo.

---

## Manual Setup (3 minutes)

### 1. Create the Railway project

1. Go to **[railway.app](https://railway.app) → New Project → Deploy from GitHub repo**
2. Pick the repo you pushed from Emergent.
3. In the service settings:
   - **Root Directory** = `backend/`
   - **Start Command** = leave blank (it auto-reads `boot.sh`)

### 2. Add MongoDB

You need a Mongo connection string. Two easy options:

- **Railway plugin:** `+ New → Database → MongoDB` — Railway injects `MONGO_URL`
  into every service in the project automatically. ✅ done.
- **MongoDB Atlas (free M0):** grab the SRV URI and set it as `MONGO_URL`.

### 3. Set the environment variables

In **Service → Variables** click **Raw Editor** and paste:

```env
# --- tells the boot script to run ONLY the bot ---
MODE=bot
BOT_STANDALONE=1

# --- required ---
MONGO_URL=mongodb+srv://user:pass@cluster0.xxx.mongodb.net/dataline
DB_NAME=dataline_store
BOT_TOKEN=123456789:YOUR-TELEGRAM-BOT-TOKEN
ADMIN_TG_IDS=8295276273,8798542436

# --- optional (enable the extra features) ---
STORM_API_KEY=
HANDYAPI_KEY=
USDT_TRC20_WALLET=TCGjtfZnsWt3JDccm3Y1uk2QvLmvM3Yt2x
LTC_WALLET=Lak56Y1JhwiW26YwcnXdgMSEMDjSUgp7PB
```

Hit **Deploy**. Railway will build, install requirements, run `boot.sh`,
`MODE=bot` will kick in, and your bot starts polling Telegram within ~60 s.

### 4. Verify

Open the service's **Deploy logs** — you should see:

```
[boot] MODE=bot PORT=8001 BOT_STANDALONE=1
… - bot - INFO - Starting DataLine bot — instance=primary bot_id=…
… - telegram.ext.Application - INFO - Application started
```

Send `/start` to your bot on Telegram — the welcome message should load.

---

## Running a second bot in parallel (backup token)

Spin up a **second Railway service** in the same project with the exact same
setup, but change **just** `BOT_TOKEN` to your backup token. Both services will
share the same MongoDB, users, balances and stock — so either bot can serve
users if the other is down.

```env
BOT_TOKEN=<your-backup-token-here>
BOT_INSTANCE_LABEL=backup
# all other variables identical to the primary service
```

No other config change needed. 🎉

---

## Common issues

| Symptom | Fix |
|---|---|
| `telegram.error.Conflict: … getUpdates` | Two processes polling the same token. Stop the other one (or use the **two tokens** pattern above). |
| Bot starts but `/start` never replies | Check logs for `Starting DataLine bot — instance=…` — if missing, verify `MODE=bot` and `BOT_TOKEN` are both set. |
| `Chat not found` when bot tries to DM admins on startup | That admin ID hasn't opened the bot yet. Send `/start` to the bot from each admin account once. |
| 429 errors from `api.blockcypher.com` | Expected on the free tier. Only matters for LTC top-up auto-credit; everything else keeps working. |

---

## What the boot script does

`backend/boot.sh` picks what to run from the `MODE` env var:

| `MODE` | What runs |
|---|---|
| `bot` (recommended for this deploy) | Just the Telegram bot, in standalone mode |
| `web` | Only the FastAPI admin API + blockchain watchers |
| `both` | API + bot together in one container |
| _(unset)_ | Defaults to `web` |

So the same repo can power **any** service type in Railway just by flipping
one variable. That's the plug-and-play part.
