import logging
from datetime import datetime
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    JobQueue,
)
from telegram import Bot

# === Configuration ===
TELEGRAM_TOKEN = "7572140503:AAHT19M1wxiWYA0AekQJMiJQEBytVwobHng"
MOVING_CHAT_ID = -1002483766416
IDLE_CHAT_ID   = -1002538630918

SOURCE_PRIORITY = {"samsara":1, "intangles":2, "clubeld":3}

# === Helper Functions ===

def parse_time(ts_str: str) -> datetime:
    return datetime.strptime(ts_str, "%m-%d-%Y %H:%M:%S %Z")

def choose_record(existing: dict, candidate: dict) -> dict:
    p_e = SOURCE_PRIORITY.get(existing["source"], 99)
    p_c = SOURCE_PRIORITY.get(candidate["source"], 99)
    if p_c != p_e:
        return candidate if p_c < p_e else existing
    # same priority → pick newer
    return candidate if parse_time(candidate["update_time"]) > parse_time(existing["update_time"]) else existing

def dedupe_by_vin(records: list[dict]) -> list[dict]:
    by_vin = {}
    for r in records:
        vin = r["vin"]
        if vin in by_vin:
            by_vin[vin] = choose_record(by_vin[vin], r)
        else:
            by_vin[vin] = r
    return list(by_vin.values())

def fetch_all_telematics() -> list[dict]:
    # ► TEMPORARY SMOKE-TEST DATA ◄
    now = datetime.utcnow().strftime("%m-%d-%Y %H:%M:%S UTC")
    return [{
        "vin":         "TESTVIN001",
        "source":      "samsara",
        "update_time": now,
        "status":      "moving",
        "name":        "Test-Truck-1",
        "address":     "100 Test Blvd, Example City",
        "speed":       "42 mph"
    }]

# === Scheduled Jobs ===

async def send_moving_updates(context: ContextTypes.DEFAULT_TYPE):
    bot: Bot = context.bot
    raw   = fetch_all_telematics()
    clean = dedupe_by_vin(raw)
    movers = [r for r in clean if r["status"] in ("moving", "rolling")]
    for r in movers:
        msg = (
            f"🚛 *{r['name']}* is _{r['status']}_\n"
            f"Location: {r['address']}\n"
            f"Speed: {r['speed']}\n"
            f"Updated: {r['update_time']}"
        )
        await bot.send_message(chat_id=MOVING_CHAT_ID, text=msg, parse_mode="Markdown")

async def send_idle_alerts(context: ContextTypes.DEFAULT_TYPE):
    bot: Bot = context.bot
    now = datetime.utcnow()
    raw   = fetch_all_telematics()
    clean = dedupe_by_vin(raw)
    idlers = [r for r in clean if r["status"] == "idle"]
    for r in idlers:
        last = parse_time(r["update_time"])
        stopped = now - last
        hrs, rem = divmod(int(stopped.total_seconds()), 3600)
        mins = rem // 60
        duration = f"{hrs}h {mins}m" if hrs else f"{mins}m"
        msg = (
            f"⚠️ *{r['name']}* stopped _{duration}_ ago\n"
            f"Location: {r['address']}\n"
            f"Speed: {r['speed']}\n"
            f"Last update: {r['update_time']}"
        )
        await bot.send_message(chat_id=IDLE_CHAT_ID, text=msg, parse_mode="Markdown")

# === Entrypoint ===

def main():
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO
    )

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    jq: JobQueue = app.job_queue

    # schedule immediately and then every 3h (10800s)
    jq.run_repeating(send_moving_updates, interval=10800, first=0)
    jq.run_repeating(send_idle_alerts,   interval=10800, first=0)

    app.run_polling()

if __name__ == "__main__":
    main()
