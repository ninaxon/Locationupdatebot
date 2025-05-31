import logging
import pytz
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, CallbackQueryHandler
from rapidfuzz import fuzz
import requests
import re
import pandas as pd
from dateutil import parser as dateparser

# --- CONFIG ---
ORS_API_KEY = "5b3ce3597851110001cf6248c7cc918711b8458c9d8ecc6b80df78a8"
TELEGRAM_TOKEN = "7572140503:AAHT19M1wxiWYA0AekQJMiJQEBytVwobHng"
TMS_API_URL = "http://18.188.22.20/api/tms_get_locations/"
TMS_API_KEY = "8bce273ab78752ced8f8092d42164569"
TMS_API_HASH = "92b23db49659b2788112e44e0015d9f72aa830e787011fe3b7e41f05d6c74179"
EXCEL_PATH = "Asset Vehicle - Safety Cameras - 29 May 2025 00_47.xlsx"

# --- Globals ---
user_sessions = {}
all_trucks = []
vin_driver_map = {}
zip_cache = {
    "84104": [-111.9845, 40.7345],
    "10001": [-73.9967, 40.7484],
    "60601": [-87.6229, 41.8864],
    "90001": [-118.2479, 33.9731],
}
geocache = {}

logging.basicConfig(level=logging.INFO)

def load_driver_vin_map():
    global vin_driver_map
    try:
        df = pd.read_excel(EXCEL_PATH, engine="openpyxl")
        for _, row in df.iterrows():
            driver_name = str(row[2]).strip().lower()
            vin = str(row[3]).strip().upper()
            vin_driver_map[driver_name] = vin
        logging.info(f"✅ Loaded {len(vin_driver_map)} driver-VIN mappings.")
    except Exception as e:
        logging.error(f"❌ Failed to load Excel data: {e}")

def load_truck_list():
    global all_trucks
    params = {"api_key": TMS_API_KEY, "api_hash": TMS_API_HASH}
    try:
        r = requests.get(TMS_API_URL, params=params)
        r.raise_for_status()
        all_trucks = []
        skipped = 0
        for truck in r.json().get("locations", []):
            address = truck.get("address", "Unknown")
            update_time_str = truck.get("update_time")
            lat = truck.get("lat")
            lng = truck.get("lng")
            source = truck.get("source", "")

            if source.lower() != "samsara":
                skipped += 1
                continue

            if not lat or not lng:
                skipped += 1
                continue

            if (not address or address.strip().lower() == "unknown") and update_time_str:
                try:
                    update_dt = datetime.strptime(update_time_str.replace("EST", ""), "%m-%d-%Y %H:%M:%S ").replace(tzinfo=pytz.timezone("America/New_York"))
                except Exception:
                    skipped += 1
                    continue
                if datetime.now(pytz.utc) - update_dt.astimezone(pytz.utc) > timedelta(hours=10):
                    skipped += 1
                    continue

            all_trucks.append(truck)
        logging.info(f"✅ Loaded {len(all_trucks)} trucks. Skipped: {skipped}")
    except Exception as e:
        logging.error(f"❌ Failed to load trucks: {e}")

def geocode(address):
    cleaned = " ".join(address.strip().lower().split())
    cleaned = re.sub(r"\s+", " ", cleaned)
    if cleaned in geocache:
        return geocache[cleaned]
    if cleaned.isdigit() and cleaned in zip_cache:
        logging.info(f"📦 Using ZIP cache for {cleaned}")
        return zip_cache[cleaned]
    replacements = {"slc": "salt lake city", "nyc": "new york"}
    for k, v in replacements.items():
        cleaned = cleaned.replace(k, v)
    url = "https://api.openrouteservice.org/geocode/search"
    params = {"api_key": ORS_API_KEY, "text": cleaned, "boundary.country": "US", "size": 1}
    try:
        r = requests.get(url, params=params)
        r.raise_for_status()
        coords = r.json()["features"][0]["geometry"]["coordinates"]
        geocache[cleaned] = coords
        return coords
    except Exception as e:
        logging.warning(f"❌ ORS Geocoding failed for '{cleaned}': {e}")
        return None

def get_route(origin, dest):
    url = "https://api.openrouteservice.org/v2/directions/driving-car"
    headers = {"Authorization": ORS_API_KEY}
    body = {"coordinates": [origin, dest]}
    try:
        r = requests.post(url, headers=headers, json=body)
        r.raise_for_status()
        data = r.json()
        return {
            "duration": timedelta(seconds=data["routes"][0]["summary"]["duration"]),
            "distance_miles": round(data["routes"][0]["summary"]["distance"] / 1609.34, 1),
            "map_url": f"https://www.openstreetmap.org/directions?engine=fossgis_osrm_car&route={origin[1]}%2C{origin[0]}%3B{dest[1]}%2C{dest[0]}"
        }
    except:
        return None

def parse_time(t):
    try:
        return dateparser.parse(t).astimezone(pytz.timezone("America/New_York"))
    except Exception:
        return None

def smart_match(query):
    query_cleaned = query.strip().lower()
    candidates = [(driver, fuzz.partial_ratio(query_cleaned, driver)) for driver in vin_driver_map]
    candidates = sorted(candidates, key=lambda x: x[1], reverse=True)[:5]
    return candidates if candidates and candidates[0][1] > 60 else []

def handle_driver_choice(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    driver = query.data
    vin = vin_driver_map.get(driver)
    truck = next((t for t in all_trucks if t.get("vin", "").upper() == vin), None)
    if not truck:
        query.edit_message_text("❌ Could not find truck for this driver.")
        return
    location = truck.get("address", "Unknown")
    unit_name = truck.get("name")
    lat = truck.get("lat")
    lng = truck.get("lng")
    status = truck.get("status", "unknown")
    update_time = truck.get("update_time", "")
    user_sessions[user_id] = {
        "unit": f"{driver.title()} / {unit_name}",
        "status": status,
        "location_data": {"location": location, "coords": [lng, lat]},
        "update_time": update_time
    }
    query.edit_message_text("📍 Found unit. Please send the *delivery address*", parse_mode='Markdown')

def start_update(update: Update, context: CallbackContext):
    if not context.args:
        update.message.reply_text("Usage: /update <Driver Name>")
        return
    user_id = update.message.from_user.id
    query_text = ' '.join(context.args)
    matches = smart_match(query_text)
    if not matches:
        update.message.reply_text("❌ Could not find truck for this driver.")
        return
    keyboard = [[InlineKeyboardButton(name.title(), callback_data=name)] for name, _ in matches]
    reply_markup = InlineKeyboardMarkup(keyboard)
    update.message.reply_text("👤 Please select the driver:", reply_markup=reply_markup)

def handle_address(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    session = user_sessions.get(user_id)
    if not session:
        update.message.reply_text("❌ Please start with /update <truck info> first.")
        return
    session["delivery_address"] = update.message.text
    update.message.reply_text("🕒 Now, send the *delivery appointment time* in any standard format", parse_mode='Markdown')

def handle_appointment_time(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    session = user_sessions.get(user_id)
    if not session or "delivery_address" not in session:
        update.message.reply_text("❌ Please send delivery address first.")
        return
    appt_time = parse_time(update.message.text)
    if not appt_time:
        update.message.reply_text("⚠️ Invalid format. Please try again with a valid date/time format.", parse_mode='Markdown')
        return
    origin = session["location_data"].get("coords")
    destination = geocode(session["delivery_address"])
    if not origin or not destination:
        update.message.reply_text("❌ Geocoding failed.")
        return
    route = get_route(origin, destination)
    if not route:
        update.message.reply_text("❌ Failed to fetch route info.")
        return
    eta_dt = datetime.utcnow() + route["duration"]
    local_tz = pytz.timezone("America/New_York")
    eta_local = eta_dt.astimezone(local_tz)
    appt_time_local = appt_time.astimezone(local_tz)
    delay_minutes = int((eta_local - appt_time_local).total_seconds() / 60)
    delay_str = "✅ On Time"
    if delay_minutes > 0:
        hours, mins = divmod(delay_minutes, 60)
        delay_str = f"⚠️ Late by {hours} hours and {mins} minutes"

    update_time_str = session.get("update_time", "N/A")
    update.message.reply_text(
        f"🚛 *Update for:* `{session['unit']}`\n"
        f"🛑 Status: {session.get('status', 'unknown').title()}\n"
        f"📍 Location: {session['location_data']['location']}\n"
        f"📦 Delivery To: {session['delivery_address']}\n"
        f"🛣️ Distance: {route['distance_miles']} miles\n"
        f"⏱ ETA: {eta_local.strftime('%I:%M %p %Z')}\n"
        f"📅 Appt: {appt_time_local.strftime('%I:%M %p %Z')}\n"
        f"📡 Last Updated: {update_time_str}\n"
        f"🗺️ [View Route]({route['map_url']})\n"
        f"{delay_str}",
        parse_mode='Markdown',
        disable_web_page_preview=True
    )

def handle_flow(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    session = user_sessions.get(user_id, {})
    if "delivery_address" not in session:
        handle_address(update, context)
    else:
        handle_appointment_time(update, context)

def main():
    load_driver_vin_map()
    load_truck_list()
    updater = Updater(TELEGRAM_TOKEN, use_context=True)
    dp = updater.dispatcher
    dp.add_handler(CommandHandler("update", start_update))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_flow))
    dp.add_handler(CallbackQueryHandler(handle_driver_choice))
    print("✅ Bot is running with Smart Match + ZIP/Fuzzy Geocode + Driver UI Support")
    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()
