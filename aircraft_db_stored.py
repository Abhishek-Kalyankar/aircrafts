from flask import Flask, jsonify
from flask_cors import CORS
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone
import logging
import threading
import time

# --- Config ---
API_URL = "https://opensky-network.org/api/states/all"
MIN_LAT, MAX_LAT = 6.0, 38.0
MIN_LON, MAX_LON = 68.0, 97.0

# --- App Setup ---
app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)

# --- Database Connection ---
def get_db_connection():
    return psycopg2.connect(
        host="127.0.0.1",              # Use your actual DB host if different
        database="aircraft_db",        # Replace with your DB name
        user="postgres",               # Replace with your DB username
        password="Riti@2901",          # Replace with your DB password
        port=5432
    )

# --- Fetch Data from OpenSky API ---
def fetch_aircraft_data():
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        aircrafts = []

        for state in data.get("states", []):
            lat = state[6]
            lon = state[5]
            if lat is not None and lon is not None and MIN_LAT <= lat <= MAX_LAT and MIN_LON <= lon <= MAX_LON:
                aircrafts.append({
                    "icao24": state[0],
                    "callsign": state[1].strip() if state[1] else None,
                    "origin_country": state[2],
                    "time_position": state[3],
                    "last_contact": state[4],
                    "longitude": lon,
                    "latitude": lat,
                    "baro_altitude": state[7],
                    "on_ground": state[8],
                    "velocity": state[9],
                    "true_track": state[10],
                    "vertical_rate": state[11],
                    "geo_altitude": state[13],
                    "squawk": state[14],
                    "spi": state[15],
                    "position_source": state[16],
                    "retrieved_at": datetime.now(timezone.utc)
                })
        return aircrafts
    except Exception as e:
        logging.error(f"Failed to fetch data from OpenSky: {e}")
        return []

# --- Save to DB ---
def save_aircrafts_to_db(aircrafts):
    if not aircrafts:
        return

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        for ac in aircrafts:
            cur.execute("""
                INSERT INTO aircraft_data (
                    icao24, callsign, origin_country, time_position,
                    last_contact, longitude, latitude, baro_altitude,
                    on_ground, velocity, true_track, vertical_rate,
                    geo_altitude, squawk, spi, position_source, retrieved_at
                ) VALUES (
                    %(icao24)s, %(callsign)s, %(origin_country)s, %(time_position)s,
                    %(last_contact)s, %(longitude)s, %(latitude)s, %(baro_altitude)s,
                    %(on_ground)s, %(velocity)s, %(true_track)s, %(vertical_rate)s,
                    %(geo_altitude)s, %(squawk)s, %(spi)s, %(position_source)s, %(retrieved_at)s
                )
            """, ac)
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Saved {len(aircrafts)} aircrafts to database.")
    except Exception as e:
        logging.error(f"Error saving to DB: {e}")

# --- Background Updater ---
def periodic_update():
    while True:
        logging.info("Fetching aircraft data...")
        data = fetch_aircraft_data()
        save_aircrafts_to_db(data)
        time.sleep(60)

# --- Flask Routes ---
@app.route("/aircrafts", methods=["GET"])
def get_aircrafts():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM aircraft_data ORDER BY retrieved_at DESC LIMIT 100")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"aircrafts": rows, "count": len(rows), "source": "db"})
    except Exception as e:
        logging.error(f"Error retrieving from DB: {e}")
        return jsonify({"aircrafts": [], "count": 0, "source": "db-fallback"}), 500

# --- Start Thread & App ---
if __name__ == "__main__":
    updater_thread = threading.Thread(target=periodic_update, daemon=True)
    updater_thread.start()
    app.run(host="0.0.0.0", port=5000)
