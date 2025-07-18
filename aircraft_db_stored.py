from flask import Flask, jsonify
from flask_cors import CORS
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone
import logging
import threading
import time
import os

# --- Config ---
API_URL = "https://opensky-network.org/api/states/all"
MIN_LAT, MAX_LAT = 6.0, 38.0
MIN_LON, MAX_LON = 68.0, 97.0

# --- App Setup ---
app = Flask(__name__)
CORS(app)  # Optional: allow frontend access
logging.basicConfig(level=logging.INFO)

#db_host = os.environ.get("DATABASE_HOST", "127.0.0.1")

# --- DB Connection ---
def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost", "127.0.0.1"),
        database=os.environ.get("DB_NAME", "aircraft_db"),
        user=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD", "Riti@2901"),
       # host=db_host,
        port=5432
    )

# --- Fetch OpenSky Data ---
def fetch_opensky_data():
    try:
        response = requests.get(API_URL, timeout=10)
        if response.status_code == 200:
            return response.json().get("states", [])
        logging.warning(f"OpenSky API Error: {response.status_code}")
    except Exception as e:
        logging.error(f"OpenSky Exception: {e}")
    return None

# --- Filter Indian Aircraft ---
def filter_indian_aircraft(states):
    aircraft = []
    seen = set()
    for s in states:
        lat, lon = s[6], s[5]
        if lat and lon and MIN_LAT <= lat <= MAX_LAT and MIN_LON <= lon <= MAX_LON:
            icao = s[0]
            if icao not in seen:
                aircraft.append(s)
                seen.add(icao)
            if len(aircraft) == 20:
                break
    return aircraft

# --- Insert Aircraft Data ---
def insert_aircraft_data(aircraft_list):
    try:
        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        for s in aircraft_list:
            try:
                data = (
                    s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
                    s[8], s[9], s[10], s[11], s[13], s[14], s[15], s[16],
                    datetime.now(timezone.utc)
                )
                cursor.execute("""
                    INSERT INTO aircraft_data (
                        icao24, callsign, origin_country, time_position,
                        last_contact, longitude, latitude, baro_altitude,
                        on_ground, velocity, true_track, vertical_rate,
                        geo_altitude, squawk, spi, position_source, timestamp
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, data)
            except Exception as e:
                logging.error(f"DB insert failed for {s[0]}: {e}")
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"DB connection or insert error: {e}")

# --- Fallback from DB ---
def fallback_from_db():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT * FROM aircraft_data ORDER BY timestamp DESC LIMIT 20")
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return data
    except Exception as e:
        logging.error(f"DB Fallback Error: {e}")
        return []

# --- Background data collector ---
def background_data_collector():
    while True:
        try:
            states = fetch_opensky_data()
            if states:
                filtered = filter_indian_aircraft(states)
                insert_aircraft_data(filtered)
                logging.info(f"Inserted {len(filtered)} aircraft records at {datetime.now(timezone.utc)}")
            else:
                logging.warning("No states fetched in background collector")
        except Exception as e:
            logging.error(f"Background thread error: {e}")
        time.sleep(10)

# --- Routes ---
@app.route("/aircrafts", methods=["GET"])
def get_aircrafts():
    try:
        states = fetch_opensky_data()
        if states:
            filtered = filter_indian_aircraft(states)
            data = [
                {
                    "icao24": s[0],
                    "callsign": s[1],
                    "origin_country": s[2],
                    "time_position": s[3],
                    "last_contact": s[4],
                    "longitude": s[5],
                    "latitude": s[6],
                    "baro_altitude": s[7],
                    "on_ground": s[8],
                    "velocity": s[9],
                    "true_track": s[10],
                    "vertical_rate": s[11],
                    "geo_altitude": s[13],
                    "squawk": s[14],
                    "spi": s[15],
                    "position_source": s[16],
                    "source": "opensky-live"
                } for s in filtered
            ]
            return jsonify({"aircrafts": data, "count": len(data)})
        else:
            db_data = fallback_from_db()
            return jsonify({"aircrafts": db_data, "count": len(db_data), "source": "db-fallback"})
    except Exception as e:
        logging.error(f"/aircrafts route error: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route("/")
def home():
    return jsonify({"message": "Aircraft Tracker API (Live + Fallback)"})

# --- Run ---
if __name__ == "__main__":
    collector_thread = threading.Thread(target=background_data_collector, daemon=True)
    collector_thread.start()

    port = int(os.environ.get("PORT", 5000))  # Required by Render
    app.run(host="0.0.0.0", port=port, debug=True)
