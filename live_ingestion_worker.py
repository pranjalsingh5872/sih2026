import time
import requests
import xml.etree.ElementTree as ET
import hashlib
import re
import uuid
import psycopg2

DB_CONFIG = {
    "dbname": "weatherdb",
    "user": "weather",
    "password": "weather_dev_pw",
    "host": "127.0.0.1",
    "port": 5432
}

dedup = set()

LOCATIONS = {
    "kerala": (76.2711, 10.8505, "Kerala", "Idukki"),
    "idukki": (76.9749, 9.8494, "Kerala", "Idukki"),
    "wayanad": (76.1320, 11.6854, "Kerala", "Wayanad"),
    "assam": (92.9376, 26.2006, "Assam", "Guwahati"),
    "guwahati": (91.7362, 26.1445, "Assam", "Kamrup"),
    "bihar": (85.1376, 25.5941, "Bihar", "Patna"),
    "patna": (85.1376, 25.5941, "Bihar", "Patna"),
    "indore": (75.8577, 22.7196, "Madhya Pradesh", "Indore"),
    "mumbai": (72.8777, 19.0760, "Maharashtra", "Mumbai"),
    "shimla": (77.1734, 31.1048, "Himachal Pradesh", "Shimla"),
    "delhi": (77.2090, 28.6139, "Delhi", "New Delhi")
}

FEEDS = [
    "https://news.google.com/rss/search?q=kerala+rain+OR+flood+OR+landslide+when:3d&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=assam+flood+OR+bihar+flood+when:3d&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=indore+waterlogging+OR+himachal+rain+when:3d&hl=en-IN&gl=IN&ceid=IN:en"
]

def cross_check_weather_verification(lat: float, lon: float, hazard_type: str) -> tuple[float, str]:
    """
    Open-Meteo se instant precipitation data fetch karke physical reality check karta hai.
    Handles None/null values gracefully.
    """
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=precipitation,rain&hourly=precipitation&past_hours=3"
        res = requests.get(url, timeout=5)
        if res.status_code == 200:
            data = res.json()
            
            # Current precipitation (None check)
            curr_val = data.get("current", {}).get("precipitation")
            curr_rain = float(curr_val) if curr_val is not None else 0.0
            
            # Past 3 hours hourly precipitation (safely filtering None/nulls)
            raw_hourly = data.get("hourly", {}).get("precipitation", [])[-3:]
            cleaned_hourly = [float(x) for x in raw_hourly if x is not None]
            past_rain = sum(cleaned_hourly) if cleaned_hourly else 0.0
            
            total_rain = max(curr_rain, past_rain)

            if total_rain >= 3.0:
                print(f"      -> [METEO CONFIRMED] Real rainfall recorded: {total_rain:.1f} mm at ({lat}, {lon})")
                return 0.95, "MET_CONFIRMED"
            elif total_rain > 0.0:
                print(f"      -> [METEO PLAUSIBLE] Light rainfall recorded: {total_rain:.1f} mm at ({lat}, {lon})")
                return 0.70, "MET_PLAUSIBLE"
            else:
                print(f"      -> [METEO REJECTED / FAKE CLAIM] 0.0 mm rain recorded at ({lat}, {lon})!")
                return 0.15, "CONTRADICTION_FLAGGED"
        else:
            print(f"      -> [METEO HTTP ERROR] Status code: {res.status_code}")
    except Exception as e:
        print(f"      -> [METEO ERROR] {e}, using baseline score.")
    
    return 0.70, "UNCORROBORATED"

def save_to_db(title, lon, lat, hazard, state, district, content_hash):
    conn = None
    try:
        # Step: Ground Truth Verification run karein
        credibility, status = cross_check_weather_verification(lat, lon, hazard)

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        ext_id = f"rss_{content_hash}"
        inc_id = str(uuid.uuid4())
        
        cur.execute("""
            INSERT INTO weather.incidents (
                incident_id, external_id, content_hash, source_type, source_name, raw_text,
                observed_at, geom, ai_category, credibility_score,
                admin_state, admin_district
            ) VALUES (
                %s, %s, %s, 'SOCIAL', 'LiveInternetCrawler', %s,
                NOW(), ST_SetSRID(ST_Point(%s, %s), 4326)::geography,
                %s, %s, %s, %s
            )
            ON CONFLICT DO NOTHING;
        """, (inc_id, ext_id, content_hash, title, lon, lat, hazard, credibility, state, district))
        conn.commit()
        cur.close()
        print(f"[LIVE INSERTED] {hazard} in {district}, {state} (Credibility: {credibility}) -> {title[:40]}...")
    except Exception as e:
        print(f"[DB ERROR] {e}")
    finally:
        if conn:
            conn.close()

print("==================================================================")
print("DIRECT POSTGIS LIVE INGESTION + WEATHER VERIFICATION ACTIVE")
print("==================================================================")

while True:
    for feed in FEEDS:
        try:
            res = requests.get(feed, timeout=6)
            if res.status_code == 200:
                root = ET.fromstring(res.content)
                for item in root.findall('.//item'):
                    t = item.find('title').text or ''
                    h = hashlib.md5(t.encode('utf-8')).hexdigest()
                    if h in dedup:
                        continue
                    t_low = t.lower()
                    for k, (lon, lat, state, district) in LOCATIONS.items():
                        if re.search(r'\b' + re.escape(k) + r'\b', t_low):
                            cat = "LANDSLIDE" if "landslide" in t_low else ("FLASH_FLOOD" if "flood" in t_low else "HEAVY_RAINFALL")
                            save_to_db(t, lon, lat, cat, state, district, h)
                            dedup.add(h)
                            time.sleep(0.2)
                            break
        except Exception:
            pass
    print("[POLL] Cycle complete. Waiting 20 seconds for new alerts...")
    time.sleep(20)