"""
Pan-India Disaster & Heavy Weather Abnormality Seeder for AAGAHI 2026.
Uses asyncpg (native to the container) to populate all disaster and heavy weather abnormality clusters across India.
"""

import asyncio
import os
import uuid
import random
from datetime import datetime, timedelta, timezone
import asyncpg

POSTGRES_USER = os.getenv("POSTGRES_USER", "weather")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "weather_dev_pw")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weatherdb")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

PAN_INDIA_DISASTER_ZONES = [
    # --- WEST COAST & WESTERN GHATS ---
    {
        "hazard": "LANDSLIDE",
        "state": "Kerala",
        "district": "Wayanad",
        "place": "Meppadi & Chooralmala, Wayanad",
        "lat": 11.6854, "lon": 76.1320,
        "count": 18, "cred": 0.98,
        "text": "Massive debris flow and cloudburst triggering hillside collapse in Chooralmala and Mundakkai."
    },
    {
        "hazard": "FLASH_FLOOD",
        "state": "Kerala",
        "district": "Idukki",
        "place": "Munnar & Devikulam, Idukki",
        "lat": 10.0889, "lon": 77.0595,
        "count": 14, "cred": 0.95,
        "text": "Muthirapuzha river breaching banks, tourist routes cut off, heavy catchment downpour."
    },
    {
        "hazard": "FLASH_FLOOD",
        "state": "Kerala",
        "district": "Alappuzha",
        "place": "Kuttanad & Vembanad Basin, Alappuzha",
        "lat": 9.4981, "lon": 76.3388,
        "count": 12, "cred": 0.93,
        "text": "Below sea-level paddy polders submerged; backwater surge inundating residential settlements."
    },
    {
        "hazard": "HEAVY_RAINFALL",
        "state": "Maharashtra",
        "district": "Mumbai Suburban",
        "place": "Kurla, Hindmata & Milan Subway, Mumbai",
        "lat": 19.0760, "lon": 72.8777,
        "count": 22, "cred": 0.97,
        "text": "Extreme convective downpour (over 200mm in 6 hrs) coinciding with 4.5m high astronomical tide; suburban rail suspended."
    },
    {
        "hazard": "LANDSLIDE",
        "state": "Maharashtra",
        "district": "Raigad",
        "place": "Mahad & Poladpur Ghats, Raigad",
        "lat": 18.2355, "lon": 73.4198,
        "count": 9, "cred": 0.92,
        "text": "Savitri river crossing danger levels; hillside rockfall along Goa-Mumbai highway."
    },
    {
        "hazard": "HEAVY_RAINFALL",
        "state": "Maharashtra",
        "district": "Ratnagiri",
        "place": "Chiplun & Coastal Belt, Ratnagiri",
        "lat": 17.5323, "lon": 73.5186,
        "count": 11, "cred": 0.91,
        "text": "Vashishti river overflowing commercial marketplace; red alert issued by IMD RMC Mumbai."
    },
    {
        "hazard": "FLASH_FLOOD",
        "state": "Karnataka",
        "district": "Dakshina Kannada",
        "place": "Mangalore & Bantwal, Dakshina Kannada",
        "lat": 12.9141, "lon": 74.8560,
        "count": 10, "cred": 0.90,
        "text": "Netravati river surge inundating agricultural land and coastal low-lying bypasses."
    },
    {
        "hazard": "LANDSLIDE",
        "state": "Karnataka",
        "district": "Kodagu",
        "place": "Madikeri & Bhagamandala, Coorg",
        "lat": 12.4244, "lon": 75.7382,
        "count": 8, "cred": 0.91,
        "text": "Talacauvery slopes saturated; mudslides blocking Virajpet connectivity."
    },

    # --- GUJARAT & ARABIAN SEA CYCLONE CORRIDOR ---
    {
        "hazard": "CYCLONE",
        "state": "Gujarat",
        "district": "Kutch",
        "place": "Mandvi & Jakhau Port, Kutch",
        "lat": 23.0333, "lon": 69.6692,
        "count": 26, "cred": 0.99,
        "text": "Very Severe Cyclonic Storm making coastal landfall with sustained winds of 145 km/h and 3.5m storm surge."
    },
    {
        "hazard": "CYCLONE",
        "state": "Gujarat",
        "district": "Devbhumi Dwarka",
        "place": "Okha & Dwarka Coastline",
        "lat": 22.2442, "lon": 68.9685,
        "count": 16, "cred": 0.97,
        "text": "Severe coastal gale damaging communications masts and uprooting trees; port operations halted."
    },

    # --- NORTHERN HIMALAYAN BELT (CLOUDBURSTS & LANDSLIDES) ---
    {
        "hazard": "LANDSLIDE",
        "state": "Uttarakhand",
        "district": "Chamoli",
        "place": "Joshimath & Badrinath Highway, Chamoli",
        "lat": 30.5564, "lon": 79.5659,
        "count": 15, "cred": 0.96,
        "text": "Alaknanda tributary surge and major landslide severing pilgrim route near Helang."
    },
    {
        "hazard": "HEAVY_RAINFALL",
        "state": "Uttarakhand",
        "district": "Rudraprayag",
        "place": "Kedarnath Valley & Gaurikund, Rudraprayag",
        "lat": 30.6441, "lon": 79.0669,
        "count": 12, "cred": 0.95,
        "text": "Localized cloudburst event dumping torrential rain; Mandakini river rising 3 meters in 1 hour."
    },
    {
        "hazard": "LANDSLIDE",
        "state": "Uttarakhand",
        "district": "Uttarkashi",
        "place": "Dharali & Bhagirathi Valley, Uttarkashi",
        "lat": 30.7268, "lon": 78.4354,
        "count": 9, "cred": 0.91,
        "text": "Debris avalanche blocking river channel; flash flood warning downstream."
    },
    {
        "hazard": "HEAVY_RAINFALL",
        "state": "Himachal Pradesh",
        "district": "Kullu",
        "place": "Manali & Beas Valley, Kullu",
        "lat": 32.2432, "lon": 77.1892,
        "count": 17, "cred": 0.96,
        "text": "Beas river in violent spate washing away riverbanks and parking structures; NH-3 damaged."
    },
    {
        "hazard": "FLASH_FLOOD",
        "state": "Himachal Pradesh",
        "district": "Shimla",
        "place": "Rampur Bushahr & Sunni, Shimla",
        "lat": 31.4500, "lon": 77.6333,
        "count": 13, "cred": 0.94,
        "text": "Satluj river swelling; localized cloudburst washing away footbridges in interior hamlets."
    },
    {
        "hazard": "LANDSLIDE",
        "state": "Himachal Pradesh",
        "district": "Mandi",
        "place": "Pandoh Dam & Aut Tunnel, Mandi",
        "lat": 31.7087, "lon": 76.9320,
        "count": 11, "cred": 0.92,
        "text": "Massive hill face collapse near Pandoh; highway completely closed to all vehicular traffic."
    },
    {
        "hazard": "LANDSLIDE",
        "state": "Jammu & Kashmir",
        "district": "Ramban",
        "place": "Mehar & Panthyal, Ramban NH-44",
        "lat": 33.2428, "lon": 75.2426,
        "count": 10, "cred": 0.93,
        "text": "Continuous shooting stones and mudflow blocking the arterial Srinagar-Jammu National Highway."
    },
    {
        "hazard": "FLASH_FLOOD",
        "state": "Jammu & Kashmir",
        "district": "Srinagar",
        "place": "Ram Munshi Bagh & Jhelum Gauge, Srinagar",
        "lat": 34.0837, "lon": 74.7973,
        "count": 8, "cred": 0.90,
        "text": "Jhelum river approaching flood spill channel alert mark following 48 hours of upper-catchment rains."
    },

    # --- NORTH-EAST BRAHMAPUTRA & BARAK VALLEYS ---
    {
        "hazard": "FLASH_FLOOD",
        "state": "Assam",
        "district": "Kamrup Metropolitan",
        "place": "Guwahati & Uzan Bazar, Kamrup Metro",
        "lat": 26.1445, "lon": 91.7362,
        "count": 24, "cred": 0.97,
        "text": "Brahmaputra flowing 1.2m above danger mark; municipal drainage sluice gates reversed."
    },
    {
        "hazard": "FLASH_FLOOD",
        "state": "Assam",
        "district": "Dibrugarh",
        "place": "Maijan & Rohmoria, Dibrugarh",
        "lat": 27.4728, "lon": 94.9120,
        "count": 15, "cred": 0.94,
        "text": "Catastrophic riverbank erosion threatening anti-flood revetments; villages inundated."
    },
    {
        "hazard": "FLASH_FLOOD",
        "state": "Assam",
        "district": "Majuli",
        "place": "Garamur & Kamalabari, Majuli Island",
        "lat": 26.9635, "lon": 94.2037,
        "count": 12, "cred": 0.93,
        "text": "Island embankment breaches in two sectors; ferry services across Brahmaputra halted."
    },
    {
        "hazard": "HEAVY_RAINFALL",
        "state": "Meghalaya",
        "district": "East Khasi Hills",
        "place": "Sohra (Cherrapunji) & Mawsynram",
        "lat": 25.2986, "lon": 91.7086,
        "count": 14, "cred": 0.96,
        "text": "Phenomenal orographic deluge recording 340mm in 24 hours; waterfalls flooding border plains."
    },
    {
        "hazard": "FLASH_FLOOD",
        "state": "Tripura",
        "district": "Gomati",
        "place": "Udaipur & Amarpur, Gomati River",
        "lat": 23.5350, "lon": 91.4883,
        "count": 7, "cred": 0.89,
        "text": "Gomati river dam overflow; low-lying agricultural fields and bridge approaches inundated."
    },

    # --- GANGETIC PLAINS & BIHAR FLOOD BELT ---
    {
        "hazard": "FLASH_FLOOD",
        "state": "Bihar",
        "district": "Patna",
        "place": "Danapur & Gandhi Ghat, Patna",
        "lat": 25.5941, "lon": 85.1376,
        "count": 19, "cred": 0.95,
        "text": "Ganga river exceeding danger level by 85 cm; diara settlements completely submerged."
    },
    {
        "hazard": "FLASH_FLOOD",
        "state": "Bihar",
        "district": "Supaul",
        "place": "Kosi Embankment & Nirmali, Supaul",
        "lat": 26.1260, "lon": 86.6053,
        "count": 16, "cred": 0.94,
        "text": "Birpur barrage releasing 380,000 cusecs; flood waters gushing through rural panchayats."
    },
    {
        "hazard": "FLASH_FLOOD",
        "state": "Bihar",
        "district": "Bhagalpur",
        "place": "Kahalgaon & Naugachia, Bhagalpur",
        "lat": 25.2425, "lon": 86.9842,
        "count": 11, "cred": 0.92,
        "text": "Ganga and Kosi confluence creating massive backwaters, disrupting NH-31."
    },
    {
        "hazard": "FLASH_FLOOD",
        "state": "Uttar Pradesh",
        "district": "Varanasi",
        "place": "Assi to Dashashwamedh Ghats, Varanasi",
        "lat": 25.3176, "lon": 82.9739,
        "count": 14, "cred": 0.93,
        "text": "Ganga river submerging ancient cremation ghat platforms; boat navigation strictly banned."
    },
    {
        "hazard": "FLASH_FLOOD",
        "state": "Uttar Pradesh",
        "district": "Gorakhpur",
        "place": "Rapti & Rohin River Basins, Gorakhpur",
        "lat": 26.7606, "lon": 83.3732,
        "count": 13, "cred": 0.92,
        "text": "Rapti river rising above extreme danger mark; ring bunds under high pressure."
    },

    # --- BAY OF BENGAL & EAST COAST ---
    {
        "hazard": "CYCLONE",
        "state": "Odisha",
        "district": "Puri",
        "place": "Puri Beach & Konark Coastal Belt",
        "lat": 19.8135, "lon": 85.8312,
        "count": 20, "cred": 0.97,
        "text": "Deep depression intensifying over Northwest Bay of Bengal; gale wind warning (85 km/h) & rough sea swell."
    },
    {
        "hazard": "THUNDERSTORM",
        "state": "Odisha",
        "district": "Mayurbhanj",
        "place": "Baripada & Similipal Hills, Mayurbhanj",
        "lat": 21.9322, "lon": 86.7262,
        "count": 11, "cred": 0.91,
        "text": "Severe convective squall with over 1,200 cloud-to-ground lightning strikes; power grid trips."
    },
    {
        "hazard": "FLASH_FLOOD",
        "state": "West Bengal",
        "district": "South 24 Parganas",
        "place": "Gosaba & Kakdwip, Sundarbans Delta",
        "lat": 21.8750, "lon": 88.5833,
        "count": 15, "cred": 0.94,
        "text": "Tidal bore breaching earthen dykes; saline flood waters entering paddy farmlands."
    },
    {
        "hazard": "HEAVY_RAINFALL",
        "state": "West Bengal",
        "district": "Kolkata",
        "place": "Park Street, Ballygunge & Ultadanga, Kolkata",
        "lat": 22.5726, "lon": 88.3639,
        "count": 16, "cred": 0.93,
        "text": "Sudden thunderstorm cloudburst flooding tram tracks and low-lying thoroughfares."
    },
    {
        "hazard": "CYCLONE",
        "state": "Andhra Pradesh",
        "district": "Visakhapatnam",
        "place": "RK Beach & Gangavaram Port, Visakhapatnam",
        "lat": 17.6868, "lon": 83.2185,
        "count": 13, "cred": 0.94,
        "text": "Coastal squall and storm surge advisory issued for north coastal Andhra fishermen."
    },
    {
        "hazard": "HEAVY_RAINFALL",
        "state": "Tamil Nadu",
        "district": "Chennai",
        "place": "Velachery, Adyar & Tambaram, Chennai",
        "lat": 13.0827, "lon": 80.2707,
        "count": 21, "cred": 0.96,
        "text": "Northeast monsoon intense cloudburst; Chembarambakkam reservoir opening sluice surplus."
    },
    {
        "hazard": "FLASH_FLOOD",
        "state": "Tamil Nadu",
        "district": "Cuddalore",
        "place": "Chidambaram & Kollidam Basin, Cuddalore",
        "lat": 11.7480, "lon": 79.7714,
        "count": 10, "cred": 0.90,
        "text": "Kollidam river discharge flooding coastal villages; evacuation shelters activated."
    },

    # --- CENTRAL INDIA & ARID REGIONS ---
    {
        "hazard": "HEAVY_RAINFALL",
        "state": "Madhya Pradesh",
        "district": "Indore",
        "place": "Rajwada, Bada Ganpati & Vijay Nagar, Indore",
        "lat": 22.7196, "lon": 75.8577,
        "count": 18, "cred": 0.95,
        "text": "Torrential urban inundation; Khan river overflow flooding central residential sectors."
    },
    {
        "hazard": "FLASH_FLOOD",
        "state": "Madhya Pradesh",
        "district": "Narmadapuram",
        "place": "Sethani Ghat & Tawa Dam, Narmadapuram",
        "lat": 22.7519, "lon": 77.7289,
        "count": 12, "cred": 0.93,
        "text": "Tawa dam opening 13 gates; Narmada river approaching danger mark at Hoshangabad."
    },
    {
        "hazard": "THUNDERSTORM",
        "state": "Jharkhand",
        "district": "Ranchi",
        "place": "Kanke, Doranda & Subarnarekha Basin, Ranchi",
        "lat": 23.3441, "lon": 85.3096,
        "count": 9, "cred": 0.89,
        "text": "Violent hailstorm and high-density lightning discharges across Chota Nagpur plateau."
    }
]

async def seed_pan_india():
    print(f"[PAN-INDIA SEEDER] Connecting to PostgreSQL PostGIS ({POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB})...")
    conn = await asyncpg.connect(
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )

    now = datetime.now(timezone.utc)
    total_incidents = 0

    print(f"[PAN-INDIA SEEDER] Ingesting disaster reports across {len(PAN_INDIA_DISASTER_ZONES)} national hotspot centers...")

    for zone in PAN_INDIA_DISASTER_ZONES:
        hazard = zone["hazard"]
        state = zone["state"]
        district = zone["district"]
        base_lat = zone["lat"]
        base_lon = zone["lon"]
        base_text = zone["text"]
        cred = zone["cred"]
        count = zone["count"]

        for i in range(count):
            inc_id = uuid.uuid4()
            ext_id = f"seed_{hazard[:3].lower()}_{district[:3].lower()}_{i}_{random.randint(1000, 9999)}"
            lat_jitter = base_lat + random.uniform(-0.06, 0.06)
            lon_jitter = base_lon + random.uniform(-0.06, 0.06)

            source_type = random.choice(["IMD", "CITIZEN", "SOCIAL", "SENSOR", "OPENWEATHER"])
            time_offset_hours = random.uniform(0.5, 48.0)
            obs_time = now - timedelta(hours=time_offset_hours)

            try:
                await conn.execute("""
                    INSERT INTO weather.incidents (
                        incident_id, external_id, source_type, source_name,
                        observed_at, geom, geo_method, admin_district, admin_state,
                        place_label, raw_text, reported_category, ai_category,
                        credibility_score, verification_status, content_hash
                    ) VALUES (
                        $1, $2, $3::weather.source_type_enum, 'NationalHazardAggregator',
                        $4, ST_SetSRID(ST_Point($5, $6), 4326)::geography,
                        'GPS_PAYLOAD'::weather.geo_method_enum, $7, $8, $9, $10,
                        $11::weather.hazard_category_enum, $12, $13, 'CONFIRMED', $14
                    )
                    ON CONFLICT (incident_id) DO NOTHING;
                """, inc_id, ext_id, source_type, obs_time, lon_jitter, lat_jitter,
                     district, state, zone["place"], f"[{source_type}] {base_text}",
                     hazard, hazard, cred, uuid.uuid4().hex)
                total_incidents += 1
            except Exception as e:
                # Log insert errors if any
                pass

    print(f"[PAN-INDIA SEEDER] Successfully ingested {total_incidents} incident reports across India!")

    # Now compute PostGIS DBSCAN Hotspot Clusters across all 38 zones
    print("[PAN-INDIA SEEDER] Executing PostGIS ST_ClusterDBSCAN spatial aggregation...")
    await conn.execute("DELETE FROM weather.disaster_hotspots;")

    cluster_insert_query = """
        WITH clustered AS (
            SELECT 
                incident_id,
                COALESCE(ai_category, 'GENERAL_WEATHER') AS hazard_type,
                admin_state,
                admin_district,
                place_label,
                credibility_score,
                observed_at,
                geom::geometry AS geom_g,
                ST_ClusterDBSCAN(geom::geometry, eps := 0.75, minpoints := 1) OVER(
                    PARTITION BY COALESCE(ai_category, 'GENERAL_WEATHER')
                ) AS cluster_id
            FROM weather.incidents
            WHERE geom IS NOT NULL
              AND observed_at >= NOW() - INTERVAL '7 days'
        ),
        grouped AS (
            SELECT 
                hazard_type,
                cluster_id,
                COUNT(*) AS cnt,
                ROUND(AVG(credibility_score)::numeric, 3) AS avg_cred,
                MIN(observed_at) AS min_time,
                MAX(observed_at) AS max_time,
                MODE() WITHIN GROUP (ORDER BY COALESCE(place_label, admin_district || ', ' || admin_state)) AS top_place,
                ST_Centroid(ST_Collect(geom_g)) AS center_pt,
                CASE 
                    WHEN COUNT(*) >= 3 AND ST_GeometryType(ST_ConvexHull(ST_Collect(geom_g))) = 'ST_Polygon'
                        THEN ST_ConvexHull(ST_Collect(geom_g))
                    ELSE 
                        ST_Buffer(ST_Centroid(ST_Collect(geom_g))::geography, 18000)::geometry
                END AS poly_geom
            FROM clustered
            WHERE cluster_id IS NOT NULL
            GROUP BY hazard_type, cluster_id
        )
        INSERT INTO weather.disaster_hotspots (
            hotspot_id, hazard_type, severity, incident_count,
            avg_credibility, centroid, convex_hull, status,
            first_reported_at, last_reported_at
        )
        SELECT 
            gen_random_uuid(),
            hazard_type,
            CASE 
                WHEN hazard_type IN ('CYCLONE', 'LANDSLIDE') OR cnt >= 15 THEN 'CRITICAL'
                WHEN cnt >= 6 THEN 'HIGH'
                WHEN cnt >= 3 THEN 'MEDIUM'
                ELSE 'LOW'
            END,
            cnt,
            ROUND(avg_cred::numeric, 2),
            ST_SetSRID(center_pt, 4326)::geometry,
            ST_SetSRID(poly_geom, 4326)::geometry,
            'ACTIVE',
            min_time,
            max_time
        FROM grouped;
    """

    res = await conn.execute(cluster_insert_query)
    await conn.close()

    print(f"[PAN-INDIA SEEDER] SUCCESS! Result: {res}")

if __name__ == "__main__":
    asyncio.run(seed_pan_india())
