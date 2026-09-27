import time
import psycopg2
import uuid

DB_CONFIG = {
    "dbname": "weatherdb",
    "user": "weather",
    "password": "weather_dev_pw",
    "host": "127.0.0.1",
    "port": 5432
}

def run_clustering():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("[PIPELINE WORKER] Running Pan-India ST_ClusterDBSCAN spatial clustering...")
        
        # 1. Purane active hotspots ko clean karein
        cur.execute("DELETE FROM weather.disaster_hotspots;")
        
        # 2. Clustered data ko insert karein strictly geometry type ke saath
        cur.execute("""
            WITH clustered AS (
                SELECT 
                    incident_id,
                    ai_category,
                    credibility_score,
                    observed_at,
                    geom,
                    ST_ClusterDBSCAN(geom::geometry, eps := 1.5, minpoints := 1) OVER(PARTITION BY ai_category) AS cid
                FROM weather.incidents
                WHERE observed_at >= NOW() - INTERVAL '48 hours'
            ),
            grouped AS (
                SELECT 
                    ai_category,
                    cid,
                    COUNT(*) AS cnt,
                    AVG(credibility_score) AS avg_cred,
                    MIN(observed_at) AS min_time,
                    MAX(observed_at) AS max_time,
                    ST_Centroid(ST_Collect(geom::geometry)) AS center_pt,
                    CASE 
                        WHEN COUNT(*) >= 3 AND ST_GeometryType(ST_ConvexHull(ST_Collect(geom::geometry))) = 'ST_Polygon'
                            THEN ST_ConvexHull(ST_Collect(geom::geometry))
                        ELSE 
                            -- Agar point ya line ho toh PostGIS buffer bana kar valid polygon me convert karein
                            ST_Buffer(ST_Centroid(ST_Collect(geom::geometry))::geography, 15000)::geometry
                    END AS poly_geom
                FROM clustered
                GROUP BY ai_category, cid
            )
            INSERT INTO weather.disaster_hotspots (
                hotspot_id, hazard_type, severity, incident_count,
                avg_credibility, centroid, convex_hull, status,
                first_reported_at, last_reported_at
            )
            SELECT 
                gen_random_uuid(),
                ai_category,
                CASE WHEN cnt >= 5 THEN 'HIGH' ELSE 'MEDIUM' END,
                cnt,
                ROUND(avg_cred::numeric, 2),
                ST_SetSRID(center_pt, 4326)::geometry,
                ST_SetSRID(poly_geom, 4326)::geometry,
                'ACTIVE',
                min_time,
                max_time
            FROM grouped;
        """)
        
        inserted = cur.rowcount
        conn.commit()
        cur.close()
        print(f"[PIPELINE WORKER] Generated {inserted} Pan-India regional hotspots across states!")
    except Exception as e:
        print(f"[PIPELINE ERROR] {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    while True:
        run_clustering()
        time.sleep(10)