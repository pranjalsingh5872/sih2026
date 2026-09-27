import json
import os
from typing import Optional
from fastapi import APIRouter, Query, HTTPException
import asyncpg

router = APIRouter()

POSTGRES_USER = os.getenv("POSTGRES_USER", "weather")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "weather_dev_pw")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weatherdb")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

_db_pool: Optional[asyncpg.Pool] = None

async def get_pool() -> asyncpg.Pool:
    global _db_pool
    if _db_pool is None:
        _db_pool = await asyncpg.create_pool(
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DB,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            min_size=2,
            max_size=10
        )
    return _db_pool

@router.get("/hotspots", summary="Get Active Disaster Hotspots (GeoJSON)")
async def get_active_hotspots(
    hazard_type: Optional[str] = Query(None, description="Filter by hazard category"),
    min_credibility: float = Query(0.0, ge=0.0, le=1.0, description="Minimum credibility threshold")
):
    pool = await get_pool()
    query = """
    SELECT 
        hotspot_id,
        hazard_type,
        severity,
        incident_count,
        avg_credibility,
        ST_AsGeoJSON(centroid)::json AS centroid_geojson,
        ST_AsGeoJSON(convex_hull)::json AS polygon_geojson,
        first_reported_at,
        last_reported_at
    FROM weather.disaster_hotspots
    WHERE status = 'ACTIVE'
      AND avg_credibility >= $1
      AND ($2::text IS NULL OR hazard_type = $2)
    ORDER BY incident_count DESC, avg_credibility DESC;
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, min_credibility, hazard_type)

    features = []
    for r in rows:
        features.append({
            "type": "Feature",
            "geometry": r["polygon_geojson"] or r["centroid_geojson"],
            "properties": {
                "hotspot_id": str(r["hotspot_id"]),
                "hazard_type": r["hazard_type"],
                "severity": r["severity"],
                "incident_count": r["incident_count"],
                "avg_credibility": r["avg_credibility"],
                "first_reported_at": r["first_reported_at"].isoformat() if r["first_reported_at"] else None,
                "last_reported_at": r["last_reported_at"].isoformat() if r["last_reported_at"] else None,
                "centroid": r["centroid_geojson"]
            }
        })

    return {
        "type": "FeatureCollection",
        "features": features,
        "count": len(features)
    }

@router.get("/incidents", summary="Get Recent Verified Incidents (GeoJSON)")
async def get_verified_incidents(
    limit: int = Query(100, ge=1, le=1000),
    hazard_type: Optional[str] = Query(None)
):
    pool = await get_pool()
    query = """
    SELECT 
        incident_id,
        source_type,
        source_name,
        observed_at,
        ST_AsGeoJSON(geom::geometry)::json AS geom_geojson,
        raw_text,
        credibility_score,
        verification_status,
        ai_category
    FROM weather.incidents
    WHERE geom IS NOT NULL
      AND ($1::text IS NULL OR ai_category = $1)
    ORDER BY observed_at DESC
    LIMIT $2;
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, hazard_type, limit)

    features = []
    for r in rows:
        features.append({
            "type": "Feature",
            "geometry": r["geom_geojson"],
            "properties": {
                "incident_id": str(r["incident_id"]),
                "source_type": str(r["source_type"]),
                "source_name": r["source_name"],
                "observed_at": r["observed_at"].isoformat() if r["observed_at"] else None,
                "text": r["raw_text"],
                "credibility_score": r["credibility_score"],
                "verification_status": r["verification_status"],
                "hazard_type": r["ai_category"]
            }
        })

    return {
        "type": "FeatureCollection",
        "features": features,
        "count": len(features)
    }

@router.get("/alerts", summary="Get Emergency Dispatch Alerts")
async def get_emergency_alerts():
    pool = await get_pool()
    query = """
    SELECT 
        hotspot_id,
        hazard_type,
        severity,
        incident_count,
        avg_credibility,
        ST_AsGeoJSON(centroid)::json AS centroid_geojson,
        first_reported_at,
        last_reported_at
    FROM weather.disaster_hotspots
    WHERE status = 'ACTIVE'
      AND (severity IN ('HIGH', 'MEDIUM') OR incident_count >= 3)
      AND avg_credibility >= 0.70
    ORDER BY incident_count DESC, avg_credibility DESC;
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

    alerts = []
    for r in rows:
        alerts.append({
            "alert_id": f"ALERT-{str(r['hotspot_id'])[:8].upper()}",
            "hazard_type": r["hazard_type"],
            "severity": r["severity"],
            "incident_count": r["incident_count"],
            "credibility": r["avg_credibility"],
            "location": r["centroid_geojson"],
            "dispatch_status": "ESCALATED" if r["severity"] == "HIGH" else "WARNING",
            "timestamp": r["last_reported_at"].isoformat() if r["last_reported_at"] else None
        })

    return {
        "total_active_alerts": len(alerts),
        "alerts": alerts
    }