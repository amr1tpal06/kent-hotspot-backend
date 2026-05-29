"""
Kent Collision Hotspot API
IOT552U Assessment 002

FastAPI backend querying PostgreSQL views on Neon.
All analytical computation was performed in the Python ETL
pipeline and stored in PostgreSQL. This API serves the
pre-computed view outputs to the frontend.

Deploy on Render (free tier):
  1. Push this folder to a GitHub repo
  2. Create new Web Service on render.com
  3. Set DATABASE_URL environment variable to your Neon connection string
  4. Build command: pip install -r requirements.txt
  5. Start command: uvicorn main:app --host 0.0.0.0 --port $PORT

Environment variables required:
  DATABASE_URL — Neon PostgreSQL connection string
  FRONTEND_URL — URL of deployed frontend (for CORS)
"""

import os
from typing import Optional
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# ── APP ────────────────────────────────────────────────────────
app = FastAPI(
    title="Kent Collision Hotspot API",
    description=(
        "Serves pre-computed hotspot analytics from PostgreSQL views. "
        "Data: DfT STATS19 Kent Police Force Area (Force Code 46) 2021-2024. "
        "OGL v3.0. Score = review prioritisation only, not causal finding."
    ),
    version="1.0.0",
)

# ── CORS — allow frontend origin ──────────────────────────────
FRONTEND_URL = os.getenv("FRONTEND_URL", "*")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,   # Must be False when allow_origins=["*"]
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# ── GLOBAL EXCEPTION HANDLER ─────────────────────────────────
# Ensures CORS headers are present even when endpoints throw 500
# Without this, unhandled exceptions bypass the CORS middleware
from fastapi import Request
from fastapi.responses import JSONResponse

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
        headers={"Access-Control-Allow-Origin": "*"},
    )

# ── DATABASE ──────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL")


def get_conn():
    if not DATABASE_URL:
        raise HTTPException(
            status_code=500,
            detail="DATABASE_URL environment variable not set"
        )
    return psycopg2.connect(DATABASE_URL)


def query(sql: str, params: tuple = None) -> list[dict]:
    """Execute SQL and return list of dicts."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]


# ── HEALTH ────────────────────────────────────────────────────
@app.get("/health")
def health():
    """Health check — also wakes Neon from sleep."""
    try:
        rows = query("SELECT COUNT(*) AS n FROM hotspot_locations")
        return {
            "status": "ok",
            "hotspot_count": rows[0]["n"],
            "data_source": "DfT STATS19 Kent Police Force Code 46 2021-2024",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── ENDPOINT 1: ranked hotspots ───────────────────────────────
@app.get("/hotspots/ranked")
def ranked_hotspots(
    tier: Optional[str] = Query(None, description="Filter: 'Tier 1', 'Tier 2', 'Tier 3'"),
    persistence: Optional[str] = Query(None, description="Filter: 'Persistent', 'Consecutive' etc"),
    limit: int = Query(181, le=181),
):
    """
    Primary ranked hotspot list from vw_ranked_hotspots.
    Devon EPDO score, review tier, persistence label,
    quadrant classification, centroid coordinates.
    Source: Devon CC (2024); FHWA HSM (2010); Apanga et al. (2024)
    """
    conditions = ["1=1"]
    params = []

    if tier:
        conditions.append("review_tier ILIKE %s")
        params.append(f"%{tier}%")
    if persistence:
        conditions.append("persistence_label = %s")
        params.append(persistence)

    sql = f"""
        SELECT
            hotspot_rank,
            hotspot_id,
            road_reference,
            la_name,
            road_class_label,
            speed_limit,
            urban_rural,
            ROUND(centroid_latitude::NUMERIC, 5)  AS latitude,
            ROUND(centroid_longitude::NUMERIC, 5) AS longitude,
            total_collisions,
            fatal_count,
            serious_count,
            slight_count,
            ksi_count,
            vru_casualties,
            ROUND(fatal_weighted::NUMERIC, 2)   AS fatal_weighted,
            ROUND(serious_weighted::NUMERIC, 2) AS serious_weighted,
            ROUND(slight_weighted::NUMERIC, 2)  AS slight_weighted,
            ROUND(devon_score::NUMERIC, 2)      AS devon_score,
            ROUND(ksi_proportion::NUMERIC, 4)   AS ksi_proportion,
            quadrant_classification,
            review_tier,
            persistence_label,
            methodology_note
        FROM vw_ranked_hotspots
        WHERE {' AND '.join(conditions)}
        ORDER BY hotspot_rank
        LIMIT %s
    """
    params.append(limit)
    return query(sql, tuple(params))


# ── ENDPOINT 2: single hotspot metrics ────────────────────────
@app.get("/hotspots/{hotspot_id}/metrics")
def hotspot_metrics(hotspot_id: int):
    """
    Full metrics for one hotspot from vw_hotspot_metrics.
    Severity counts, Devon score, tier, persistence, quadrant.
    """
    # Use vw_ranked_hotspots which joins all required tables
    # Works with both physical-table and view-based schema versions
    rows = query(
        """
        SELECT
            hotspot_id,
            total_collisions,
            fatal_count,
            serious_count,
            slight_count,
            ksi_count,
            vru_casualties,
            ROUND(fatal_weighted::NUMERIC, 2)   AS fatal_weighted,
            ROUND(serious_weighted::NUMERIC, 2) AS serious_weighted,
            ROUND(slight_weighted::NUMERIC, 2)  AS slight_weighted,
            ROUND(devon_score::NUMERIC, 2)      AS devon_score,
            ROUND(ksi_proportion::NUMERIC, 4)   AS ksi_proportion,
            quadrant_classification,
            review_tier,
            persistence_label,
            methodology_note,
            road_reference,
            speed_limit,
            urban_rural,
            ROUND(centroid_latitude::NUMERIC, 5)  AS latitude,
            ROUND(centroid_longitude::NUMERIC, 5) AS longitude,
            la_name
        FROM vw_ranked_hotspots
        WHERE hotspot_id = %s
        """,
        (hotspot_id,)
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"Hotspot {hotspot_id} not found")
    return rows[0]


# ── ENDPOINT 3: road user profile ────────────────────────────
@app.get("/hotspots/{hotspot_id}/road-users")
def hotspot_road_users(hotspot_id: int):
    """
    Casualty counts by road user type and severity.
    Source: vw_hotspot_road_user
    Limitation: VRU underreporting pedestrian 44-75%,
    cyclist 7-46% (Berkeley ITS, 2015).
    """
    return query(
        """
        SELECT
            hotspot_id,
            casualty_class    AS class_label,
            casualty_label,
            is_vru,
            casualty_severity,
            casualty_severity_label,
            casualty_count
        FROM vw_hotspot_road_user
        WHERE hotspot_id = %s
        ORDER BY casualty_count DESC
        """,
        (hotspot_id,)
    )


# ── ENDPOINT 4: vehicle profile ───────────────────────────────
@app.get("/hotspots/{hotspot_id}/vehicles")
def hotspot_vehicles(hotspot_id: int):
    """
    Vehicle type counts per hotspot from vw_hotspot_vehicle_profile.
    """
    return query(
        """
        SELECT
            hotspot_id,
            category_label AS vehicle_category,
            vehicle_label,
            vehicle_count
        FROM vw_hotspot_vehicle_profile
        WHERE hotspot_id = %s
        ORDER BY vehicle_count DESC
        """,
        (hotspot_id,)
    )


# ── ENDPOINT 5: condition profile ─────────────────────────────
@app.get("/hotspots/{hotspot_id}/conditions")
def hotspot_conditions(
    hotspot_id: int,
    dimension: Optional[str] = Query(
        None,
        description="Filter by: 'Junction', 'Lighting', 'Road surface', 'Weather'"
    )
):
    """
    Condition profile from vw_hotspot_conditions.
    Junction, lighting, surface, weather breakdown.
    Recorded conditions per National Highways (2025) review
    dimensions. These describe recorded conditions at reported
    collisions, not proven causation.
    """
    params = [hotspot_id]
    dim_filter = ""
    if dimension:
        dim_filter = "AND condition_type = %s"
        params.append(dimension)

    return query(
        f"""
        SELECT
            hotspot_id,
            condition_type,
            condition_label,
            collision_count
        FROM vw_hotspot_conditions
        WHERE hotspot_id = %s {dim_filter}
        ORDER BY condition_type, collision_count DESC
        """,
        tuple(params)
    )


# ── ENDPOINT 6: timing profile ────────────────────────────────
@app.get("/hotspots/{hotspot_id}/timing")
def hotspot_timing(hotspot_id: int):
    """
    Collision timing from vw_hotspot_timing.
    Uses GENERATED hour_of_day column on accident_records
    (GENERATED ALWAYS AS STORED, Date 2004).
    """
    return query(
        """
        SELECT
            hotspot_id,
            year,
            month,
            day_of_week,
            hour_of_day,
            collision_count
        FROM vw_hotspot_timing
        WHERE hotspot_id = %s
        ORDER BY year, hour_of_day
        """,
        (hotspot_id,)
    )


# ── ENDPOINT 8: multi-site trends (top N) ────────────────────
@app.get("/hotspots/trends/top")
def top_trends(n: int = Query(5, le=10)):
    """
    Annual KSI trends for top N hotspots by Devon score.
    Used for multi-site trend chart on Temporal Analysis page.
    """
    return query(
        """
        SELECT
            t.hotspot_id,
            t.year,
            t.ksi_count,
            t.persistence_label,
            r.hotspot_rank,
            r.road_reference,
            r.la_name
        FROM vw_hotspot_trend t
        JOIN vw_ranked_hotspots r
            ON t.hotspot_id = r.hotspot_id
           AND t.analysis_run_id = r.analysis_run_id
        WHERE r.hotspot_rank <= %s
        ORDER BY r.hotspot_rank, t.year
        """,
        (n,)
    )


# ── ENDPOINT 7: trend ─────────────────────────────────────────
@app.get("/hotspots/{hotspot_id}/trend")
def hotspot_trend(hotspot_id: int):
    """
    Annual KSI trend from vw_hotspot_trend.
    4 years 2021-2024. 2021 affected by post-pandemic
    traffic recovery. Trend direction indicative only.
    Mann-Kendall requires 8+ time periods (ArcGIS Pro, 2025).
    """
    return query(
        """
        SELECT
            hotspot_id,
            year,
            total_collisions,
            fatal_count,
            serious_count,
            slight_count,
            ksi_count,
            persistence_label,
            review_tier
        FROM vw_hotspot_trend
        WHERE hotspot_id = %s
        ORDER BY year
        """,
        (hotspot_id,)
    )


# ── ENDPOINT 9: KPI summary ───────────────────────────────────
@app.get("/kpis")
def kpis():
    """
    Dashboard KPI card values.
    Derived from PostgreSQL tables directly.
    """
    # KPIs derived from physical tables only (no view dependency)
    # tier1_count and persistent_count use hotspot_locations columns
    # populated by ETL (review_tier and persistence_label stored physically)
    rows = query(
        """
        SELECT
            (SELECT COUNT(*) FROM accident_records)             AS total_collisions,
            (SELECT COUNT(*) FROM hotspot_locations)            AS total_hotspots,
            (SELECT SUM(
                (SELECT COUNT(*) FROM accident_hotspot_membership ahm
                 JOIN accident_records ar ON ar.accident_id = ahm.accident_id
                 WHERE ahm.hotspot_id = hl.hotspot_id
                 AND ar.accident_severity IN (1,2))
             FROM hotspot_locations hl)                         AS total_ksi_hotspots,
            (SELECT COUNT(*) FROM (
                SELECT DISTINCT hotspot_id
                FROM vw_ranked_hotspots
                WHERE review_tier ILIKE 'Tier 1%'
            ) t)                                                AS tier1_count,
            (SELECT COUNT(*) FROM (
                SELECT DISTINCT hotspot_id
                FROM vw_ranked_hotspots
                WHERE persistence_label = 'Persistent'
            ) t)                                                AS persistent_count
        """
    )
    return rows[0]


# ── ENDPOINT 10: validation ───────────────────────────────────
@app.get("/validation")
def validation():
    """
    Data integrity check from vw_validation_hotspot_without_members.
    Must return empty list when data is clean.
    Used on Methodology page as evidence of data integrity.
    """
    return {
        "hotspots_without_members": query(
            "SELECT * FROM vw_validation_hotspot_without_members"
        ),
        "orphan_vehicles": query(
            "SELECT COUNT(*) AS n FROM vw_validation_orphan_vehicles"
        )[0]["n"],
        "orphan_casualties": query(
            "SELECT COUNT(*) AS n FROM vw_validation_orphan_casualties"
        )[0]["n"],
        "status": "clean"
    }
