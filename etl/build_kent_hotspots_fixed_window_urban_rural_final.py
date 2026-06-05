#!/usr/bin/env python3
"""
build_kent_hotspots_view_based_final.py

Final view-based ETL for IOT552U Kent STATS19 hotspot project.

This script outputs only physical-table CSVs for the clean schema:

PHYSICAL TABLE OUTPUTS
----------------------
Lookup/reference:
    local_authorities
    road_classes
    junction_types
    lighting_conditions
    road_surface_types
    weather_conditions
    vehicle_categories
    vehicle_types
    casualty_classes
    casualty_types

Core STATS19 facts:
    accident_records
    vehicle_records
    casualty_records

Provenance:
    analysis_runs

Python spatial-screening outputs:
    hotspot_locations
    accident_hotspot_membership

NOT OUTPUT BY PYTHON
--------------------
    hotspot_metrics
    hotspot_yearly_metrics

These are derived in PostgreSQL views:
    vw_hotspot_metrics
    vw_hotspot_yearly_metrics

Why:
    - Python is used for ETL and fixed-window spatial screening.
    - PostgreSQL stores cleaned facts and cluster membership.
    - SQL views derive severity counts, Devon scores, tiers, persistence and trends.
"""

from __future__ import annotations

import argparse
import math
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

warnings.filterwarnings("ignore", category=FutureWarning)

EARTH_RADIUS_M = 6_371_000

FORCE_CODE = 46
STUDY_YEARS = [2021, 2022, 2023, 2024]
DEVON_FATAL = 7.1
DEVON_SERIOUS = 4.5
DEVON_SLIGHT = 1.0
EPS_M = 25.0

# Kent-style fixed-window thresholds:
# 1 = Urban, 2 = Rural in STATS19 urban_or_rural_area.
# A 25m radius represents a 50m diameter review window.
URBAN_MIN_CLUSTER = 6
RURAL_MIN_CLUSTER = 4

# Stored in analysis_runs.min_cluster_size as the minimum valid threshold.
# Urban/rural-specific thresholds are recorded in analysis_runs.notes.
MIN_CLUSTER = RURAL_MIN_CLUSTER

COLUMN_ALIASES: Dict[str, List[str]] = {
    "accident_index": ["accident_index", "collision_index", "accident_reference", "Accident_Index"],
    "police_force": ["police_force", "Police_Force", "police_force_code"],
    "accident_year": ["accident_year", "collision_year", "Accident_Year", "year"],
    "accident_date": ["date", "accident_date", "Date"],
    "accident_time": ["time", "accident_time", "Time"],
    "day_of_week": ["day_of_week", "Day_of_Week"],
    "accident_severity": ["accident_severity", "collision_severity", "Accident_Severity"],
    "number_of_vehicles": ["number_of_vehicles", "Number_of_Vehicles"],
    "number_of_casualties": ["number_of_casualties", "Number_of_Casualties"],
    "latitude": ["latitude", "Latitude", "lat"],
    "longitude": ["longitude", "Longitude", "lon"],
    "local_authority_district": [
        "local_authority_district",
        "Local_Authority_(District)",
        "local_authority_ons_district",
        "local_authority_highway",
    ],
    "first_road_class": ["first_road_class", "1st_Road_Class", "road_class"],
    "first_road_number": ["first_road_number", "1st_Road_Number", "road_number"],
    "speed_limit": ["speed_limit", "Speed_limit"],
    "junction_detail": ["junction_detail", "Junction_Detail"],
    "light_conditions": ["light_conditions", "Light_Conditions"],
    "weather_conditions": ["weather_conditions", "Weather_Conditions"],
    "road_surface_conditions": ["road_surface_conditions", "Road_Surface_Conditions"],
    "urban_or_rural_area": ["urban_or_rural_area", "Urban_or_Rural_Area", "urban_rural"],
    "vehicle_reference": ["vehicle_reference", "Vehicle_Reference"],
    "vehicle_type": ["vehicle_type", "Vehicle_Type"],
    "driver_sex": ["sex_of_driver", "Sex_of_Driver", "driver_sex"],
    "driver_age": ["age_of_driver", "Age_of_Driver", "driver_age"],
    "skidding_overturning": ["skidding_and_overturning", "Skidding_and_Overturning"],
    "casualty_reference": ["casualty_reference", "Casualty_Reference"],
    "casualty_class": ["casualty_class", "Casualty_Class"],
    "casualty_type": ["casualty_type", "Casualty_Type"],
    "casualty_severity": ["casualty_severity", "Casualty_Severity"],
    "casualty_sex": ["sex_of_casualty", "Sex_of_Casualty"],
    "casualty_age": ["age_of_casualty", "Age_of_Casualty"],
}

ROAD_CLASSES = {
    -1: "Unknown",
    1: "Motorway",
    2: "A(M)",
    3: "A",
    4: "B",
    5: "C",
    6: "Unclassified",
}

JUNCTION_TYPES = {
    -1: "Data missing or out of range",
    0: "Not at junction or within 20 metres",
    1: "Roundabout",
    2: "Mini-roundabout",
    3: "T or staggered junction",
    5: "Slip road",
    6: "Crossroads",
    7: "More than 4 arms (not roundabout)",
    8: "Private drive or entrance",
    9: "Other junction",
}

LIGHTING_CONDITIONS = {
    -1: "Data missing or out of range",
    1: "Daylight",
    4: "Darkness - lights lit",
    5: "Darkness - lights unlit",
    6: "Darkness - no lighting",
    7: "Darkness - lighting unknown",
}

ROAD_SURFACE_TYPES = {
    -1: "Data missing or out of range",
    1: "Dry",
    2: "Wet or damp",
    3: "Snow",
    4: "Frost or ice",
    5: "Flood over 3cm deep",
    6: "Oil or diesel",
    7: "Mud",
}

WEATHER_CONDITIONS = {
    -1: "Data missing or out of range",
    1: "Fine no high winds",
    2: "Raining no high winds",
    3: "Snowing no high winds",
    4: "Fine and high winds",
    5: "Raining and high winds",
    6: "Snowing and high winds",
    7: "Fog or mist",
    8: "Other",
    9: "Unknown",
}

VEHICLE_TYPE_DETAIL = {
    -1: ("Data missing or out of range", "unknown", "Unknown"),
    1: ("Pedal cycle", "cycle", "Cycle"),
    2: ("Motorcycle 50cc and under", "motorcycle", "Motorcycle"),
    3: ("Motorcycle 125cc and under", "motorcycle", "Motorcycle"),
    4: ("Motorcycle over 125cc and up to 500cc", "motorcycle", "Motorcycle"),
    5: ("Motorcycle over 500cc", "motorcycle", "Motorcycle"),
    8: ("Taxi or private hire car", "car", "Car"),
    9: ("Car", "car", "Car"),
    10: ("Minibus", "bus", "Bus/coach"),
    11: ("Bus or coach", "bus", "Bus/coach"),
    16: ("Ridden horse", "other", "Other"),
    17: ("Agricultural vehicle", "other", "Other"),
    18: ("Tram", "other", "Other"),
    19: ("Van or goods 3.5 tonnes mgw or under", "lgv", "Light goods vehicle"),
    20: ("Goods over 3.5t and under 7.5t", "hgv", "Heavy goods vehicle"),
    21: ("Goods 7.5 tonnes mgw and over", "hgv", "Heavy goods vehicle"),
    22: ("Mobility scooter", "other", "Other"),
    23: ("Electric motorcycle", "motorcycle", "Motorcycle"),
    90: ("Other vehicle", "other", "Other"),
    97: ("Motorcycle - unknown cc", "motorcycle", "Motorcycle"),
    98: ("Goods vehicle - unknown weight", "hgv", "Heavy goods vehicle"),
    99: ("Unknown or other", "unknown", "Unknown"),
}

CASUALTY_CLASSES = {
    -1: "Unknown",
    1: "Driver/rider",
    2: "Passenger",
    3: "Pedestrian",
}

CASUALTY_TYPE_DETAIL = {
    -1: ("Unknown", False),
    0: ("Pedestrian", True),
    1: ("Cyclist", True),
    2: ("Motorcycle 50cc and under - rider", True),
    3: ("Motorcycle 125cc and under - rider", True),
    4: ("Motorcycle over 125cc and up to 500cc - rider", True),
    5: ("Motorcycle over 500cc - rider", True),
    8: ("Taxi or private hire car occupant", False),
    9: ("Car occupant", False),
    10: ("Minibus occupant", False),
    11: ("Bus or coach occupant", False),
    16: ("Horse rider", True),
    17: ("Agricultural vehicle occupant", False),
    18: ("Tram occupant", False),
    19: ("Van occupant", False),
    20: ("Goods vehicle occupant over 3.5t", False),
    21: ("Mobility scooter rider", True),
    22: ("Electric motorcycle rider", True),
    90: ("Other vehicle occupant", False),
    97: ("Motorcycle rider - unknown cc", True),
    98: ("Goods vehicle occupant - unknown weight", False),
    99: ("Unknown", False),
}


@dataclass
class ETLConfig:
    accidents_path: Path
    vehicles_path: Path
    casualties_path: Path
    output_dir: Path
    force_code: int = FORCE_CODE
    start_year: int = 2021
    end_year: int = 2024
    eps_m: float = EPS_M
    min_cluster_size: int = MIN_CLUSTER
    urban_min_cluster_size: int = URBAN_MIN_CLUSTER
    rural_min_cluster_size: int = RURAL_MIN_CLUSTER
    fatal_weight: float = DEVON_FATAL
    serious_weight: float = DEVON_SERIOUS
    slight_weight: float = DEVON_SLIGHT
    study_years: List[int] = field(default_factory=lambda: STUDY_YEARS)


def standardise_columns(df: pd.DataFrame) -> pd.DataFrame:
    lower_map = {c.lower(): c for c in df.columns}
    rename = {}
    used_targets = set()
    for target, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            actual = lower_map.get(alias.lower())
            if actual and target not in used_targets:
                rename[actual] = target
                used_targets.add(target)
                break
    return df.rename(columns=rename)


def require_columns(df: pd.DataFrame, required: List[str], name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"{name}: missing required columns: {missing}\n"
            f"Available columns: {sorted(df.columns.tolist())}"
        )


def to_int(s: pd.Series, default: int = -1) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(default).astype(int)


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r1 = math.radians(lat1)
    r2 = math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    dp = math.radians(lat2 - lat1)
    a = math.sin(dp / 2) ** 2 + math.cos(r1) * math.cos(r2) * math.sin(dl / 2) ** 2
    return EARTH_RADIUS_M * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def age_band(age: object) -> Optional[str]:
    try:
        a = int(age)
    except Exception:
        return None
    if a < 0:
        return None
    if a <= 15:
        return "0-15"
    if a <= 24:
        return "16-24"
    if a <= 34:
        return "25-34"
    if a <= 44:
        return "35-44"
    if a <= 54:
        return "45-54"
    if a <= 64:
        return "55-64"
    return "65+"


def sex_label(v: object) -> Optional[str]:
    try:
        return {1: "Male", 2: "Female", 3: "Unknown", -1: "Unknown"}.get(int(v), "Unknown")
    except Exception:
        return None


def decode_road_reference(row: pd.Series) -> str:
    cls_code = row.get("first_road_class", -1)
    num = row.get("first_road_number", 0)

    prefix_map = {1: "M", 2: "A", 3: "A", 4: "B", 5: "C"}
    try:
        cls_int = int(cls_code)
        num_int = int(num)
    except (TypeError, ValueError):
        return "Unclassified"

    if cls_int == 2:
        return f"A(M){num_int}" if num_int > 0 else "A(M)"

    prefix = prefix_map.get(cls_int)
    if prefix is None:
        return "Unclassified"
    return f"{prefix}{num_int}" if num_int > 0 else prefix


def filter_and_clean(accidents_raw: pd.DataFrame, config: ETLConfig) -> pd.DataFrame:
    df = accidents_raw.copy()

    df["police_force"] = to_int(df["police_force"])
    df["accident_year"] = to_int(df.get("accident_year", pd.Series(dtype=object)))

    df = df[df["police_force"] == config.force_code].copy()
    df = df[df["accident_year"].between(config.start_year, config.end_year)].copy()

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    df = df[
        df["latitude"].between(51.05, 51.55)
        & df["longitude"].between(-0.55, 1.55)
    ].copy()
    df = df.dropna(subset=["latitude", "longitude"])

    df["accident_date"] = pd.to_datetime(
        df.get("accident_date"), errors="coerce", dayfirst=True
    ).dt.date

    if "accident_time" in df.columns:
        df["accident_time"] = pd.to_datetime(df["accident_time"], errors="coerce").dt.time
    else:
        df["accident_time"] = None

    for col in [
        "day_of_week",
        "accident_severity",
        "number_of_vehicles",
        "number_of_casualties",
        "speed_limit",
        "urban_or_rural_area",
        "junction_detail",
        "light_conditions",
        "weather_conditions",
        "road_surface_conditions",
        "first_road_class",
        "first_road_number",
    ]:
        if col in df.columns:
            df[col] = to_int(df[col])

    df["road_reference"] = df.apply(decode_road_reference, axis=1)

    df = df.dropna(subset=["accident_date"])
    df = df.drop_duplicates(subset=["accident_index"])
    df = df.reset_index(drop=True)

    print(f"  Filtered to {len(df):,} Kent accident records.")
    return df


def build_lookups(accidents: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    la_codes = sorted(
        accidents.get("local_authority_district", pd.Series(dtype=str))
        .fillna("UNKNOWN")
        .astype(str)
        .unique()
    )

    la_names = {
        "E07000225": "Folkestone and Hythe",
        "E07000224": "Dover",
        "E07000227": "Maidstone",
        "E07000228": "Sevenoaks",
        "E07000226": "Gravesham",
        "E07000229": "Swale",
        "E07000223": "Ashford",
        "E07000222": "Canterbury",
        "E07000231": "Tunbridge Wells",
        "E07000230": "Tonbridge and Malling",
        "E07000232": "Thanet",
        "E06000035": "Medway",
        "UNKNOWN": "Unknown",
    }

    local_authorities = pd.DataFrame([
        {
            "la_id": i + 1,
            "la_code": code,
            "la_name": la_names.get(code, code),
            "authority_type": "Unitary authority" if code == "E06000035" else "District",
            "is_kcc_highway_authority": True,
        }
        for i, code in enumerate(la_codes)
    ])

    road_classes = pd.DataFrame([
        {"road_class_id": i + 1, "road_class_code": str(code), "road_class_label": label}
        for i, (code, label) in enumerate(ROAD_CLASSES.items())
    ])

    junction_types = pd.DataFrame([
        {"junction_id": i + 1, "junction_code": code, "junction_label": label}
        for i, (code, label) in enumerate(JUNCTION_TYPES.items())
    ])

    lighting_conditions = pd.DataFrame([
        {"lighting_id": i + 1, "lighting_code": code, "lighting_label": label}
        for i, (code, label) in enumerate(LIGHTING_CONDITIONS.items())
    ])

    road_surface_types = pd.DataFrame([
        {"surface_id": i + 1, "surface_code": code, "surface_label": label}
        for i, (code, label) in enumerate(ROAD_SURFACE_TYPES.items())
    ])

    weather_conditions = pd.DataFrame([
        {"weather_id": i + 1, "weather_code": code, "weather_label": label}
        for i, (code, label) in enumerate(WEATHER_CONDITIONS.items())
    ])

    # vehicle_categories first, then vehicle_types references category IDs.
    category_rows = []
    seen = {}
    for _, (_, cat_code, cat_label) in VEHICLE_TYPE_DETAIL.items():
        if cat_code not in seen:
            seen[cat_code] = len(seen) + 1
            category_rows.append({
                "vehicle_category_id": seen[cat_code],
                "category_code": cat_code,
                "category_label": cat_label,
            })

    vehicle_categories = pd.DataFrame(category_rows)

    vehicle_types = pd.DataFrame([
        {
            "vehicle_type_id": i + 1,
            "vehicle_code": code,
            "vehicle_label": label,
            "vehicle_category_id": seen[cat_code],
        }
        for i, (code, (label, cat_code, _cat_label)) in enumerate(VEHICLE_TYPE_DETAIL.items())
    ])

    casualty_classes = pd.DataFrame([
        {"casualty_class_id": i + 1, "class_code": code, "class_label": label}
        for i, (code, label) in enumerate(CASUALTY_CLASSES.items())
    ])

    casualty_types = pd.DataFrame([
        {
            "casualty_type_id": i + 1,
            "casualty_code": code,
            "casualty_label": label,
            "is_vru": is_vru,
        }
        for i, (code, (label, is_vru)) in enumerate(CASUALTY_TYPE_DETAIL.items())
    ])

    return {
        "local_authorities": local_authorities,
        "road_classes": road_classes,
        "junction_types": junction_types,
        "lighting_conditions": lighting_conditions,
        "road_surface_types": road_surface_types,
        "weather_conditions": weather_conditions,
        "vehicle_categories": vehicle_categories,
        "vehicle_types": vehicle_types,
        "casualty_classes": casualty_classes,
        "casualty_types": casualty_types,
    }


def build_accident_records(accidents: pd.DataFrame, lookups: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    la_map = dict(zip(lookups["local_authorities"]["la_code"].astype(str), lookups["local_authorities"]["la_id"]))
    road_map = dict(zip(lookups["road_classes"]["road_class_code"].astype(str), lookups["road_classes"]["road_class_id"]))
    jcn_map = dict(zip(lookups["junction_types"]["junction_code"], lookups["junction_types"]["junction_id"]))
    ltg_map = dict(zip(lookups["lighting_conditions"]["lighting_code"], lookups["lighting_conditions"]["lighting_id"]))
    srf_map = dict(zip(lookups["road_surface_types"]["surface_code"], lookups["road_surface_types"]["surface_id"]))
    wtr_map = dict(zip(lookups["weather_conditions"]["weather_code"], lookups["weather_conditions"]["weather_id"]))

    acc = accidents.reset_index(drop=True)

    out = pd.DataFrame({
        "accident_id": range(1, len(acc) + 1),
        "accident_index": acc["accident_index"].astype(str),
        "la_id": acc.get("local_authority_district", pd.Series("UNKNOWN", index=acc.index))
            .fillna("UNKNOWN").astype(str).map(la_map).fillna(1).astype(int),
        "road_class_id": acc.get("first_road_class", pd.Series(-1, index=acc.index))
            .astype(str).map(road_map),
        "junction_id": acc.get("junction_detail", pd.Series(-1, index=acc.index))
            .map(jcn_map).fillna(jcn_map.get(-1, 1)).astype(int),
        "lighting_id": acc.get("light_conditions", pd.Series(-1, index=acc.index))
            .map(ltg_map).fillna(ltg_map.get(-1, 1)).astype(int),
        "surface_id": acc.get("road_surface_conditions", pd.Series(-1, index=acc.index))
            .map(srf_map).fillna(srf_map.get(-1, 1)).astype(int),
        "weather_id": acc.get("weather_conditions", pd.Series(-1, index=acc.index))
            .map(wtr_map).fillna(wtr_map.get(-1, 1)).astype(int),
        "accident_date": acc["accident_date"],
        "accident_time": acc.get("accident_time"),
        "day_of_week": acc.get("day_of_week"),
        "accident_severity": acc["accident_severity"].astype(int),
        "number_of_vehicles": acc["number_of_vehicles"].astype(int),
        "number_of_casualties": acc["number_of_casualties"].astype(int),
        "road_reference": acc["road_reference"],
        "speed_limit": acc.get("speed_limit"),
        "urban_rural": acc.get("urban_or_rural_area"),
        "latitude": acc["latitude"].round(6),
        "longitude": acc["longitude"].round(6),
    })

    return out


def build_vehicle_records(
    vehicles_raw: pd.DataFrame,
    accident_records: pd.DataFrame,
    lookups: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    acc_map = dict(zip(accident_records["accident_index"].astype(str), accident_records["accident_id"]))
    vt_map = dict(zip(lookups["vehicle_types"]["vehicle_code"], lookups["vehicle_types"]["vehicle_type_id"]))
    vt_default = vt_map.get(-1, 1)

    veh = standardise_columns(vehicles_raw.copy())
    veh = veh[veh["accident_index"].astype(str).isin(acc_map)].copy()
    veh["vehicle_type_int"] = to_int(veh.get("vehicle_type", pd.Series(-1, index=veh.index)))

    out = pd.DataFrame({
        "vehicle_id": range(1, len(veh) + 1),
        "accident_id": veh["accident_index"].astype(str).map(acc_map),
        "vehicle_type_id": veh["vehicle_type_int"].map(vt_map).fillna(vt_default).astype(int),
        "vehicle_sequence": to_int(veh.get("vehicle_reference", pd.Series(1, index=veh.index)), 1),
        "driver_age_band": veh["driver_age"].apply(age_band) if "driver_age" in veh.columns else None,
        "driver_sex": veh["driver_sex"].apply(sex_label) if "driver_sex" in veh.columns else None,
        "skidding_overturning": (
            (to_int(veh["skidding_overturning"], 0) != 0)
            if "skidding_overturning" in veh.columns
            else None
        ),
    })

    out = out.drop_duplicates(subset=["accident_id", "vehicle_sequence"])
    out["vehicle_id"] = range(1, len(out) + 1)
    return out


def build_casualty_records(
    casualties_raw: pd.DataFrame,
    accident_records: pd.DataFrame,
    lookups: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    acc_map = dict(zip(accident_records["accident_index"].astype(str), accident_records["accident_id"]))
    ct_map = dict(zip(lookups["casualty_types"]["casualty_code"], lookups["casualty_types"]["casualty_type_id"]))
    cc_map = dict(zip(lookups["casualty_classes"]["class_code"], lookups["casualty_classes"]["casualty_class_id"]))
    ct_default = ct_map.get(-1, 1)
    cc_default = cc_map.get(-1, 1)

    cas = standardise_columns(casualties_raw.copy())
    cas = cas[cas["accident_index"].astype(str).isin(acc_map)].copy()

    if "casualty_type" in cas.columns:
        raw_type = to_int(cas["casualty_type"])
    else:
        raw_type = pd.Series(-1, index=cas.index)

    if "casualty_class" in cas.columns:
        raw_class = to_int(cas["casualty_class"])
    else:
        raw_class = pd.Series(-1, index=cas.index)

    out = pd.DataFrame({
        "casualty_id": range(1, len(cas) + 1),
        "accident_id": cas["accident_index"].astype(str).map(acc_map),
        "casualty_class_id": raw_class.map(cc_map).fillna(cc_default).astype(int),
        "casualty_type_id": raw_type.map(ct_map).fillna(ct_default).astype(int),
        "casualty_sequence": to_int(cas.get("casualty_reference", pd.Series(1, index=cas.index)), 1),
        "vehicle_sequence": (
            to_int(cas["vehicle_reference"], -1).replace(-1, pd.NA)
            if "vehicle_reference" in cas.columns
            else None
        ),
        "casualty_severity": to_int(cas["casualty_severity"], 3),
        "casualty_age_band": cas["casualty_age"].apply(age_band) if "casualty_age" in cas.columns else None,
        "casualty_sex": cas["casualty_sex"].apply(sex_label) if "casualty_sex" in cas.columns else None,
    })

    out = out.drop_duplicates(subset=["accident_id", "casualty_sequence"])
    out["casualty_id"] = range(1, len(out) + 1)
    return out


def run_fixed_window_screening(accident_records: pd.DataFrame, config: ETLConfig) -> pd.DataFrame:
    """Build hotspot membership using Kent-style fixed-window screening.

    This replaces the earlier DBSCAN prototype. DBSCAN with min_samples=1
    creates proximity-connected components, which can chain collisions along
    a road segment and produce clusters wider than the intended review window.

    This function uses a stricter fixed-window method:

        1. Treat each collision point as a candidate centre.
        2. Count all collisions within config.eps_m metres of that centre.
        3. Apply the centre collision's STATS19 urban/rural threshold:
              urban centre (urban_rural = 1): config.urban_min_cluster_size
              rural centre (urban_rural = 2): config.rural_min_cluster_size
              unknown/null: config.min_cluster_size fallback
        4. Rank qualifying candidate windows by Devon severity-weighted score,
           then member count.
        5. Deduplicate overlapping windows greedily. The strongest window keeps
           its members; a later overlapping window is accepted only if enough
           still-unassigned collisions remain to meet its own threshold.
        6. Assign each collision to at most one final hotspot.

    The output keeps the same column contract as the old DBSCAN function:
    accident_id, latitude, longitude, accident_severity, accident_date,
    cluster_label, accident_year.
    """

    clusterable = accident_records[
        accident_records["latitude"].notna() & accident_records["longitude"].notna()
    ].copy().reset_index(drop=True)

    coords_rad = np.radians(clusterable[["latitude", "longitude"]].astype(float).to_numpy())
    radius_rad = config.eps_m / EARTH_RADIUS_M

    print(
        f"  Running fixed-window spatial screening: {len(clusterable):,} points, "
        f"radius={config.eps_m}m; "
        f"urban threshold={config.urban_min_cluster_size}, "
        f"rural threshold={config.rural_min_cluster_size}"
    )

    tree = BallTree(coords_rad, metric="haversine")
    neighbour_indices = tree.query_radius(coords_rad, r=radius_rad)

    severity_weights = {
        1: config.fatal_weight,
        2: config.serious_weight,
        3: config.slight_weight,
    }

    def threshold_for_urban_rural(value: object) -> int:
        """Return the threshold for a candidate centre's urban/rural code."""
        try:
            urban_rural = int(value)
        except Exception:
            return int(config.min_cluster_size)

        if urban_rural == 1:
            return int(config.urban_min_cluster_size)
        if urban_rural == 2:
            return int(config.rural_min_cluster_size)
        return int(config.min_cluster_size)

    candidate_rows = []
    severities = clusterable["accident_severity"].astype(int).to_numpy()

    for centre_idx, members in enumerate(neighbour_indices):
        members = np.array(members, dtype=int)
        centre = clusterable.iloc[centre_idx]
        threshold = threshold_for_urban_rural(centre.get("urban_rural"))

        if len(members) < threshold:
            continue

        score = float(sum(severity_weights.get(int(severities[i]), 0.0) for i in members))

        candidate_rows.append({
            "centre_idx": int(centre_idx),
            "member_indices": set(int(i) for i in members),
            "member_count": int(len(members)),
            "devon_score": round(score, 2),
            "centre_latitude": float(centre["latitude"]),
            "centre_longitude": float(centre["longitude"]),
            "urban_rural": int(centre["urban_rural"]) if pd.notna(centre.get("urban_rural")) else None,
            "threshold": int(threshold),
        })

    print(f"  Candidate fixed windows meeting threshold: {len(candidate_rows):,}")

    labels = np.full(len(clusterable), -1, dtype=int)

    if not candidate_rows:
        out = clusterable[["accident_id", "latitude", "longitude", "accident_severity", "accident_date"]].copy()
        out["cluster_label"] = labels
        out["accident_year"] = pd.to_datetime(out["accident_date"]).dt.year.astype(int)
        return out

    candidates = pd.DataFrame(candidate_rows).sort_values(
        by=["devon_score", "member_count", "centre_idx"],
        ascending=[False, False, True],
    ).reset_index(drop=True)

    assigned: set[int] = set()
    final_cluster_id = 0

    for _, candidate in candidates.iterrows():
        members = set(candidate["member_indices"])
        unassigned_members = members - assigned
        threshold = int(candidate["threshold"])

        # Skip duplicate/overlapping windows once fewer than the relevant
        # urban/rural threshold of still-unassigned collisions remains.
        if len(unassigned_members) < threshold:
            continue

        for idx in sorted(unassigned_members):
            labels[idx] = final_cluster_id

        assigned.update(unassigned_members)
        final_cluster_id += 1

    print(f"  Final deduplicated hotspot windows: {final_cluster_id:,}")

    out = clusterable[["accident_id", "latitude", "longitude", "accident_severity", "accident_date"]].copy()
    out["cluster_label"] = labels
    out["accident_year"] = pd.to_datetime(out["accident_date"]).dt.year.astype(int)
    return out

def build_hotspot_outputs(
    clustered: pd.DataFrame,
    accident_records: pd.DataFrame,
    config: ETLConfig,
) -> Dict[str, pd.DataFrame]:
    run_id = 1

    analysis_runs = pd.DataFrame([{
        "analysis_run_id": run_id,
        "run_timestamp": datetime.now().isoformat(timespec="seconds"),
        "source_dataset": "DfT STATS19 road safety open data",
        "source_year_start": config.start_year,
        "source_year_end": config.end_year,
        "police_force_code": config.force_code,
        "cluster_method": "Python fixed-window haversine screening",
        "cluster_epsilon_m": config.eps_m,
        "min_cluster_size": config.min_cluster_size,
        "fatal_weight": config.fatal_weight,
        "serious_weight": config.serious_weight,
        "slight_weight": config.slight_weight,
        "notes": (
            "Python ETL creates cleaned STATS19 tables and fixed-window hotspot membership. "
            f"Candidate windows use {config.eps_m}m radius. "
            f"Urban centres require {config.urban_min_cluster_size}+ collisions; "
            f"rural centres require {config.rural_min_cluster_size}+ collisions. "
            "analysis_runs.min_cluster_size stores the minimum valid threshold for generic validation. "
            "PostgreSQL views derive hotspot metrics, yearly trends and dashboard outputs."
        ),
    }])

    cluster_sizes = clustered.groupby("cluster_label").size()
    qualifying_labels = set(cluster_sizes[cluster_sizes >= config.min_cluster_size].index)
    qualifying_labels.discard(-1)

    members = clustered[clustered["cluster_label"].isin(qualifying_labels)].copy()
    print(f"  Qualifying hotspot windows: {len(qualifying_labels):,}")

    if members.empty:
        raise ValueError("No qualifying hotspots found. Adjust eps_m or min_cluster_size.")

    acc_lookup = accident_records.set_index("accident_id")

    hotspot_location_rows = []
    membership_rows = []

    for hotspot_id, (cluster_label, group) in enumerate(members.groupby("cluster_label"), start=1):
        centroid_lat = float(group["latitude"].mean())
        centroid_lon = float(group["longitude"].mean())

        dists = group.apply(
            lambda r: haversine_m(float(r["latitude"]), float(r["longitude"]), centroid_lat, centroid_lon),
            axis=1,
        )

        rep_id = int(group.loc[dists.idxmin(), "accident_id"])
        rep_row = acc_lookup.loc[rep_id]

        hotspot_location_rows.append({
            "hotspot_id": hotspot_id,
            "analysis_run_id": run_id,
            "cluster_id": int(cluster_label),
            "la_id": int(rep_row.get("la_id", 1)),
            "road_class_id": rep_row.get("road_class_id"),
            "road_reference": rep_row.get("road_reference", "Unclassified"),
            "speed_limit": rep_row.get("speed_limit"),
            "urban_rural": rep_row.get("urban_rural"),
            "centroid_latitude": round(centroid_lat, 6),
            "centroid_longitude": round(centroid_lon, 6),
        })

        for _, r in group.iterrows():
            membership_rows.append({
                "analysis_run_id": run_id,
                "hotspot_id": hotspot_id,
                "accident_id": int(r["accident_id"]),
                "distance_to_centroid_m": round(
                    haversine_m(float(r["latitude"]), float(r["longitude"]), centroid_lat, centroid_lon),
                    2,
                ),
            })

    hotspot_locations = pd.DataFrame(hotspot_location_rows)
    membership = pd.DataFrame(membership_rows)

    print(f"  hotspot_locations: {len(hotspot_locations):,}")
    print(f"  accident_hotspot_membership: {len(membership):,}")

    return {
        "analysis_runs": analysis_runs,
        "hotspot_locations": hotspot_locations,
        "accident_hotspot_membership": membership,
    }


LOAD_ORDER = [
    "local_authorities",
    "road_classes",
    "junction_types",
    "lighting_conditions",
    "road_surface_types",
    "weather_conditions",
    "vehicle_categories",
    "vehicle_types",
    "casualty_classes",
    "casualty_types",
    "accident_records",
    "vehicle_records",
    "casualty_records",
    "analysis_runs",
    "hotspot_locations",
    "accident_hotspot_membership",
]


def write_outputs(output_dir: Path, tables: Dict[str, pd.DataFrame]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    for name in LOAD_ORDER:
        df = tables.get(name)
        if df is None or df.empty:
            print(f"  SKIP {name} (empty)")
            continue
        path = output_dir / f"{name}.csv"
        df.to_csv(path, index=False)
        print(f"  Wrote {path.name} ({len(df):,} rows)")

    load_lines = [
        "-- load_tables.sql",
        "-- Generated by build_kent_hotspots_fixed_window_urban_rural_final.py",
        "-- Run after creating the PostgreSQL schema.",
        "BEGIN;",
        "",
    ]

    for name in LOAD_ORDER:
        path = output_dir / f"{name}.csv"
        if not path.exists():
            continue
        cols = list(tables[name].columns)
        col_list = ", ".join(cols)
        load_lines.append(
            f"\\copy {name} ({col_list}) FROM '{path.as_posix()}' "
            f"WITH (FORMAT csv, HEADER true);"
        )

    load_lines.extend(["", "COMMIT;", ""])
    (output_dir / "load_tables.sql").write_text("\n".join(load_lines), encoding="utf-8")
    print("  Wrote load_tables.sql")


def validate_outputs(tables: Dict[str, pd.DataFrame], min_cluster_size: int) -> None:
    print("\n-- VALIDATION SUMMARY -------------------------------------")
    for name in [
        "accident_records",
        "vehicle_records",
        "casualty_records",
        "hotspot_locations",
        "accident_hotspot_membership",
    ]:
        print(f"  {name:35s} {len(tables.get(name, pd.DataFrame())):>8,}")

    acc = tables.get("accident_records", pd.DataFrame())
    veh = tables.get("vehicle_records", pd.DataFrame())
    cas = tables.get("casualty_records", pd.DataFrame())
    mem = tables.get("accident_hotspot_membership", pd.DataFrame())

    if not acc.empty and not veh.empty:
        orphan_v = (~veh["accident_id"].isin(acc["accident_id"])).sum()
        print(f"  orphan vehicle records: {orphan_v:,}")

    if not acc.empty and not cas.empty:
        orphan_c = (~cas["accident_id"].isin(acc["accident_id"])).sum()
        print(f"  orphan casualty records: {orphan_c:,}")

    if not mem.empty:
        sizes = mem.groupby("hotspot_id").size()
        too_small = int((sizes < min_cluster_size).sum())
        print(f"  hotspots below min_cluster_size: {too_small:,}")
        print(f"  largest hotspot size: {int(sizes.max()):,}")

    print("----------------------------------------------------------\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build view-based Kent STATS19 hotspot outputs.")
    parser.add_argument("--accidents", required=True, type=Path)
    parser.add_argument("--vehicles", required=True, type=Path)
    parser.add_argument("--casualties", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--force-code", default=FORCE_CODE, type=int)
    parser.add_argument("--start-year", default=2021, type=int)
    parser.add_argument("--end-year", default=2024, type=int)
    parser.add_argument("--eps-m", default=EPS_M, type=float)
    parser.add_argument(
        "--min-cluster",
        default=MIN_CLUSTER,
        type=int,
        help="Fallback/minimum threshold used when urban_rural is missing. Default is rural threshold."
    )
    parser.add_argument("--urban-min-cluster", default=URBAN_MIN_CLUSTER, type=int)
    parser.add_argument("--rural-min-cluster", default=RURAL_MIN_CLUSTER, type=int)
    args = parser.parse_args()

    config = ETLConfig(
        accidents_path=args.accidents,
        vehicles_path=args.vehicles,
        casualties_path=args.casualties,
        output_dir=args.output_dir,
        force_code=args.force_code,
        start_year=args.start_year,
        end_year=args.end_year,
        eps_m=args.eps_m,
        min_cluster_size=args.min_cluster,
        urban_min_cluster_size=args.urban_min_cluster,
        rural_min_cluster_size=args.rural_min_cluster,
        study_years=list(range(args.start_year, args.end_year + 1)),
    )

    print("\n== IOT552U Kent STATS19 View-Based ETL ==\n")

    print("Step 1 — Load raw STATS19 files")
    accidents_raw = standardise_columns(pd.read_csv(config.accidents_path, low_memory=False))
    vehicles_raw = standardise_columns(pd.read_csv(config.vehicles_path, low_memory=False))
    casualties_raw = standardise_columns(pd.read_csv(config.casualties_path, low_memory=False))

    require_columns(
        accidents_raw,
        [
            "accident_index",
            "police_force",
            "accident_year",
            "latitude",
            "longitude",
            "accident_severity",
            "number_of_vehicles",
            "number_of_casualties",
        ],
        "accidents",
    )
    require_columns(vehicles_raw, ["accident_index", "vehicle_reference"], "vehicles")
    require_columns(casualties_raw, ["accident_index", "casualty_reference", "casualty_severity"], "casualties")

    print(f"  raw accidents: {len(accidents_raw):,}")
    print(f"  raw vehicles: {len(vehicles_raw):,}")
    print(f"  raw casualties: {len(casualties_raw):,}")

    print("\nStep 2 — Filter and clean accidents")
    accidents_clean = filter_and_clean(accidents_raw, config)

    print("\nStep 3 — Build lookup tables")
    lookups = build_lookups(accidents_clean)
    for name, df in lookups.items():
        print(f"  {name:30s} {len(df):>5,}")

    print("\nStep 4 — Build core fact tables")
    accident_records = build_accident_records(accidents_clean, lookups)
    vehicle_records = build_vehicle_records(vehicles_raw, accident_records, lookups)
    casualty_records = build_casualty_records(casualties_raw, accident_records, lookups)

    print(f"  accident_records: {len(accident_records):,}")
    print(f"  vehicle_records: {len(vehicle_records):,}")
    print(f"  casualty_records: {len(casualty_records):,}")

    print("\nStep 5 — Run fixed-window spatial screening")
    clustered = run_fixed_window_screening(accident_records, config)

    print("\nStep 6 — Build hotspot location and membership outputs")
    hotspot_tables = build_hotspot_outputs(clustered, accident_records, config)

    all_tables = {
        **lookups,
        "accident_records": accident_records,
        "vehicle_records": vehicle_records,
        "casualty_records": casualty_records,
        **hotspot_tables,
    }

    print("\nStep 7 — Write CSV outputs and load script")
    write_outputs(config.output_dir, all_tables)

    print("\nStep 8 — Validate outputs")
    validate_outputs(all_tables, config.min_cluster_size)

    print(f"Done. Outputs written to: {config.output_dir.resolve()}\n")


if __name__ == "__main__":
    main()
