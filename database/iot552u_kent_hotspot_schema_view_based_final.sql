-- ============================================================
-- IOT552U Kent STATS19 Collision Hotspot Project
-- Final Clean View-Based Schema
--
-- Design:
-- 16 physical tables + derived SQL views.
--
-- Physical tables store:
--   - STATS19 reference/lookups
--   - cleaned STATS19 facts
--   - ETL/analysis provenance
--   - Python DBSCAN clustering outputs
--
-- SQL views derive:
--   - hotspot metrics
--   - yearly hotspot metrics
--   - ranked/dashboard outputs
--   - validation checks
--
-- Key design decision:
-- hotspot_metrics is NOT a physical table in this version.
-- It is derived as vw_hotspot_metrics because severity counts,
-- Devon score, review tier, persistence label and quadrant
-- classification are deterministic calculations from stored data.
-- ============================================================

-- ============================================================
-- DROP VIEWS
-- ============================================================

DROP VIEW IF EXISTS vw_validation_hotspot_min_cluster_size CASCADE;
DROP VIEW IF EXISTS vw_validation_hotspot_without_members CASCADE;
DROP VIEW IF EXISTS vw_validation_orphan_casualties CASCADE;
DROP VIEW IF EXISTS vw_validation_orphan_vehicles CASCADE;
DROP VIEW IF EXISTS vw_hotspot_vehicle_profile CASCADE;
DROP VIEW IF EXISTS vw_hotspot_trend CASCADE;
DROP VIEW IF EXISTS vw_hotspot_timing CASCADE;
DROP VIEW IF EXISTS vw_hotspot_conditions CASCADE;
DROP VIEW IF EXISTS vw_hotspot_road_user CASCADE;
DROP VIEW IF EXISTS vw_ranked_hotspots CASCADE;
DROP VIEW IF EXISTS vw_hotspot_metrics CASCADE;
DROP VIEW IF EXISTS vw_hotspot_yearly_metrics CASCADE;

-- ============================================================
-- DROP TABLES
-- ============================================================

DROP TABLE IF EXISTS accident_hotspot_membership CASCADE;
DROP TABLE IF EXISTS hotspot_locations CASCADE;
DROP TABLE IF EXISTS analysis_runs CASCADE;
DROP TABLE IF EXISTS casualty_records CASCADE;
DROP TABLE IF EXISTS vehicle_records CASCADE;
DROP TABLE IF EXISTS accident_records CASCADE;
DROP TABLE IF EXISTS casualty_types CASCADE;
DROP TABLE IF EXISTS casualty_classes CASCADE;
DROP TABLE IF EXISTS vehicle_types CASCADE;
DROP TABLE IF EXISTS vehicle_categories CASCADE;
DROP TABLE IF EXISTS weather_conditions CASCADE;
DROP TABLE IF EXISTS road_surface_types CASCADE;
DROP TABLE IF EXISTS lighting_conditions CASCADE;
DROP TABLE IF EXISTS junction_types CASCADE;
DROP TABLE IF EXISTS road_classes CASCADE;
DROP TABLE IF EXISTS local_authorities CASCADE;

-- ============================================================
-- 1. LOOKUP / REFERENCE TABLES
-- 10 physical tables. These support 3NF by separating
-- STATS19 code meanings from fact records.
-- ============================================================

CREATE TABLE local_authorities (
    la_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    la_code VARCHAR(20) NOT NULL UNIQUE,
    la_name VARCHAR(120) NOT NULL,
    authority_type VARCHAR(60),
    is_kcc_highway_authority BOOLEAN DEFAULT TRUE
);

CREATE TABLE road_classes (
    road_class_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    road_class_code VARCHAR(20) NOT NULL UNIQUE,
    road_class_label VARCHAR(100) NOT NULL
);

CREATE TABLE junction_types (
    junction_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    junction_code SMALLINT NOT NULL UNIQUE,
    junction_label VARCHAR(120) NOT NULL
);

CREATE TABLE lighting_conditions (
    lighting_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    lighting_code SMALLINT NOT NULL UNIQUE,
    lighting_label VARCHAR(120) NOT NULL
);

CREATE TABLE road_surface_types (
    surface_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    surface_code SMALLINT NOT NULL UNIQUE,
    surface_label VARCHAR(120) NOT NULL
);

CREATE TABLE weather_conditions (
    weather_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    weather_code SMALLINT NOT NULL UNIQUE,
    weather_label VARCHAR(120) NOT NULL
);

CREATE TABLE vehicle_categories (
    vehicle_category_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category_code VARCHAR(40) NOT NULL UNIQUE,
    category_label VARCHAR(80) NOT NULL
);

CREATE TABLE vehicle_types (
    vehicle_type_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    vehicle_code SMALLINT NOT NULL UNIQUE,
    vehicle_label VARCHAR(120) NOT NULL,
    vehicle_category_id INTEGER NOT NULL REFERENCES vehicle_categories(vehicle_category_id)
);

CREATE TABLE casualty_classes (
    casualty_class_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    class_code SMALLINT NOT NULL UNIQUE,
    class_label VARCHAR(80) NOT NULL
);

CREATE TABLE casualty_types (
    casualty_type_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    casualty_code SMALLINT NOT NULL UNIQUE,
    casualty_label VARCHAR(120) NOT NULL,
    is_vru BOOLEAN NOT NULL DEFAULT FALSE
);

-- ============================================================
-- 2. CORE STATS19 FACT TABLES
-- 3 physical tables preserving the accident-vehicle-casualty
-- structure of STATS19.
-- ============================================================

CREATE TABLE accident_records (
    accident_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    accident_index VARCHAR(40) NOT NULL UNIQUE,

    la_id INTEGER NOT NULL REFERENCES local_authorities(la_id),
    road_class_id INTEGER REFERENCES road_classes(road_class_id),
    junction_id INTEGER NOT NULL REFERENCES junction_types(junction_id),
    lighting_id INTEGER NOT NULL REFERENCES lighting_conditions(lighting_id),
    surface_id INTEGER NOT NULL REFERENCES road_surface_types(surface_id),
    weather_id INTEGER NOT NULL REFERENCES weather_conditions(weather_id),

    accident_date DATE NOT NULL,
    accident_time TIME,
    day_of_week SMALLINT,
    accident_severity SMALLINT NOT NULL,

    severity_label VARCHAR(10)
        GENERATED ALWAYS AS (
            CASE accident_severity
                WHEN 1 THEN 'Fatal'
                WHEN 2 THEN 'Serious'
                WHEN 3 THEN 'Slight'
                ELSE 'Unknown'
            END
        ) STORED,

    year SMALLINT
        GENERATED ALWAYS AS (EXTRACT(YEAR FROM accident_date)::SMALLINT) STORED,

    month SMALLINT
        GENERATED ALWAYS AS (EXTRACT(MONTH FROM accident_date)::SMALLINT) STORED,

    hour_of_day SMALLINT
        GENERATED ALWAYS AS (
            CASE
                WHEN accident_time IS NULL THEN NULL
                ELSE EXTRACT(HOUR FROM accident_time)::SMALLINT
            END
        ) STORED,

    number_of_vehicles SMALLINT NOT NULL,
    number_of_casualties SMALLINT NOT NULL,

    road_reference VARCHAR(80),
    speed_limit SMALLINT,
    urban_rural SMALLINT,

    latitude NUMERIC(10,6) NOT NULL,
    longitude NUMERIC(10,6) NOT NULL,

    CONSTRAINT chk_accident_severity CHECK (accident_severity IN (1,2,3)),
    CONSTRAINT chk_accident_vehicle_count CHECK (number_of_vehicles >= 1),
    CONSTRAINT chk_accident_casualty_count CHECK (number_of_casualties >= 1),
    CONSTRAINT chk_day_of_week CHECK (day_of_week IS NULL OR day_of_week BETWEEN 1 AND 7),
    CONSTRAINT chk_speed_limit CHECK (speed_limit IS NULL OR speed_limit IN (20,30,40,50,60,70)),
    CONSTRAINT chk_urban_rural CHECK (urban_rural IS NULL OR urban_rural IN (1,2)),
    CONSTRAINT chk_kent_coordinates CHECK (
        latitude BETWEEN 51.0 AND 51.6
        AND longitude BETWEEN -0.6 AND 1.6
    )
);

CREATE TABLE vehicle_records (
    vehicle_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    accident_id INTEGER NOT NULL REFERENCES accident_records(accident_id) ON DELETE RESTRICT,
    vehicle_type_id INTEGER NOT NULL REFERENCES vehicle_types(vehicle_type_id),
    vehicle_sequence SMALLINT NOT NULL,

    -- Driver/rider attributes remain here because STATS19 records them
    -- as attributes of a vehicle in a collision, not as persistent people.
    driver_age_band VARCHAR(20),
    driver_sex VARCHAR(20),
    skidding_overturning BOOLEAN,

    CONSTRAINT uq_vehicle_per_accident UNIQUE (accident_id, vehicle_sequence),
    CONSTRAINT chk_vehicle_sequence CHECK (vehicle_sequence >= 1)
);

CREATE TABLE casualty_records (
    casualty_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    accident_id INTEGER NOT NULL REFERENCES accident_records(accident_id) ON DELETE RESTRICT,
    casualty_class_id INTEGER REFERENCES casualty_classes(casualty_class_id),
    casualty_type_id INTEGER NOT NULL REFERENCES casualty_types(casualty_type_id),
    casualty_sequence SMALLINT NOT NULL,
    vehicle_sequence SMALLINT,
    casualty_severity SMALLINT NOT NULL,
    casualty_age_band VARCHAR(20),
    casualty_sex VARCHAR(20),

    CONSTRAINT uq_casualty_per_accident UNIQUE (accident_id, casualty_sequence),
    CONSTRAINT chk_casualty_sequence CHECK (casualty_sequence >= 1),
    CONSTRAINT chk_casualty_severity CHECK (casualty_severity IN (1,2,3))
);

-- ============================================================
-- 3. ANALYSIS PROVENANCE
-- 1 physical table.
-- ============================================================

CREATE TABLE analysis_runs (
    analysis_run_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_dataset VARCHAR(120) NOT NULL DEFAULT 'DfT STATS19 road safety open data',
    source_year_start SMALLINT NOT NULL,
    source_year_end SMALLINT NOT NULL,
    police_force_code SMALLINT NOT NULL,
    cluster_method VARCHAR(80) NOT NULL DEFAULT 'Python DBSCAN haversine',
    cluster_epsilon_m NUMERIC(8,2) NOT NULL,
    min_cluster_size SMALLINT NOT NULL,
    fatal_weight NUMERIC(5,2) NOT NULL DEFAULT 7.10,
    serious_weight NUMERIC(5,2) NOT NULL DEFAULT 4.50,
    slight_weight NUMERIC(5,2) NOT NULL DEFAULT 1.00,
    notes TEXT,

    CONSTRAINT chk_year_range CHECK (source_year_start <= source_year_end),
    CONSTRAINT chk_cluster_epsilon CHECK (cluster_epsilon_m > 0),
    CONSTRAINT chk_min_cluster_size CHECK (min_cluster_size >= 1),
    CONSTRAINT chk_weights_positive CHECK (
        fatal_weight > 0 AND serious_weight > 0 AND slight_weight > 0
    )
);

-- ============================================================
-- 4. PYTHON DBSCAN HOTSPOT OUTPUT TABLES
-- 2 physical tables. These are stored because they are algorithm
-- outputs produced by Python, not direct STATS19 attributes.
-- ============================================================

CREATE TABLE hotspot_locations (
    hotspot_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    analysis_run_id INTEGER NOT NULL REFERENCES analysis_runs(analysis_run_id) ON DELETE CASCADE,
    cluster_id INTEGER NOT NULL,

    la_id INTEGER REFERENCES local_authorities(la_id),
    road_class_id INTEGER REFERENCES road_classes(road_class_id),

    road_reference VARCHAR(80),
    speed_limit SMALLINT,
    urban_rural SMALLINT,

    centroid_latitude NUMERIC(10,6) NOT NULL,
    centroid_longitude NUMERIC(10,6) NOT NULL,

    CONSTRAINT uq_hotspot_cluster_per_run UNIQUE (analysis_run_id, cluster_id),
    CONSTRAINT uq_hotspot_run_pair UNIQUE (analysis_run_id, hotspot_id),
    CONSTRAINT chk_hotspot_speed_limit CHECK (speed_limit IS NULL OR speed_limit IN (20,30,40,50,60,70)),
    CONSTRAINT chk_hotspot_urban_rural CHECK (urban_rural IS NULL OR urban_rural IN (1,2)),
    CONSTRAINT chk_hotspot_coordinates CHECK (
        centroid_latitude BETWEEN 51.0 AND 51.6
        AND centroid_longitude BETWEEN -0.6 AND 1.6
    )
);

CREATE TABLE accident_hotspot_membership (
    analysis_run_id INTEGER NOT NULL,
    hotspot_id INTEGER NOT NULL,
    accident_id INTEGER NOT NULL REFERENCES accident_records(accident_id) ON DELETE RESTRICT,
    distance_to_centroid_m NUMERIC(8,2),

    PRIMARY KEY (analysis_run_id, hotspot_id, accident_id),

    CONSTRAINT fk_membership_hotspot_run
        FOREIGN KEY (analysis_run_id, hotspot_id)
        REFERENCES hotspot_locations(analysis_run_id, hotspot_id)
        ON DELETE CASCADE,

    CONSTRAINT chk_distance_to_centroid
        CHECK (distance_to_centroid_m IS NULL OR distance_to_centroid_m >= 0)
);

-- ============================================================
-- 5. INDEXES
-- ============================================================

CREATE INDEX idx_accident_year ON accident_records(year);
CREATE INDEX idx_accident_hour ON accident_records(hour_of_day);
CREATE INDEX idx_accident_severity ON accident_records(accident_severity);
CREATE INDEX idx_accident_location ON accident_records(latitude, longitude);
CREATE INDEX idx_accident_la ON accident_records(la_id);

CREATE INDEX idx_vehicle_accident ON vehicle_records(accident_id);
CREATE INDEX idx_vehicle_type ON vehicle_records(vehicle_type_id);

CREATE INDEX idx_casualty_accident ON casualty_records(accident_id);
CREATE INDEX idx_casualty_type ON casualty_records(casualty_type_id);
CREATE INDEX idx_casualty_class ON casualty_records(casualty_class_id);
CREATE INDEX idx_casualty_severity ON casualty_records(casualty_severity);

CREATE INDEX idx_membership_hotspot ON accident_hotspot_membership(analysis_run_id, hotspot_id);
CREATE INDEX idx_membership_accident ON accident_hotspot_membership(accident_id);

CREATE INDEX idx_hotspot_run ON hotspot_locations(analysis_run_id);

-- ============================================================
-- 6. DERIVED SQL VIEWS
-- ============================================================

-- ------------------------------------------------------------
-- 6.1 Yearly hotspot metrics
-- Pure aggregation from accident records and hotspot membership.
-- ------------------------------------------------------------

CREATE OR REPLACE VIEW vw_hotspot_yearly_metrics AS
SELECT
    ahm.analysis_run_id,
    ahm.hotspot_id,
    ar.year,

    COUNT(*)::INTEGER AS total_collisions,

    SUM(CASE WHEN ar.accident_severity = 1 THEN 1 ELSE 0 END)::INTEGER AS fatal_count,
    SUM(CASE WHEN ar.accident_severity = 2 THEN 1 ELSE 0 END)::INTEGER AS serious_count,
    SUM(CASE WHEN ar.accident_severity = 3 THEN 1 ELSE 0 END)::INTEGER AS slight_count,
    SUM(CASE WHEN ar.accident_severity IN (1,2) THEN 1 ELSE 0 END)::INTEGER AS ksi_count
FROM accident_hotspot_membership ahm
JOIN accident_records ar
    ON ahm.accident_id = ar.accident_id
GROUP BY
    ahm.analysis_run_id,
    ahm.hotspot_id,
    ar.year;

-- ------------------------------------------------------------
-- 6.2 Hotspot metrics
-- Derived from accident_records, casualty_records, membership,
-- casualty type lookups and analysis run weights.
-- ------------------------------------------------------------

CREATE OR REPLACE VIEW vw_hotspot_metrics AS
WITH base_counts AS (
    SELECT
        ahm.analysis_run_id,
        ahm.hotspot_id,

        COUNT(*)::INTEGER AS total_collisions,

        SUM(CASE WHEN ar.accident_severity = 1 THEN 1 ELSE 0 END)::INTEGER AS fatal_count,
        SUM(CASE WHEN ar.accident_severity = 2 THEN 1 ELSE 0 END)::INTEGER AS serious_count,
        SUM(CASE WHEN ar.accident_severity = 3 THEN 1 ELSE 0 END)::INTEGER AS slight_count,
        SUM(CASE WHEN ar.accident_severity IN (1,2) THEN 1 ELSE 0 END)::INTEGER AS ksi_count
    FROM accident_hotspot_membership ahm
    JOIN accident_records ar
        ON ahm.accident_id = ar.accident_id
    GROUP BY
        ahm.analysis_run_id,
        ahm.hotspot_id
),

vru_counts AS (
    SELECT
        ahm.analysis_run_id,
        ahm.hotspot_id,
        COUNT(*)::INTEGER AS vru_casualties
    FROM accident_hotspot_membership ahm
    JOIN casualty_records cr
        ON ahm.accident_id = cr.accident_id
    JOIN casualty_types ct
        ON cr.casualty_type_id = ct.casualty_type_id
    WHERE ct.is_vru = TRUE
    GROUP BY
        ahm.analysis_run_id,
        ahm.hotspot_id
),

scored AS (
    SELECT
        bc.analysis_run_id,
        bc.hotspot_id,

        bc.total_collisions,
        bc.fatal_count,
        bc.serious_count,
        bc.slight_count,
        bc.ksi_count,
        COALESCE(vc.vru_casualties, 0)::INTEGER AS vru_casualties,

        ROUND((bc.fatal_count * arun.fatal_weight)::NUMERIC, 2) AS fatal_weighted,
        ROUND((bc.serious_count * arun.serious_weight)::NUMERIC, 2) AS serious_weighted,
        ROUND((bc.slight_count * arun.slight_weight)::NUMERIC, 2) AS slight_weighted,

        ROUND((
            bc.fatal_count * arun.fatal_weight
            + bc.serious_count * arun.serious_weight
            + bc.slight_count * arun.slight_weight
        )::NUMERIC, 2) AS devon_score,

        CASE
            WHEN bc.total_collisions = 0 THEN NULL
            ELSE ROUND((bc.ksi_count::NUMERIC / bc.total_collisions), 4)
        END AS ksi_proportion
    FROM base_counts bc
    JOIN analysis_runs arun
        ON bc.analysis_run_id = arun.analysis_run_id
    LEFT JOIN vru_counts vc
        ON bc.analysis_run_id = vc.analysis_run_id
       AND bc.hotspot_id = vc.hotspot_id
),

thresholds AS (
    SELECT
        analysis_run_id,

        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY devon_score) AS p75_score,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY devon_score) AS p50_score,

        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_collisions) AS median_total_collisions,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ksi_proportion) AS median_ksi_proportion
    FROM scored
    GROUP BY analysis_run_id
),

persistence AS (
    SELECT
        analysis_run_id,
        hotspot_id,
        COUNT(*) FILTER (WHERE ksi_count > 0)::INTEGER AS years_with_ksi,
        MIN(year) FILTER (WHERE ksi_count > 0) AS first_ksi_year,
        MAX(year) FILTER (WHERE ksi_count > 0) AS latest_ksi_year,
        MAX(year) AS latest_data_year,

        CASE
            WHEN COUNT(*) FILTER (WHERE ksi_count > 0) >= 3
                THEN 'Persistent'

            WHEN COUNT(*) FILTER (WHERE ksi_count > 0) = 2
                 AND MAX(year) FILTER (WHERE ksi_count > 0)
                     - MIN(year) FILTER (WHERE ksi_count > 0) = 1
                THEN 'Consecutive'

            WHEN COUNT(*) FILTER (WHERE ksi_count > 0) = 1
                 AND MAX(year) FILTER (WHERE ksi_count > 0) >= MAX(year) - 1
                THEN 'Emerging'

            WHEN COUNT(*) FILTER (WHERE ksi_count > 0) >= 1
                THEN 'Sporadic'

            ELSE 'None'
        END AS persistence_label
    FROM vw_hotspot_yearly_metrics
    GROUP BY
        analysis_run_id,
        hotspot_id
)

SELECT
    ROW_NUMBER() OVER (
        PARTITION BY s.analysis_run_id
        ORDER BY s.devon_score DESC, s.hotspot_id
    )::INTEGER AS hotspot_metric_id,

    s.analysis_run_id,
    s.hotspot_id,

    s.total_collisions,
    s.fatal_count,
    s.serious_count,
    s.slight_count,
    s.ksi_count,
    s.vru_casualties,

    s.fatal_weighted,
    s.serious_weighted,
    s.slight_weighted,
    s.devon_score,
    s.ksi_proportion,

    CASE
        WHEN s.total_collisions >= t.median_total_collisions
         AND s.ksi_proportion >= t.median_ksi_proportion
            THEN 'High Frequency / High Severity'

        WHEN s.total_collisions >= t.median_total_collisions
         AND s.ksi_proportion < t.median_ksi_proportion
            THEN 'High Frequency / Low Severity'

        WHEN s.total_collisions < t.median_total_collisions
         AND s.ksi_proportion >= t.median_ksi_proportion
            THEN 'Low Frequency / High Severity'

        ELSE 'Low Frequency / Low Severity'
    END AS quadrant_classification,

    CASE
        WHEN s.devon_score >= t.p75_score
            THEN 'Tier 1 - Highest priority review'
        WHEN s.devon_score >= t.p50_score
            THEN 'Tier 2 - Enhanced monitoring'
        ELSE 'Tier 3 - Standard monitoring'
    END AS review_tier,

    COALESCE(p.persistence_label, 'None') AS persistence_label,

    'Devon severity formula uses analysis_runs weights. Metrics are derived SQL outputs for hotspot review prioritisation and do not prove causation.' AS methodology_note
FROM scored s
JOIN thresholds t
    ON s.analysis_run_id = t.analysis_run_id
LEFT JOIN persistence p
    ON s.analysis_run_id = p.analysis_run_id
   AND s.hotspot_id = p.hotspot_id;

-- ------------------------------------------------------------
-- 6.3 Ranked hotspots
-- Main dashboard view for priority table, map and scatter.
-- ------------------------------------------------------------

CREATE OR REPLACE VIEW vw_ranked_hotspots AS
SELECT
    RANK() OVER (
        PARTITION BY hm.analysis_run_id
        ORDER BY hm.devon_score DESC
    ) AS hotspot_rank,

    h.analysis_run_id,
    h.hotspot_id,
    h.road_reference,

    la.la_name,
    rc.road_class_label,

    h.speed_limit,
    h.urban_rural,
    h.centroid_latitude,
    h.centroid_longitude,

    hm.total_collisions,
    hm.fatal_count,
    hm.serious_count,
    hm.slight_count,
    hm.ksi_count,
    hm.vru_casualties,

    hm.fatal_weighted,
    hm.serious_weighted,
    hm.slight_weighted,
    hm.devon_score,
    hm.ksi_proportion,

    hm.quadrant_classification,
    hm.review_tier,
    hm.persistence_label,
    hm.methodology_note
FROM hotspot_locations h
JOIN vw_hotspot_metrics hm
    ON hm.analysis_run_id = h.analysis_run_id
   AND hm.hotspot_id = h.hotspot_id
LEFT JOIN local_authorities la
    ON h.la_id = la.la_id
LEFT JOIN road_classes rc
    ON h.road_class_id = rc.road_class_id;

-- ------------------------------------------------------------
-- 6.4 Road-user profile
-- ------------------------------------------------------------

CREATE OR REPLACE VIEW vw_hotspot_road_user AS
SELECT
    ahm.analysis_run_id,
    ahm.hotspot_id,

    cc.class_label AS casualty_class,
    ct.casualty_label,
    ct.is_vru,

    cr.casualty_severity,

    CASE cr.casualty_severity
        WHEN 1 THEN 'Fatal'
        WHEN 2 THEN 'Serious'
        WHEN 3 THEN 'Slight'
        ELSE 'Unknown'
    END AS casualty_severity_label,

    COUNT(*)::INTEGER AS casualty_count
FROM accident_hotspot_membership ahm
JOIN casualty_records cr
    ON ahm.accident_id = cr.accident_id
LEFT JOIN casualty_classes cc
    ON cr.casualty_class_id = cc.casualty_class_id
JOIN casualty_types ct
    ON cr.casualty_type_id = ct.casualty_type_id
GROUP BY
    ahm.analysis_run_id,
    ahm.hotspot_id,
    cc.class_label,
    ct.casualty_label,
    ct.is_vru,
    cr.casualty_severity;

-- ------------------------------------------------------------
-- 6.5 Condition profile
-- ------------------------------------------------------------

CREATE OR REPLACE VIEW vw_hotspot_conditions AS
SELECT
    ahm.analysis_run_id,
    ahm.hotspot_id,
    'Junction' AS condition_type,
    jt.junction_label AS condition_label,
    COUNT(*)::INTEGER AS collision_count
FROM accident_hotspot_membership ahm
JOIN accident_records ar ON ahm.accident_id = ar.accident_id
JOIN junction_types jt ON ar.junction_id = jt.junction_id
GROUP BY ahm.analysis_run_id, ahm.hotspot_id, jt.junction_label

UNION ALL

SELECT
    ahm.analysis_run_id,
    ahm.hotspot_id,
    'Lighting' AS condition_type,
    lc.lighting_label AS condition_label,
    COUNT(*)::INTEGER AS collision_count
FROM accident_hotspot_membership ahm
JOIN accident_records ar ON ahm.accident_id = ar.accident_id
JOIN lighting_conditions lc ON ar.lighting_id = lc.lighting_id
GROUP BY ahm.analysis_run_id, ahm.hotspot_id, lc.lighting_label

UNION ALL

SELECT
    ahm.analysis_run_id,
    ahm.hotspot_id,
    'Road surface' AS condition_type,
    rst.surface_label AS condition_label,
    COUNT(*)::INTEGER AS collision_count
FROM accident_hotspot_membership ahm
JOIN accident_records ar ON ahm.accident_id = ar.accident_id
JOIN road_surface_types rst ON ar.surface_id = rst.surface_id
GROUP BY ahm.analysis_run_id, ahm.hotspot_id, rst.surface_label

UNION ALL

SELECT
    ahm.analysis_run_id,
    ahm.hotspot_id,
    'Weather' AS condition_type,
    wc.weather_label AS condition_label,
    COUNT(*)::INTEGER AS collision_count
FROM accident_hotspot_membership ahm
JOIN accident_records ar ON ahm.accident_id = ar.accident_id
JOIN weather_conditions wc ON ar.weather_id = wc.weather_id
GROUP BY ahm.analysis_run_id, ahm.hotspot_id, wc.weather_label;

-- ------------------------------------------------------------
-- 6.6 Timing profile
-- ------------------------------------------------------------

CREATE OR REPLACE VIEW vw_hotspot_timing AS
SELECT
    ahm.analysis_run_id,
    ahm.hotspot_id,

    ar.year,
    ar.month,
    ar.day_of_week,
    ar.hour_of_day,

    COUNT(*)::INTEGER AS collision_count
FROM accident_hotspot_membership ahm
JOIN accident_records ar
    ON ahm.accident_id = ar.accident_id
GROUP BY
    ahm.analysis_run_id,
    ahm.hotspot_id,
    ar.year,
    ar.month,
    ar.day_of_week,
    ar.hour_of_day;

-- ------------------------------------------------------------
-- 6.7 Trend view
-- ------------------------------------------------------------

CREATE OR REPLACE VIEW vw_hotspot_trend AS
SELECT
    hym.analysis_run_id,
    hym.hotspot_id,
    hym.year,

    hym.total_collisions,
    hym.fatal_count,
    hym.serious_count,
    hym.slight_count,
    hym.ksi_count,

    hm.persistence_label,
    hm.review_tier,
    hm.quadrant_classification
FROM vw_hotspot_yearly_metrics hym
JOIN vw_hotspot_metrics hm
    ON hym.analysis_run_id = hm.analysis_run_id
   AND hym.hotspot_id = hm.hotspot_id;

-- ------------------------------------------------------------
-- 6.8 Vehicle profile
-- ------------------------------------------------------------

CREATE OR REPLACE VIEW vw_hotspot_vehicle_profile AS
SELECT
    ahm.analysis_run_id,
    ahm.hotspot_id,
    vc.category_label AS vehicle_category,
    vt.vehicle_label,
    COUNT(*)::INTEGER AS vehicle_count
FROM accident_hotspot_membership ahm
JOIN vehicle_records vr
    ON ahm.accident_id = vr.accident_id
JOIN vehicle_types vt
    ON vr.vehicle_type_id = vt.vehicle_type_id
JOIN vehicle_categories vc
    ON vt.vehicle_category_id = vc.vehicle_category_id
GROUP BY
    ahm.analysis_run_id,
    ahm.hotspot_id,
    vc.category_label,
    vt.vehicle_label;

-- ------------------------------------------------------------
-- 6.9 Validation views
-- ------------------------------------------------------------

CREATE OR REPLACE VIEW vw_validation_orphan_vehicles AS
SELECT v.*
FROM vehicle_records v
LEFT JOIN accident_records a
    ON v.accident_id = a.accident_id
WHERE a.accident_id IS NULL;

CREATE OR REPLACE VIEW vw_validation_orphan_casualties AS
SELECT c.*
FROM casualty_records c
LEFT JOIN accident_records a
    ON c.accident_id = a.accident_id
WHERE a.accident_id IS NULL;

CREATE OR REPLACE VIEW vw_validation_hotspot_without_members AS
SELECT
    h.analysis_run_id,
    h.hotspot_id,
    COUNT(ahm.accident_id)::INTEGER AS member_accidents
FROM hotspot_locations h
LEFT JOIN accident_hotspot_membership ahm
    ON h.analysis_run_id = ahm.analysis_run_id
   AND h.hotspot_id = ahm.hotspot_id
GROUP BY
    h.analysis_run_id,
    h.hotspot_id
HAVING COUNT(ahm.accident_id) = 0;

CREATE OR REPLACE VIEW vw_validation_hotspot_min_cluster_size AS
SELECT
    h.analysis_run_id,
    h.hotspot_id,
    COUNT(ahm.accident_id)::INTEGER AS member_accidents,
    arun.min_cluster_size
FROM hotspot_locations h
JOIN analysis_runs arun
    ON h.analysis_run_id = arun.analysis_run_id
LEFT JOIN accident_hotspot_membership ahm
    ON h.analysis_run_id = ahm.analysis_run_id
   AND h.hotspot_id = ahm.hotspot_id
GROUP BY
    h.analysis_run_id,
    h.hotspot_id,
    arun.min_cluster_size
HAVING COUNT(ahm.accident_id) < arun.min_cluster_size;

-- ============================================================
-- END
-- ============================================================
