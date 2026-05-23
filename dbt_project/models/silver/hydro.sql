-- =============================================================================
-- silver.hydro
-- Pivots bronze.hydro from long format to wide format
-- One row per station per hour with columns for each measurement type
-- Applies EH2000 water level correction using point-in-time snapshot join
-- (snapshots.snap_hydro_stations) — historically correct per measurement row
-- =============================================================================

WITH source AS (
    SELECT
        jaam_kood,
        jaam_nimi,
        jaam_taisnimi,
        valgala_nimi,
        valgala_suurus_km2,
        kaugus_suudmest_km,
        jaam_laiuskraad         AS latitude,
        jaam_pikkuskraad        AS longitude,
        veekogu_nimi,
        timeline_ts_utc,
        timeline_ts_local,
        aegrida_nimi,
        vaartus
    FROM {{ source('bronze', 'hydro') }}
),

pivoted AS (
    SELECT
        jaam_kood,
        jaam_nimi,
        jaam_taisnimi,
        valgala_nimi,
        valgala_suurus_km2,
        kaugus_suudmest_km,
        latitude,
        longitude,
        veekogu_nimi,
        timeline_ts_utc,
        timeline_ts_local,

        -- Water level (cm)
        MAX(CASE WHEN aegrida_nimi = 'WL avg' THEN vaartus END) AS wl_avg,
        MAX(CASE WHEN aegrida_nimi = 'WL min' THEN vaartus END) AS wl_min,
        MAX(CASE WHEN aegrida_nimi = 'WL max' THEN vaartus END) AS wl_max,

        -- Water temperature (°C)
        MAX(CASE WHEN aegrida_nimi = 'WT avg' THEN vaartus END) AS wt_avg,
        MAX(CASE WHEN aegrida_nimi = 'WT min' THEN vaartus END) AS wt_min,
        MAX(CASE WHEN aegrida_nimi = 'WT max' THEN vaartus END) AS wt_max,

        -- Runoff / discharge (m³/s)
        MAX(CASE WHEN aegrida_nimi = 'Äravool avg' THEN vaartus END) AS discharge_avg,
        MAX(CASE WHEN aegrida_nimi = 'Äravool min' THEN vaartus END) AS discharge_min,
        MAX(CASE WHEN aegrida_nimi = 'Äravool max' THEN vaartus END) AS discharge_max

    FROM source
    GROUP BY
        jaam_kood, jaam_nimi, jaam_taisnimi,
        valgala_nimi, valgala_suurus_km2, kaugus_suudmest_km,
        latitude, longitude, veekogu_nimi,
        timeline_ts_utc, timeline_ts_local
),

snap AS (
    -- Point-in-time join against snapshot — picks the station parameters
    -- that were valid at the exact time of each measurement.
    -- If station_altitude_msl_m is corrected, historical rows keep the
    -- altitude that was recorded at the time, not the current value.
    -- NOTE: snapshot only tracks changes from first run (2026-05-09).
    -- For data before that date, falls back to live ref table via COALESCE.
    SELECT
        station_code,
        station_category,
        station_altitude_msl_m,
        dbt_valid_from,
        dbt_valid_to
    FROM {{ ref('snap_hydro_stations') }}
),

ref_stations AS (
    -- Fallback for measurements older than first snapshot run
    SELECT
        station_code,
        station_category,
        station_altitude_msl_m
    FROM {{ source('ref', 'hydrometric_stations') }}
),

final AS (
    SELECT
        p.*,
        COALESCE(s.station_category,       r.station_category)       AS station_category,
        COALESCE(s.station_altitude_msl_m, r.station_altitude_msl_m) AS station_altitude_msl_m,

        -- EH2000 absolute water level (metres above EH2000 datum):
        -- wl_avg is in cm — divide by 100 to convert to metres
        -- station_altitude_msl_m is the gauge zero elevation in EH2000 (metres)
        -- Result: absolute water surface elevation in metres (EH2000)
        -- Verified: Vasknarva wl_avg=83.5cm → (0.835 + 29.18) = 30.015m ≈ ilmateenistus 30.02m
        -- NOTE: use wl_avg (raw cm) for precipitation/temperature comparisons — not this column
        CASE
            WHEN COALESCE(s.station_altitude_msl_m, r.station_altitude_msl_m) IS NOT NULL
            THEN (p.wl_avg / 100.0) + COALESCE(s.station_altitude_msl_m, r.station_altitude_msl_m)
            ELSE NULL
        END AS wl_avg_eh2000

    FROM pivoted p
    LEFT JOIN snap s
        ON  p.jaam_kood       = s.station_code
        AND p.timeline_ts_utc >= s.dbt_valid_from::timestamptz
        AND p.timeline_ts_utc <  s.dbt_valid_to::timestamptz
    LEFT JOIN ref_stations r
        ON  p.jaam_kood = r.station_code
)

SELECT * FROM final
