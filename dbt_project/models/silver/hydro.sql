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

stations AS (
    -- Point-in-time join against snapshot — picks the station parameters
    -- that were valid at the exact time of each measurement.
    -- If station_altitude_msl_m is corrected, historical rows keep the
    -- altitude that was recorded at the time, not the current value.
    SELECT
        station_code,
        station_category,
        station_altitude_msl_m,
        dbt_valid_from,
        dbt_valid_to
    FROM {{ ref('snap_hydro_stations') }}
),

final AS (
    SELECT
        p.*,
        s.station_category,
        s.station_altitude_msl_m,

        -- EH2000 water level correction:
        -- station_altitude_msl_m is the gauge zero elevation relative to EH2000
        -- Adding it converts the raw reading to EH2000 (Amsterdam zero)
        -- Applies to all stations with a known gauge zero, not just coastal
        -- Verified against official ilmateenistus.ee values:
        --   Vihterpalu (monitoring, +5.56m): wl_avg + 5.56 = EH2000
        --   Narva linn  (monitoring, -0.90m): wl_avg - 0.90 = EH2000
        CASE
            WHEN s.station_altitude_msl_m IS NOT NULL
            THEN p.wl_avg + s.station_altitude_msl_m
            ELSE p.wl_avg
        END AS wl_avg_corrected

    FROM pivoted p
    LEFT JOIN stations s
        ON  p.jaam_kood       = s.station_code
        AND p.timeline_ts_utc >= s.dbt_valid_from::timestamptz
        AND p.timeline_ts_utc <  s.dbt_valid_to::timestamptz
)

SELECT * FROM final
