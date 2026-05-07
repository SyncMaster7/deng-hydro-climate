-- =============================================================================
-- silver.hydro
-- Pivots bronze.hydro from long format to wide format
-- One row per station per hour with columns for each measurement type
-- Applies coastal water level correction using ref.hydrometric_stations
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
    SELECT
        station_code,
        station_category,
        station_altitude_msl_m
    FROM {{ source('ref', 'hydrometric_stations') }}
),

final AS (
    SELECT
        p.*,
        s.station_category,
        s.station_altitude_msl_m,

        -- Coastal water level correction:
        -- Coastal stations have gauge zero at -500 cm EH2000
        -- Apply offset to make readings comparable to river stations
        CASE
            WHEN s.station_category = 'coastal'
            THEN p.wl_avg + s.station_altitude_msl_m
            ELSE p.wl_avg
        END AS wl_avg_corrected

    FROM pivoted p
    LEFT JOIN stations s ON p.jaam_kood = s.station_code
)

SELECT * FROM final
