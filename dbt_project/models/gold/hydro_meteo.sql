-- =============================================================================
-- gold.hydro_meteo
-- Joins silver.hydro with nearest meteorological station data (rank 1)
-- Timestamps converted to Estonian local time (Europe/Tallinn)
-- One row per hydro station per hour
-- =============================================================================

WITH hydro AS (
    SELECT
        jaam_kood               AS hydro_station_code,
        jaam_nimi               AS hydro_station_name,
        jaam_taisnimi           AS hydro_station_fullname,
        valgala_nimi            AS catchment_name,
        valgala_suurus_km2      AS catchment_size_km2,
        kaugus_suudmest_km      AS distance_from_mouth_km,
        latitude                AS hydro_latitude,
        longitude               AS hydro_longitude,
        veekogu_nimi            AS water_body,
        station_category,
        station_altitude_msl_m,
        timeline_ts_utc,

        -- Convert to Estonian local time for display
        timezone('Europe/Tallinn', timeline_ts_utc) AS observation_ts_local,

        wl_avg,
        wl_min,
        wl_max,
        wl_avg_corrected,
        wt_avg,
        wt_min,
        wt_max,
        discharge_avg,
        discharge_min,
        discharge_max

    FROM {{ ref('hydro') }}
),

proximity AS (
    SELECT
        hydro_station_code,
        meteo_station_code,
        distance_km
    FROM {{ source('ref', 'station_proximity') }}
    WHERE proximity_rank = 1
),

meteo AS (
    SELECT
        jaam_kood               AS meteo_station_code,
        jaam_nimi               AS meteo_station_name,
        observation_ts,
        precipitation_mm,
        temp_avg,
        temp_min,
        temp_max,
        humidity_pct,
        pressure_hpa,
        wind_speed_ms,
        wind_gust_ms,
        wind_direction_deg,
        sunshine_duration_min
    FROM {{ ref('meteo') }}
),

meteo_stations AS (
    SELECT
        station_code,
        latitude    AS meteo_latitude,
        longitude   AS meteo_longitude
    FROM {{ source('ref', 'meteorological_stations') }}
),

final AS (
    SELECT
        -- Hydro station info
        h.hydro_station_code,
        h.hydro_station_name,
        h.hydro_station_fullname,
        h.catchment_name,
        h.catchment_size_km2,
        h.distance_from_mouth_km,
        h.hydro_latitude,
        h.hydro_longitude,
        h.water_body,
        h.station_category,
        h.station_altitude_msl_m,

        -- Timestamps
        h.timeline_ts_utc,
        h.observation_ts_local,

        -- Water measurements
        h.wl_avg,
        h.wl_min,
        h.wl_max,
        h.wl_avg_corrected,
        h.wt_avg,
        h.wt_min,
        h.wt_max,
        h.discharge_avg,
        h.discharge_min,
        h.discharge_max,

        -- Nearest meteo station info
        p.meteo_station_code,
        m.jaam_nimi             AS meteo_station_name,
        p.distance_km           AS meteo_distance_km,
        ms.meteo_latitude,
        ms.meteo_longitude,

        -- Meteo measurements
        me.precipitation_mm,
        me.temp_avg             AS air_temp_avg,
        me.temp_min             AS air_temp_min,
        me.temp_max             AS air_temp_max,
        me.humidity_pct,
        me.pressure_hpa,
        me.wind_speed_ms,
        me.wind_gust_ms,
        me.wind_direction_deg,
        me.sunshine_duration_min

    FROM hydro h
    LEFT JOIN proximity p
        ON h.hydro_station_code = p.hydro_station_code
    LEFT JOIN meteo me
        ON p.meteo_station_code = me.meteo_station_code
        AND h.timeline_ts_utc = me.observation_ts
    LEFT JOIN meteo_stations ms
        ON p.meteo_station_code = ms.station_code
    LEFT JOIN (
        SELECT DISTINCT jaam_kood, jaam_nimi
        FROM {{ ref('meteo') }}
    ) m ON p.meteo_station_code = m.jaam_kood
)

SELECT * FROM final
