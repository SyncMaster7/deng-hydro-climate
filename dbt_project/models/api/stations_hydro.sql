select
    station_code,
    station_name,
    station_fullname,
    water_body,
    catchment_name,
    catchment_size_km2,
    distance_from_mouth_km,
    station_altitude_msl_m,
    latitude,
    longitude,
    station_category,
    is_active
from {{ ref('hydrometric_stations') }}
