select
    station_code,
    station_name,
    latitude,
    longitude,
    altitude_m,
    is_active
from {{ source('ref', 'meteorological_stations') }}