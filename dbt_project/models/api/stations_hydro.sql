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
    case station_category
        when 'coastal'      then 'rannikujaam'
        when 'monitoring'   then 'seirejaam'
        when 'water_level'  then 'hüdromeetriajaam'
        else station_category
    end as station_category,
    is_active
from {{ source('ref', 'hydrometric_stations') }}