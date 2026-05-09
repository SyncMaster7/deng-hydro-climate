{% snapshot snap_hydro_stations %}

{{
    config(
        unique_key='station_code',
        strategy='check',
        check_cols=[
            'station_name',
            'station_fullname',
            'station_category',
            'water_body',
            'catchment_name',
            'catchment_size_km2',
            'distance_from_mouth_km',
            'station_altitude_msl_m',
            'latitude',
            'longitude',
            'is_active',
        ],
        target_schema='snapshots',
        dbt_valid_to_current="'9999-12-31'::timestamp",
    )
}}

SELECT
    station_code,
    station_name,
    station_fullname,
    station_category,
    water_body,
    catchment_name,
    catchment_size_km2,
    distance_from_mouth_km,
    station_altitude_msl_m,
    latitude,
    longitude,
    is_active
FROM {{ source('ref', 'hydrometric_stations') }}

{% endsnapshot %}

