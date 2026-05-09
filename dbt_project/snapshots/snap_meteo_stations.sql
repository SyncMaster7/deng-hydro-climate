{% snapshot snap_meteo_stations %}

{{
    config(
        unique_key='station_code',
        strategy='check',
        check_cols=[
            'station_name',
            'station_category',
            'latitude',
            'longitude',
            'altitude_m',
            'is_active',
        ],
        target_schema='snapshots',
        dbt_valid_to_current="'9999-12-31'::timestamp",
    )
}}

SELECT
    station_code,
    station_name,
    station_category,
    latitude,
    longitude,
    altitude_m,
    is_active
FROM {{ source('ref', 'meteorological_stations') }}

{% endsnapshot %}

