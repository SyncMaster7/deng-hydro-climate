{{
    config(
        materialized='incremental',
        unique_key=['station_code', 'obs_ts', 'element_code']
    )
}}

select
    jaam_kood      as station_code,
    observation_ts as obs_ts,
    'pr1h'         as element_code,
    precipitation_mm as obs_value,
    now()          as published_at
from {{ ref('meteo') }}
where precipitation_mm is not null
{% if is_incremental() %}
    and observation_ts > (select max(obs_ts) from {{ this }})
{% endif %}

union all
select jaam_kood, observation_ts, 'ta', temp_avg, now()
from {{ ref('meteo') }} where temp_avg is not null
{% if is_incremental() %} and observation_ts > (select max(obs_ts) from {{ this }}) {% endif %}

union all
select jaam_kood, observation_ts, 'tan1h', temp_min, now()
from {{ ref('meteo') }} where temp_min is not null
{% if is_incremental() %} and observation_ts > (select max(obs_ts) from {{ this }}) {% endif %}

union all
select jaam_kood, observation_ts, 'tax1h', temp_max, now()
from {{ ref('meteo') }} where temp_max is not null
{% if is_incremental() %} and observation_ts > (select max(obs_ts) from {{ this }}) {% endif %}

union all
select jaam_kood, observation_ts, 'rh', humidity_pct, now()
from {{ ref('meteo') }} where humidity_pct is not null
{% if is_incremental() %} and observation_ts > (select max(obs_ts) from {{ this }}) {% endif %}

union all
select jaam_kood, observation_ts, 'pa0', pressure_hpa, now()
from {{ ref('meteo') }} where pressure_hpa is not null
{% if is_incremental() %} and observation_ts > (select max(obs_ts) from {{ this }}) {% endif %}

union all
select jaam_kood, observation_ts, 'ws10m', wind_speed_ms, now()
from {{ ref('meteo') }} where wind_speed_ms is not null
{% if is_incremental() %} and observation_ts > (select max(obs_ts) from {{ this }}) {% endif %}

union all
select jaam_kood, observation_ts, 'wsx1h', wind_gust_ms, now()
from {{ ref('meteo') }} where wind_gust_ms is not null
{% if is_incremental() %} and observation_ts > (select max(obs_ts) from {{ this }}) {% endif %}

union all
select jaam_kood, observation_ts, 'wd10m', wind_direction_deg, now()
from {{ ref('meteo') }} where wind_direction_deg is not null
{% if is_incremental() %} and observation_ts > (select max(obs_ts) from {{ this }}) {% endif %}

union all
select jaam_kood, observation_ts, 'sdur1h', sunshine_duration_min, now()
from {{ ref('meteo') }} where sunshine_duration_min is not null
{% if is_incremental() %} and observation_ts > (select max(obs_ts) from {{ this }}) {% endif %}
