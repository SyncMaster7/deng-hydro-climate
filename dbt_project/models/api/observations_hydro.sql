{{
    config(
        materialized='incremental',
        unique_key=['station_code', 'obs_ts', 'element_code']
    )
}}

select
    jaam_kood         as station_code,
    timeline_ts_local as obs_ts,
    'wl_avg'          as element_code,
    wl_avg            as obs_value,
    now()             as published_at
from {{ ref('hydro') }}
where wl_avg is not null
{% if is_incremental() %}
    and timeline_ts_local > (select max(obs_ts) from {{ this }})
{% endif %}

union all
select jaam_kood, timeline_ts_local, 'wl_min', wl_min, now()
from {{ ref('hydro') }} where wl_min is not null
{% if is_incremental() %} and timeline_ts_local > (select max(obs_ts) from {{ this }}) {% endif %}

union all
select jaam_kood, timeline_ts_local, 'wl_max', wl_max, now()
from {{ ref('hydro') }} where wl_max is not null
{% if is_incremental() %} and timeline_ts_local > (select max(obs_ts) from {{ this }}) {% endif %}

union all
select jaam_kood, timeline_ts_local, 'wt_avg', wt_avg, now()
from {{ ref('hydro') }} where wt_avg is not null
{% if is_incremental() %} and timeline_ts_local > (select max(obs_ts) from {{ this }}) {% endif %}

union all
select jaam_kood, timeline_ts_local, 'wt_min', wt_min, now()
from {{ ref('hydro') }} where wt_min is not null
{% if is_incremental() %} and timeline_ts_local > (select max(obs_ts) from {{ this }}) {% endif %}

union all
select jaam_kood, timeline_ts_local, 'wt_max', wt_max, now()
from {{ ref('hydro') }} where wt_max is not null
{% if is_incremental() %} and timeline_ts_local > (select max(obs_ts) from {{ this }}) {% endif %}

union all
select jaam_kood, timeline_ts_local, 'q_avg', discharge_avg, now()
from {{ ref('hydro') }} where discharge_avg is not null
{% if is_incremental() %} and timeline_ts_local > (select max(obs_ts) from {{ this }}) {% endif %}

union all
select jaam_kood, timeline_ts_local, 'q_min', discharge_min, now()
from {{ ref('hydro') }} where discharge_min is not null
{% if is_incremental() %} and timeline_ts_local > (select max(obs_ts) from {{ this }}) {% endif %}

union all
select jaam_kood, timeline_ts_local, 'q_max', discharge_max, now()
from {{ ref('hydro') }} where discharge_max is not null
{% if is_incremental() %} and timeline_ts_local > (select max(obs_ts) from {{ this }}) {% endif %}
