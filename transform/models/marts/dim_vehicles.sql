{{ config(
    materialized='incremental',
    unique_key='vehicle_id'
) }}

with source as (
    select * from {{ ref('int_busses_cleaned') }}
    {% if is_incremental() %}
    where ping_at > (select coalesce(max(ping_at), '1970-01-01') from {{ this }})
    {% endif %}
),

latest_vehicle as (
    select
        *,
        row_number() over (partition by vehicle_id order by ping_at desc) as rn
    from source
    where vehicle_id is not null
)

select
    vehicle_id,
    operator_code,
    source_system,
    ping_at
from latest_vehicle
where rn = 1
