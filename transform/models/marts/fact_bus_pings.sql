{{ config(materialized='incremental') }}

select
    -- Primary Keys
    vehicle_ping_id,

    -- Foreign Keys
    vehicle_id,
    course_loid,

    -- Location and Status
    latitude,
    longitude,
    heading_degrees,
    stop_status,

    -- Metrics
    delay_seconds,
    reached_meters,
    order_in_course,

    -- Stop information
    last_stop_symbol,
    last_stop_order_no,
    nearest_stop_symbol,
    nearest_stop_order_no,
    distance_from_passed_stop_m,
    distance_to_nearest_stop_m,

    -- Metadata
    ping_at,
    ingested_at
from {{ ref('int_busses_cleaned') }}

{% if is_incremental() %}
    where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
