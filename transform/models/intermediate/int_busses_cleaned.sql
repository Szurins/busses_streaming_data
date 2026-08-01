select
    vehicle_ping_id,
    vehicle_id,
    course_loid,
    day_course_loid,
    external_course_loid,
    variant_loid,

    trim(line_name) as line_name,
    trim(operator_code) as operator_code,
    trim(direction_name) as direction_name,
    optional_direction_name,
    coalesce(trim(on_stop_point_symbol), 'IN_TRANSIT') as stop_status,

    case
        when latitude between 49.0 and 55.0 then latitude
        else null
    end as latitude,

    case
        when longitude between 14.0 and 24.0 then longitude
        else null
    end as longitude,

    heading_degrees,
    delay_seconds,
    reached_meters,
    order_in_course,
    last_stop_symbol,
    last_stop_order_no,
    nearest_stop_symbol,
    nearest_stop_order_no,
    distance_from_passed_stop_m,
    distance_to_nearest_stop_m,
    stop_points_raw,

    ping_at,
    ingested_at,
    source_system
from {{ ref('stg_busses_info') }}
where extract(year from ping_at) >= 1970
