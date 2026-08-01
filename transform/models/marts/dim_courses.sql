{{ config(
    materialized='incremental',
    unique_key='course_loid'
) }}

with source as (
    select * from {{ ref('int_busses_cleaned') }}
    {% if is_incremental() %}
    where ping_at > (select coalesce(max(ping_at), '1970-01-01') from {{ this }})
    {% endif %}
),

latest_course as (
    select
        *,
        row_number() over (partition by course_loid order by ping_at desc) as rn
    from source
    where course_loid is not null
)

select
    course_loid,
    day_course_loid,
    external_course_loid,
    variant_loid,
    line_name,
    direction_name,
    optional_direction_name,
    ping_at
from latest_course
where rn = 1
