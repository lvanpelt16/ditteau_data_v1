{{ config(
    materialized = 'table',
    schema = 'DISTRIBUTE'
) }}
select
    row_number() over (order by course_number) as course_key,  -- ✅ ADD
    course_number as course_code,
    concat(title_line_1, ' ', title_line_2, ' ', title_line_3) as course_title,
    catalog_code as course_catalog,
    program_code as course_program_level,
    department_code as course_dept,
    status_date as source_active_date,
    to_date('9999-12-31') as source_inactive_date,
    'Y' as is_current,
    current_timestamp() as creation_timestamp
from {{ ref('stg_jcx__courses') }}