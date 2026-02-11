{{
    config(
        materialized='table',
        schema='distribute',
        tags=['dimension']
    )
}}

select
    -- Surrogate key (primary key for dimension)
    {{ dbt_utils.generate_surrogate_key(['course_number']) }} as course_key,

    -- Natural key (business identifier)
    course_number as course_code,

    -- Descriptive attributes
    concat(title_line_1, ' ', title_line_2, ' ', title_line_3) as course_title,
    catalog_code as course_catalog,
    program_code as course_program_level,
    department_code as course_dept,

    -- Status tracking
    status_date as source_active_date,
    null as source_inactive_date,
    true as is_active,

    -- Metadata
    true as is_current,  -- For SCD Type 2 if needed later
    current_timestamp() as creation_timestamp,
    current_timestamp() as last_modified_timestamp

from {{ ref('stg_jcx__courses') }}