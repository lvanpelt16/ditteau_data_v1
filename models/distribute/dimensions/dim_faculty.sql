{{
    config(
        materialized='table',
        schema='distribute',
        tags=['dimension']
    )
}}

select
    -- Surrogate key (primary key for dimension)
    {{ dbt_utils.generate_surrogate_key(['a.faculty_id']) }} as faculty_key,

    -- Natural key (business identifier)
    a.faculty_id as faculty_code,

    -- Descriptive attributes
    b.last_name as faculty_lastname,
    b.first_name as faculty_firstname,
    b.middle_name as faculty_middlename,
    b.title as faculty_prefix,
    b.suffix as faculty_suffix,
    a.title_code as faculty_rank,
    d.dept_code as faculty_dept,

    -- Status tracking
    a.status_date as source_active_date,
    null as source_inactive_date,
    true as is_active,

    -- Metadata
    true as is_current,  -- For SCD Type 2 if needed later
    current_timestamp() as creation_timestamp,
    current_timestamp() as last_modified_timestamp

from {{ ref('stg_jcx__faculty') }} a
join {{ ref('stg_jcx__id') }} b on a.faculty_id = b.person_id
join {{ ref('dim_dept') }} d on a.primary_department_code = d.dept_code
