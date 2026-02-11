{{
    config(
        materialized='table',
        schema='distribute',
        tags=['dimension']
    )
}}

select
    -- Surrogate key (primary key for dimension)
    {{ dbt_utils.generate_surrogate_key(['b.course_number', 'b.section_number', 'b.academic_year', 'b.term_code']) }} as course_section_key,

    -- Natural key components (business identifiers)
    b.course_number as course_section_course_number,
    b.section_number as course_section_section_number,

    -- Descriptive attributes
    concat(a.title_line_1, ' ', a.title_line_2, ' ', a.title_line_3) as course_title,
    b.section_title as section_title,
    b.catalog_code as course_section_catalog,
    d.dept_code as course_section_dept,
    b.term_code as course_section_sess,
    b.academic_year as course_section_year,
    b.subsession_code as course_section_subsess,
    a.program_code as course_section_program_level,
    b.current_registered_count as course_section_reg_number,
    b.status_code as course_section_status,
    c.faculty_code as course_section_faculty_key,

    -- Status tracking
    b.begin_date as source_active_date,
    b.end_date as source_inactive_date,
    case
        when b.end_date is null or b.end_date >= current_date() then true
        else false
    end as is_active,

    -- Metadata
    true as is_current,  -- For SCD Type 2 if needed later
    current_timestamp() as creation_timestamp,
    current_timestamp() as last_modified_timestamp

from {{ ref('stg_jcx__courses') }} a
join {{ ref('stg_jcx__sections') }} b
  on a.course_number = b.course_number
  and a.catalog_code = b.catalog_code
join {{ ref('dim_faculty') }} c
  on b.primary_faculty_id = c.faculty_code
join {{ ref('dim_dept') }} d
  on a.department_code = d.dept_code