{{
    config(
        materialized='table',
        schema='distribute',
        tags=['fact']
    )
}}

/*
Purpose: Enrollment fact table tracking course registrations and grades
Source: deterge_cx_cw_rec_enriched (Jenzabar course work records)
Grain: One row per student-course-section enrollment
*/

select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['a.CW_NO']) }} as enrollment_key,

    -- Foreign keys to dimensions
    b.student_key as enrollment_student_key,
    c.course_section_key as enrollment_course_section_key,
    e.term_key as enrollment_term_key,
    c.course_section_faculty_key as enrollment_faculty_key,
    g.grade_key as enrollment_grade_key,
    h.date_key as enrollment_first_date_key,
    i.date_key as enrollment_latest_date_key,

    -- Degenerate dimensions
    a.CW_NO as enrollment_source_serial,
    a.PROG as enrollment_program_level,

    -- Measures
    a.HRS as enrollment_hours,

    -- Status attributes
    a.STAT as enrollment_status,
    a.first_event_type as enrollment_first_stat,
    a.latest_event_type_final as enrollment_latest_stat,

    -- Status tracking
    a.BEG_DATE as source_active_date,
    a.END_DATE as source_inactive_date,
    case
        when a.END_DATE is null or a.END_DATE >= current_date() then true
        else false
    end as is_active,

    -- Metadata
    true as is_current,
    current_timestamp() as creation_timestamp,
    current_timestamp() as last_modified_timestamp

from {{ ref('deterge_cx_cw_rec_enriched') }} a
inner join {{ ref('dim_student') }} b
    on a.ID = b.jcx_student_id
inner join {{ ref('dim_course_section') }} c
    on a.CRS_NO = c.course_section_course_number
    and a.SEC = c.course_section_section_number
    and a.CAT = c.course_section_catalog
    and a.PROG = c.course_section_program_level
    and a.SUBSESS = c.course_section_subsess
left join {{ ref('dim_term') }} e
    on a.SESS = e.term_sess
    and a.YR = e.term_year
    and a.PROG = e.term_program_level
    and a.SUBSESS = e.term_subsess
left join {{ ref('dim_grade') }} g
    on a.GRD = g.grade_code
left join {{ ref('dim_date') }} h
    on a.first_event_timestamp::date = h.date_actual
left join {{ ref('dim_date') }} i
    on a.latest_event_timestamp::date = i.date_actual
