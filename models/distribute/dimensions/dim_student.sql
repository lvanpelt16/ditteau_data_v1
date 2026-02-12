{{
    config(
        materialized='table',
        schema='distribute',
        tags=['dimension']
    )
}}

select
    -- Surrogate key (primary key for dimension)
    {{ dbt_utils.generate_surrogate_key(['a.person_id']) }} as student_key,

    -- Natural key (business identifier)
    a.person_id as student_code,

    -- Descriptive attributes
    b.last_name as student_lastname,
    b.first_name as student_firstname,
    b.middle_name as student_middlename,
    b.title as student_prefix,
    b.suffix as student_suffix,
    d.major_code as student_major,
    a.birth_date as student_dob,
    a.gender_code as student_gender,
    a.residence_country as student_country,
    f.ethnicity_code as student_ethnicity,
    c.degree_grant_date as student_grad_date,
    e.txt as student_acad_stat,
    c.program_code as student_program_level,

    -- Status tracking
    b.record_added_date as source_active_date,
    null as source_inactive_date,
    true as is_active,

    -- Metadata
    true as is_current,  -- For SCD Type 2 if needed later
    current_timestamp() as creation_timestamp,
    current_timestamp() as last_modified_timestamp

from {{ ref('stg_jcx__profile') }} a
join {{ ref('stg_jcx__id') }} b on a.person_id = b.person_id
join {{ ref('stg_jcx__students') }} c on c.student_id = b.person_id
join {{ ref('dim_major') }} d on c.major_1_code = d.major_code
join {{ ref('dim_ethnicity') }} f on f.ethnicity_code = a.ethnicity_code
join {{ source('jenzabar_cx_archive', 'acad_stat_table') }} e on c.academic_standing_code = e.acst