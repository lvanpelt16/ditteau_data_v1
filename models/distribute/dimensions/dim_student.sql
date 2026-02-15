{{
    config(
        materialized='table',
        schema='distribute',
        tags=['dimension']
    )
}}

/*
Purpose: Student dimension using integrated student data from Workday and Jenzabar CX
Source: int_students (golden record)
Grain: One row per student (Ditteau Master ID)
*/

select
    -- Surrogate key (primary key for dimension)
    {{ dbt_utils.generate_surrogate_key(['ditteau_id']) }} as student_key,

    -- Natural key (business identifier)
    ditteau_id as student_code,

    -- Source system IDs (for reference/troubleshooting)
    wd_student_id,
    jcx_student_id,
    primary_source_system,

    -- Descriptive attributes - Name
    last_name as student_lastname,
    first_name as student_firstname,
    middle_name as student_middlename,
    preferred_name as student_preferred_name,
    full_name as student_full_name,

    -- Demographics
    birth_date as student_dob,
    calculated_age as student_age,
    gender as student_gender,
    gender_identity as student_gender_identity,
    race_ethnicity as student_ethnicity,
    is_hispanic_or_latino as student_is_hispanic,

    -- Location
    country as student_country,
    state as student_state,
    city as student_city,
    postal_code as student_postal_code,

    -- Contact
    email as student_email,

    -- Academic
    primary_program as student_major,
    academic_unit as student_academic_unit,
    primary_academic_level as student_academic_level,
    jcx_program_code as student_program_level,
    jcx_degree_code as student_degree_code,
    jcx_class_level as student_class_level,
    planned_graduation_year as student_planned_grad_year,
    planned_graduation_term as student_planned_grad_term,

    -- Status flags
    is_active,
    is_matriculated,
    is_graduated,
    is_degree_seeking,
    is_first_generation,
    is_independent_student,
    has_privacy_block,
    is_deceased,

    -- System tracking
    in_both_systems,
    workday_only,
    jcx_only,
    data_quality_flag,

    -- Status tracking
    first_entry_date as source_active_date,
    case when is_deceased then date_of_death else null end as source_inactive_date,

    -- Metadata
    true as is_current,  -- For SCD Type 2 if needed later
    current_timestamp() as creation_timestamp,
    current_timestamp() as last_modified_timestamp

from {{ ref('int_students') }}
