{{
    config(
        materialized='table',
        schema='deterge',
        tags=['integration', 'students', 'core', 'golden_record']
    )
}}

/*
Purpose: Integrated student view combining Workday Student (primary) and Jenzabar CX (legacy)
Source: Multiple systems via dim_integration_ids
Grain: One row per student (Ditteau Master ID)

Data Priority:
1. Workday Student (NEW system of record) - use when available
2. Jenzabar CX (LEGACY system) - use only if Workday data unavailable

This model creates a "golden record" for each student by intelligently
merging data from both systems based on which is more current/complete.
*/

-- ============================================================================
-- WORKDAY STUDENT DATA (PRIMARY SOURCE)
-- ============================================================================
with wd_students as (
    select * from {{ ref('stg_wd__students') }}
),

-- ============================================================================
-- JENZABAR CX DATA (LEGACY SOURCE)
-- ============================================================================
jcx_students as (
    select
        stu.student_id,
        
        -- Name fields from ID_REC
        id_rec.first_name,
        id_rec.last_name,
        id_rec.middle_name,
        
        -- Demographics from PROFILE_REC
        prof.birth_date,
        prof.gender_code as gender,
        prof.ethnicity_code as race_ethnicity_code,
        prof.is_hispanic,
        
        -- Email from AA_REC
        emails.email_address as email,
        
        -- Academic fields from PROG_ENR_REC
        stu.major_1_code as primary_major,
        stu.program_code,
        stu.degree_code,
        stu.academic_standing_code as class_standing_code,
        stu.advisor_id,
        stu.current_enrollment_status as current_enrollment_status_code,
        case 
           when stu.degree_code is not null 
           and stu.expects_to_graduate = true 
           then true
           else false
        end as is_degree_seeking,
        stu.enrollment_date as program_enrollment_date,
        stu.admission_date as program_admission_date,
        stu.planned_graduation_year,
        stu.planned_graduation_term,
        stu.academic_standing_date as current_status_date,
        
        -- Derived: is_active (based on enrollment status)
        case 
            when stu.current_enrollment_status in ('A', 'C', 'E') then true  -- Active codes
            else false
        end as is_active,
        
        -- Metadata
        stu._dbt_loaded_at
        
    from {{ ref('stg_jcx__students') }} stu
    left join {{ ref('stg_jcx__id') }} id_rec 
        on stu.student_id = id_rec.person_id
    left join {{ ref('stg_jcx__profile') }} prof
        on stu.student_id = prof.person_id
    left join {{ ref('stg_jcx__emails') }} emails
        on stu.student_id = emails.person_id
),
-- ============================================================================
-- GET DITTEAU IDS AND SYSTEM MAPPINGS
-- ============================================================================
-- Get all student identity mappings
student_ids as (
    select
        ditteau_id,
        source_system,
        source_id,
        is_primary_source,
        match_confidence,
        match_method
    from {{ ref('dim_integration_ids') }}
    where entity_type = 'PERSON'
      and role_type = 'STUDENT'
      and is_current = true
),

-- Pivot to get both Workday and Jenzabar IDs for each person
student_identity_map as (
    select
        ditteau_id,
        max(case when source_system = 'WORKDAY' then source_id end) as wd_student_id,
        max(case when source_system = 'JENZABAR_CX' then source_id end) as jcx_student_id,
        max(case when source_system = 'WORKDAY' then is_primary_source end) as wd_is_primary,
        max(case when source_system = 'JENZABAR_CX' then is_primary_source end) as jcx_is_primary
    from student_ids
    group by ditteau_id
),

-- ============================================================================
-- JOIN TO SOURCE DATA
-- ============================================================================
joined_data as (
    select
        -- ===================================================================
        -- DITTEAU MASTER ID
        -- ===================================================================
        map.ditteau_id,
        
        -- ===================================================================
        -- SOURCE SYSTEM IDS (for reference)
        -- ===================================================================
        map.wd_student_id,
        map.jcx_student_id,
        
        -- Which system is primary for this student?
        case
            when map.wd_is_primary then 'WORKDAY'
            when map.jcx_is_primary then 'JENZABAR_CX'
            else 'UNKNOWN'
        end as primary_source_system,
        
        -- ===================================================================
        -- DEMOGRAPHICS - Prefer Workday, fallback to Jenzabar
        -- ===================================================================
        coalesce(wd.first_name, jcx.first_name) as first_name,
        coalesce(wd.last_name, jcx.last_name) as last_name,
        coalesce(wd.middle_name, jcx.middle_name) as middle_name,
        
        -- Workday has preferred names - use those if available
        wd.preferred_name,
        wd.full_legal_name,
        
        -- Derived: full name
        trim(
            coalesce(wd.first_name, jcx.first_name) || ' ' || 
            coalesce(wd.last_name, jcx.last_name)
        ) as full_name,
        
        -- Birth date
        coalesce(wd.birth_date, jcx.birth_date) as birth_date,
        
        -- Age (Workday calculates this)
        wd.age,
        
        -- ===================================================================
        -- CONTACT INFORMATION
        -- ===================================================================
        coalesce(wd.email, jcx.email) as email,
        
        -- Address (only in Workday)
        wd.city,
        wd.state,
        wd.postal_code,
        wd.county,
        wd.country,
        
        -- ===================================================================
        -- ENHANCED DEMOGRAPHICS (mostly Workday-only)
        -- ===================================================================
        -- Gender fields (Workday has more detail)
        coalesce(wd.gender, jcx.gender) as gender,
        wd.gender_identity,
        wd.sexual_orientation,
        wd.pronoun,
        
        -- Race/Ethnicity
        coalesce(wd.race_ethnicities, jcx.race_ethnicity_code) as race_ethnicity,
        coalesce(wd.is_hispanic_or_latino, jcx.is_hispanic) as is_hispanic_or_latino,
        
        -- Citizenship/Nationality (Workday only)
        wd.citizenship_status,
        wd.primary_nationality,
        
        -- Other demographics
        wd.marital_status,
        wd.religion,
        
        -- ===================================================================
        -- ACADEMIC INFORMATION
        -- ===================================================================
        -- Program information (prefer Workday, fallback to Jenzabar)
        coalesce(wd.primary_program_of_study, jcx.primary_major) as primary_program,
        wd.academic_unit,
        wd.primary_academic_level,
        
        -- Jenzabar-specific academic info
        jcx.program_code as jcx_program_code,
        jcx.degree_code as jcx_degree_code,
        jcx.class_standing_code as jcx_class_level,
        jcx.advisor_id as jcx_advisor_id,
        
        -- ===================================================================
        -- STATUS FLAGS
        -- ===================================================================
        -- Active/enrollment status (prefer Workday)
        coalesce(wd.is_active_matriculated, jcx.is_active) as is_active,
        coalesce(wd.is_matriculated, false) as is_matriculated,
        coalesce(wd.is_graduated, false) as is_graduated,
        
        -- Academic status from Jenzabar
        jcx.current_enrollment_status_code as jcx_enrollment_status,
        
        -- Special statuses
        coalesce(wd.is_first_generation, false) as is_first_generation,
        coalesce(wd.has_privacy_block, false) as has_privacy_block,
        coalesce(wd.is_deceased, false) as is_deceased,
        
        -- Degree seeking (Jenzabar)
        coalesce(jcx.is_degree_seeking, false) as is_degree_seeking,
        
        -- ===================================================================
        -- DATES
        -- ===================================================================
        -- Entry dates
        coalesce(wd.created_on_date, jcx.program_enrollment_date) as first_entry_date,
        jcx.program_enrollment_date as jcx_program_entrance_date,
        jcx.program_admission_date as jcx_program_acceptance_date,
        
        -- Expected graduation
        jcx.planned_graduation_year,
        jcx.planned_graduation_term,
        
        -- Status date
        jcx.current_status_date,
        
        -- Death date (Workday only)
        wd.date_of_death,
        
        -- ===================================================================
        -- BUSINESS LOGIC: DERIVED FIELDS
        -- ===================================================================
        -- Current age calculation
        case
            when coalesce(wd.birth_date, jcx.birth_date) is not null
            then datediff(
                year, 
                coalesce(wd.birth_date, jcx.birth_date), 
                current_date()
            )
            else null
        end as calculated_age,
        
        -- Adult student flag (24+)
        case
            when coalesce(wd.birth_date, jcx.birth_date) is not null
            then datediff(
                year, 
                coalesce(wd.birth_date, jcx.birth_date), 
                current_date()
            ) >= 24
            else null
        end as is_independent_student,
        
        -- Has complete contact information
        case
            when coalesce(wd.email, jcx.email) is not null
            then true
            else false
        end as has_email,
        
        -- In both systems (migrated student)
        case
            when map.wd_student_id is not null 
             and map.jcx_student_id is not null
            then true
            else false
        end as in_both_systems,
        
        -- Workday-only student (new student post-migration)
        case
            when map.wd_student_id is not null 
             and map.jcx_student_id is null
            then true
            else false
        end as workday_only,
        
        -- Jenzabar-only student (legacy student not yet migrated)
        case
            when map.wd_student_id is null 
             and map.jcx_student_id is not null
            then true
            else false
        end as jcx_only,
        
        -- ===================================================================
        -- DATA QUALITY INDICATORS
        -- ===================================================================
        case
            when coalesce(wd.first_name, jcx.first_name) is null then 'MISSING_NAME'
            when coalesce(wd.email, jcx.email) is null then 'MISSING_EMAIL'
            when coalesce(wd.birth_date, jcx.birth_date) is null then 'MISSING_DOB'
            else 'COMPLETE'
        end as data_quality_flag,
        
        -- ===================================================================
        -- METADATA
        -- ===================================================================
        greatest(
            coalesce(wd._dbt_loaded_at, '1900-01-01'::timestamp),
            coalesce(jcx._dbt_loaded_at, '1900-01-01'::timestamp)
        ) as _dbt_loaded_at,
        
        case
            when map.wd_student_id is not null and map.jcx_student_id is not null
            then 'INTEGRATED'
            when map.wd_student_id is not null
            then 'WORKDAY'
            else 'JENZABAR_CX'
        end as _source_system,
        
        'CONFIDENTIAL' as _data_classification,
        'REGISTRAR' as _data_owner,
        
        current_timestamp() as _integrated_at
        
    from student_identity_map map
    left join wd_students wd
        on map.wd_student_id = wd.student_id
    left join jcx_students jcx
        on map.jcx_student_id = jcx.student_id
)

select * from joined_data