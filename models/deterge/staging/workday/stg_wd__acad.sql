{{
    config(
        materialized='view'
    )
}}

/*
Purpose: Staging model for Workday Student academic program data
Source: DEPOSIT.STA_ACAD_WD (Workday Student)
Grain: One row per student academic record
Key: student_id
Updates: TBD - need to determine refresh cadence
Owner: Registrar / Institutional Research

Notes:
- Many fields contain semicolon-separated lists (e.g., academic periods, programs)
- These are preserved as-is in staging; splitting happens in intermediate models
- Integration with Jenzabar data happens in int_students.sql
*/

select
    -- Primary Identifiers
    student_id,
    reference_id,
    universal_id,
    
    -- Academic Level & Program
    academic_level,
    primary_academic_plan,
    primary_program_of_study,
    primary_program_of_study_completion_status,
    primary_program_of_study_record_status,
    
    -- Degree/Certificate Status
    case 
        when degree_or_certificate_seeking = 1 then true
        when degree_or_certificate_seeking = 0 then false
        else null
    end as is_degree_seeking,
    
    -- Academic Periods (semicolon-separated - preserved as-is)
    academic_periods_with_academic_period_records,
    first_academic_period,
    first_standard_academic_period_for_student,
    current_standard_academic_period,
    last_academic_period_with_final_grade,
    
    -- Academic Standing & Performance
    latest_academic_standing,
    latest_class_standing,
    latest_load_status,
    anticipated_load_status,
    gpa__cumulative_institutional,
    gpa__cumulative_overall,
    gpa__transfer,
    
    -- Enrollment Status
    case 
        when currently_enrolled = 1 then true
        when currently_enrolled = 0 then false
        else null
    end as is_currently_enrolled,
    
    current_housing_status,
    
    -- Demographics (relevant to academic record)
    date_of_birth,
    gender,
    citizenship_status,
    primary_nationality_citizenship_status,
    country_of_birth,
    state_of_birth,
    
    -- Special Populations
    case 
        when is_international_student = 1 then true
        when is_international_student = 0 then false
        else null
    end as is_international_student,
    
    case 
        when "Is_Student-Athlete" = 1 then true
        when "Is_Student-Athlete" = 0 then false
        else null
    end as is_student_athlete,
    
    case 
        when first_generation_college_student = 1 then true
        when first_generation_college_student = 0 then false
        else null
    end as is_first_generation,
    
    case 
        when military_relationship = 1 then true
        when military_relationship = 0 then false
        else null
    end as has_military_relationship,
    
    -- Dates
    application_date,
    start_date,
    expected_completion_date,
    declare_date_for_new_program_of_study_for_new_academic_calendar,
    effective_date_of_student_program_of_study_record_status,
    effective_date_for_earliest_inactive_program_of_study_record_status_assignment,
    
    -- Academic Record Status
    academic_record_status,
    case 
        when academic_record_locked = 1 then true
        when academic_record_locked = 0 then false
        else null
    end as is_academic_record_locked,
    
    case 
        when has_pending_grades = 1 then true
        when has_pending_grades = 0 then false
        else null
    end as has_pending_grades,
    
    case 
        when has_no_registered_courses = 1 then true
        when has_no_registered_courses = 0 then false
        else null
    end as has_no_registered_courses,
    
    -- Institution & Academic Unit
    institution,
    institution_academic_unit,
    academicunit,
    
    -- Student Type
    student_applicant_type,
    student_applicant_type_category,
    "Number_of_Non-Degree_or_Certificate_Programs_of_Study" as number_of_non_degree_programs,
    
    -- Privacy
    case 
        when privacy_block = 1 then true
        when privacy_block = 0 then false
        else null
    end as has_privacy_block,
    
    case 
        when deceased = 1 then true
        when deceased = 0 then false
        else null
    end as is_deceased,
    
    -- Semicolon-separated list fields (preserved as-is)
    -- These will be split in intermediate models
    academic_calendar_as_of_primary_program_of_study_declare_date,
    academic_period_records,
    academic_record,
    academic_requirement_s__for_student,
    academic_requirement_area_assignments,
    academic_requirement_overrides,
    academic_requirements,
    activated_historical_academic_record,
    active_holdable_assignments,
    all_academic_periods_for_continuing_student_registration_onboarding,
    all_cumulative_gpa_overrides,
    awards_and_activities,
    citizenship_countries,
    current_athletic_teams,
    current_academic_year,
    enrolled_unit_type,
    financial_aid_period_records,
    financial__aid_record,
    historical_academic_record,
    historical_academic_record_activation_date_and_time,
    historical_academic_record_on_transcript_record,
    historical_match_reconciliations,
    internal_articulated_registrations,
    marital_status,
    matching_historical_student,
    new_academic_calendar_for_new_program_of_study,
    new_program_of_study_for_new_academic_calendar,
    program_cip_code_on_academic_record_start_date,
    programs_of_study,
    race_ethnicities,
    sexual_orientations,
    student,
    student_accomplishments,
    student_applications,
    student_cohort,
    student_cohort_assignments,
    student_cohorts,
    student_groupings,
    student_hold_assignments,
    student_program_of_study_record,
    visa_id_types,
    
    -- Metadata (to be added via macro once source is defined)
    current_timestamp() as _dbt_loaded_at

from {{ source('workday', 'sta_acad_wd') }}
where student_id is not null