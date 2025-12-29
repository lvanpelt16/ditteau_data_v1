{{ 
    config(
        materialized='view',
        tags=['staging', 'jenzabar', 'student', 'enrollment']
    )
}}
with source as (
    select * from {{ source('jenzabar_cx_archive', 'prog_enr_rec') }}
),
renamed as (
    select 
        -- Primary Keys
        ID as student_id,
        PROG as program_code,
        SUBPROG as subprogram_code,
        
        -- Program Information
        DEG as degree_code,
        MAJOR1 as major_1_code,  -- Note: No underscore in source
        MAJOR2 as major_2_code,
        MAJOR3 as major_3_code,
        MINOR1 as minor_1_code,
        MINOR2 as minor_2_code,
        CONC1 as concentration_1_code,
        CONC2 as concentration_2_code,
        
        -- Classification
        CL as class_level_code,
        SITE as site_code,
        CAT as catalog_code,
        
        -- Enrollment Status
        CURRENR as current_enrollment_status,
        ACST as academic_standing_code,
        
        -- Admission Dates
        cast(ADM_DATE as date) as admission_date,
        ADM_SESS as admission_term,
        ADM_YR as admission_year,
        ADM_STAT as admission_status_code,
        
        -- Important Dates
        cast(ENR_DATE as date) as enrollment_date,
        cast(MATRIC_DATE as date) as matriculation_date,
        cast(ACAD_DATE as date) as academic_standing_date,
        cast(DECL_DATE as date) as declaration_date,
        cast(LV_DATE as date) as leave_date,
        
        -- Planned Graduation
        PLAN_GRAD_SESS as planned_graduation_term,
        PLAN_GRAD_YR as planned_graduation_year,
        
        -- Degree Application/Grant
        cast(DEG_APP_DATE as date) as degree_application_date,
        cast(DEG_GRANT_DATE as date) as degree_grant_date,
        DEG_GRANT_SESS as degree_grant_term,
        DEG_GRANT_YR as degree_grant_year,
        
        -- Advising
        ADV_ID as advisor_id,
        
        -- Registration Limits
        MAX_HRS_REG as max_hours_allowed,
        MIN_HRS_REG as min_hours_required,
        
        -- Flags
        case when RSTR_SCHD = 'Y' then true when RSTR_SCHD = 'N' then false else null end as has_restricted_schedule,
        case when TO_ALUM = 'Y' then true when TO_ALUM = 'N' then false else null end as moved_to_alumni,
        case when EXPECT_GRAD = 'Y' then true when EXPECT_GRAD = 'N' then false else null end as expects_to_graduate,
        case when PRIMARY_FLAG = 'Y' then true when PRIMARY_FLAG = 'N' then false else null end as is_primary_program,
        
        -- State Reporting
        RES_ST as residence_state_code,
        COHORT_YR as cohort_year,
        COHORT_CTGRY as cohort_category,
        
        -- Additional Info
        REASON as status_reason_code,
        LAST_SESS as last_term_attended,
        GRD_RPT_ID as grade_report_id,
        DEGGRP as degree_group_code,
        PRIM_ASSOC as primary_association_code,
        
        -- Metadata
        {{ add_source_metadata('jenzabar_cx_archive', 'prog_enr_rec') }}
        
    from source
    where ID is not null
)
select * from renamed