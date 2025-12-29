{{ 
    config(
        materialized='view',
        tags=['staging', 'jenzabar', 'course', 'academic']
    )
}}
with source as (
    select * from {{ source('jenzabar_cx_archive', 'crs_rec') }}
),
renamed as (
    select 
        -- Primary Keys
        CRS_NO as course_number,
        CAT as catalog_code,
        
        -- Course Titles
        TITLE1 as title_line_1,
        TITLE2 as title_line_2,
        TITLE3 as title_line_3,
        
        -- Academic Classification
        DEPT as department_code,
        PROG as program_code,
        LVL as course_level_code,
        DISC as discipline_code,
        CRSCTGRY as course_category,
        
        -- Credit Hours (Note: No plain 'HRS' column!)
        MIN_HRS as min_credit_hours,
        MAX_HRS as max_credit_hours,
        CLOCK_HRS as clock_hours,
        
        -- Grading
        GRDG as grading_scheme_code,
        GRDG_DISALLOW as grading_disallowed,
        GRD_REP as grade_repeat_code,
        CNTG as continuing_education_code,
        
        -- Repeatable Course Info
        REP as repeatable_code,
        MAX_REP as max_repeats,
        MAX_REP_HRS as max_repeat_hours,
        CR_REP as credit_repeats,
        CR_MAX_REP as max_credit_repeats,
        
        -- Enrollment Limits
        PREF_CL_SIZE as preferred_class_size,
        
        -- Billing
        TUIT_CODE as tuition_code,
        FEE_CODE as fee_code,
        BILL_CODE as billing_code,
        
        -- Special Flags
        case when VO_ED = 'Y' then true when VO_ED = 'N' then false else null end as is_vocational_education,
        case when FAC_CONSENT = 'Y' then true when FAC_CONSENT = 'N' then false else null end as requires_faculty_consent,
        FAC_CONSENT_NO as faculty_consent_number,
        case when INDEP_STUDY_ALLOW = 'Y' then true when INDEP_STUDY_ALLOW = 'N' then false else null end as allows_independent_study,
        case when DIR_STUDY_ALLOW = 'Y' then true when DIR_STUDY_ALLOW = 'N' then false else null end as allows_directed_study,
        case when CORE = 'Y' then true when CORE = 'N' then false else null end as is_core_course,
        case when STU_ADD = 'Y' then true when STU_ADD = 'N' then false else null end as student_can_add,
        case when STU_DROP = 'Y' then true when STU_DROP = 'N' then false else null end as student_can_drop,
        case when STU_CHG = 'Y' then true when STU_CHG = 'N' then false else null end as student_can_change,
        
        -- Scheduling
        YR_OFFER as year_offered,
        SESS_OFFER as term_offered,
        MAX_DAYS as max_days_to_complete,
        LAST_DROP_DAYS as last_drop_days,
        SITE as site_code,
        
        -- Status
        STAT as status_code,
        cast(STAT_DATE as date) as status_date,
        cast(ACCEPT_DATE as date) as acceptance_date,
        
        -- Faculty Load
        FAC_LOAD as faculty_load_code,
        
        -- Additional
        PHRASE_NO as phrase_number,
        DEGAPPLY as degree_applicability,
        PRIM_ASSOC as primary_association_code,
        CIP_NO as cip_code,
        
        -- State Reporting
        STA_CRD_HRS as state_credit_hours,
        STA_CRSCNT as state_course_count,
        
        -- Metadata
        {{ add_source_metadata('jenzabar_cx_archive', 'crs_rec') }}
        
    from source
    where CRS_NO is not null
)
select * from renamed