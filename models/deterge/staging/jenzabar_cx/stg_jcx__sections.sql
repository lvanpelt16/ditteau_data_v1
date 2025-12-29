{{ 
    config(
        materialized='view',
        tags=['staging', 'jenzabar', 'sections', 'course_offerings']
    )
}}
with source as (
    select * from {{ source('jenzabar_cx_archive', 'sec_rec') }}
),
renamed as (
    select 
        -- Primary Keys
        CRS_NO as course_number,
        CAT as catalog_code,
        YR as academic_year,
        SESS as term_code,
        SEC_NO as section_number,
        
        -- Credit Hours
        HRS as credit_hours,
        CLOCK_HRS as clock_hours,
        
        -- Subsession
        SUBSESS as subsession_code,
        
        -- Section Details
        TITLE as section_title,
        RSTR as restriction_code,
        
        -- Faculty
        FAC_ID as primary_faculty_id,
        
        -- Billing
        TUIT_CODE as tuition_code,
        FEE_CODE as fee_code,
        BILL_CODE as billing_code,
        
        -- Grading
        GRDG as grading_scheme_code,
        GRDG_DISALLOW as grading_disallowed,
        
        -- Enrollment Limits
        MAX_REG as max_registration,
        MAX_WAIT as max_waitlist,
        REG_NUM as current_registered_count,
        RESRV_NUM as reserved_count,
        WAIT_NUM as waitlist_count,
        OVER_REG as over_registration_count,
        MAX_REG_RSV as max_registration_reserved,
        REG_NUM_RSV as registered_reserved_count,
        
        -- Dates
        cast(BEG_DATE as date) as begin_date,
        cast(END_DATE as date) as end_date,
        MAX_DAYS as max_days,
        cast(BILLING_DATE as date) as billing_date,
        cast(SCHD_UPD_DATE as date) as schedule_update_date,
        
        -- Flags
        case when REMEDIAL = 'Y' then true when REMEDIAL = 'N' then false else null end as is_remedial,
        case when CREDIT = 'Y' then true when CREDIT = 'N' then false else null end as is_credit,
        case when PRINT_SCHD = 'Y' then true when PRINT_SCHD = 'N' then false else null end as prints_on_schedule,
        case when CLASS_LIST = 'Y' then true when CLASS_LIST = 'N' then false else null end as has_class_list,
        case when EXCH = 'Y' then true when EXCH = 'N' then false else null end as is_exchange,
        case when OPEN_ENR = 'Y' then true when OPEN_ENR = 'N' then false else null end as is_open_enrollment,
        case when SCHD_CHG = 'Y' then true when SCHD_CHG = 'N' then false else null end as has_schedule_change,
        case when PUBREG = 'Y' then true when PUBREG = 'N' then false else null end as is_public_registration,
        case when FA_INELIG = 'Y' then true when FA_INELIG = 'N' then false else null end as is_financial_aid_ineligible,
        case when ESL_SECTION = 'Y' then true when ESL_SECTION = 'N' then false else null end as is_esl_section,
        
        -- Status
        STAT as status_code,
        cast(STAT_DATE as date) as status_date,
        
        -- Additional
        EXAM_CODE as exam_code,
        REF_NO as reference_number,
        BRDG_ORD as bridge_order,
        ALTCAL as alternate_calendar,
        ALTRFND_NO as alternate_refund_number,
        ALTRFND as alternate_refund_code,
        WEEKS as duration_weeks,
        PHRASE_NO as phrase_number,
        DEGGRP as degree_group_code,
        
        -- Metadata
        {{ add_source_metadata('jenzabar_cx_archive', 'sec_rec') }}
        
    from source
    where CRS_NO is not null
        and YR is not null
        and SESS is not null
        and SEC_NO is not null
)
select * from renamed