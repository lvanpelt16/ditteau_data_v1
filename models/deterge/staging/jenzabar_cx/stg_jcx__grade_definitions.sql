{{ 
    config(
        materialized='view',
        tags=['staging', 'jenzabar', 'reference', 'grades']
    )
}}
with source as (
    select * from {{ source('jenzabar_cx_archive', 'grd_table') }}
),
renamed as (
    select 
        -- Primary Key
        GRD as grade_code,  -- Note: Not GRDG!
        
        -- Grade Description
        TXT as grade_description,
        CTGRY as category_code,
        SCHEME as grading_scheme,
        
        -- Points and Value
        PTS as grade_points,
        VAL as numeric_value,
        MIN_HRS as minimum_hours,
        
        -- Calculation Factors
        ATT_FCTR as attempted_factor,
        EARN_FCTR as earned_factor,
        PASS_FCTR as passed_factor,
        QUAL_FCTR as quality_factor,
        AUDIT_FCTR as audit_factor,
        AUDIT_HRS_FCTR as audit_hours_factor,
        
        -- Cumulative Factors
        CUM_ATT_FCTR as cumulative_attempted_factor,
        CUM_EARN_FCTR as cumulative_earned_factor,
        CUM_PASS_FCTR as cumulative_passed_factor,
        CUM_QUAL_FCTR as cumulative_quality_factor,
        
        -- Flags
        case when INSTR_ASSGN = 'Y' then true when INSTR_ASSGN = 'N' then false else null end as is_instructor_assigned,
        case when REP = 'Y' then true when REP = 'N' then false else null end as is_repeatable,
        case when WEB_UPD = 'Y' then true when WEB_UPD = 'N' then false else null end as allows_web_update,
        case when WEB_DISPLAY = 'Y' then true when WEB_DISPLAY = 'N' then false else null end as displays_on_web,
        case when INCMPL = 'Y' then true when INCMPL = 'N' then false else null end as is_incomplete,
        case when LDOA_REQUIRED = 'Y' then true when LDOA_REQUIRED = 'N' then false else null end as requires_last_date_attended,
        case when FA_PASS = 'Y' then true when FA_PASS = 'N' then false else null end as counts_for_financial_aid,
        case when REPEATABLE_CRS = 'Y' then true when REPEATABLE_CRS = 'N' then false else null end as allows_repeatable_course,
        case when MIN_GRD_CHK = 'Y' then true when MIN_GRD_CHK = 'N' then false else null end as requires_minimum_grade_check,
        case when PREREQ_GRD_CHK = 'Y' then true when PREREQ_GRD_CHK = 'N' then false else null end as requires_prerequisite_grade_check,
        
        -- Alternate Grades
        ALT_GRD as alternate_grade,
        ALT_TYPE as alternate_type,
        
        -- Display
        PRNT as print_flag,
        SORT as sort_order,
        
        -- Dates
        cast(ACTIVE_DATE as date) as active_date,
        cast(INACTIVE_DATE as date) as inactive_date,
        
        -- Additional
        AUCOMM as auto_comment_code,
        CC_CODE as clearinghouse_code,
        AUDIT_PLANNED_FCTR as audit_planned_factor,
        
        -- Metadata
        {{ add_source_metadata('jenzabar_cx_archive', 'grd_table') }}
        
    from source
    where GRD is not null
)
select * from renamed