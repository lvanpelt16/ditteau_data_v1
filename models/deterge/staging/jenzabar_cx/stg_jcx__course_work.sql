{{ 
    config(
        materialized='view',
        tags=['staging', 'jenzabar', 'academic', 'grades']
    )
}}

with source as (
    select * from {{ source('jenzabar_cx_archive', 'cw_rec') }}
),

renamed as (
    select 
        -- Primary Keys
        cw_no as course_work_number,
        id as student_id,
        yr as year,
        sess as session_code,
        subsess as subsession_code,
        prog as program_code,
        crs_no as course_number,
        cat as catalog_code,
        sec as section_number,
        
        -- Academic Performance
        grd as final_grade,
        midsess_grd as midsession_grade,
        rawgrd as raw_final_grade,
        midsess_rawgrd as raw_midsession_grade,
        
        -- Credit Information
        hrs as credit_hours,
        clock_hrs as clock_hours,
        sta_crd_hrs as state_credit_hours,
        sta_crscnt as state_course_count,
        
        -- Grade Configuration
        grdg as grading_code,
        cntg as counting_code,
        rep as repeat_code,
        
        -- Dates
        cast(beg_date as date) as begin_date,
        cast(end_date as date) as end_date,
        cast(last_attend_date as date) as last_attendance_date,
        
        -- Status
        stat as status_code,
        
        -- Attendance
        absnt_hrs as absent_hours,
        absnt_upd as absent_hours_updated_flag,
        
        -- Location
        site as course_site_code,
        
        -- Financial Aid Eligibility
        fa_elig_flag as is_fa_eligible,
        fa_inelig as is_fa_ineligible_repeat,
        
        -- Registration
        regctgry as registration_category_code,
        
        -- Metadata
        {{ add_source_metadata('jenzabar_cx_archive', 'cw_rec') }}
        
    from source
    where cw_no is not null
      and id is not null
)

select * from renamed