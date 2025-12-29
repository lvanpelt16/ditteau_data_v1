{{ 
    config(
        materialized='view',
        tags=['staging', 'jenzabar', 'registration', 'enrollment']
    )
}}
with source as (
    select * from {{ source('jenzabar_cx_archive', 'reg_rec') }}
),
renamed as (
    select 
        -- Primary Key (Note: No ID column - this is course work based!)
        CW_NO as course_work_number,
        
        -- Registration Event Details
        cast(BEG_DATE as date) as event_date,
        TM as event_time,
        cast(SYS_DATE as date) as system_date,
        
        -- Billing Codes
        TUIT_CODE as tuition_code,
        FEE_CODE as fee_code,
        BILL_CODE as billing_code,
        
        -- Status
        STAT as registration_status_code,
        REASON as status_reason_code,
        
        -- Grading
        GRDG as grading_scheme_code,
        
        -- User Tracking
        OPR_ID as operator_id,
        USER_ID as user_id,
        
        -- Metadata
        {{ add_source_metadata('jenzabar_cx_archive', 'reg_rec') }}
        
    from source
    where CW_NO is not null
)
select * from renamed