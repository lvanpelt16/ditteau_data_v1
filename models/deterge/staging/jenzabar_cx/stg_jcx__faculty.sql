{{ 
    config(
        materialized='view',
        tags=['staging', 'jenzabar', 'faculty', 'hr']
    )
}}
with source as (
    select * from {{ source('jenzabar_cx_archive', 'fac_rec') }}
),
renamed as (
    select 
        -- Primary Key
        ID as faculty_id,
        
        -- Academic Info
        TITLE as title_code,
        RANK as academic_rank_code,
        DEPT as primary_department_code,
        PRIM_ASSOC as primary_association_code,
        
        -- Location
        CAMPUS as campus_code,
        BLDG as building_code,
        ROOM as room_code,
        
        -- Employment Status
        STAT as employment_status_code,
        cast(STAT_DATE as date) as status_date,
        case when PT = 'Y' then true when PT = 'N' then false else null end as is_part_time,
        case when CTRCT = 'Y' then true when CTRCT = 'N' then false else null end as has_contract,
        
        -- Contract/Tenure Dates
        TENURE_DATE as tenure_date,
        CTRCT_BEG_DATE as contract_begin_date,
        CTRCT_MOS as contract_months,
        
        -- Accrual
        ACCRL_METH as accrual_method,
        ACCRL_DATE as accrual_date,
        
        -- Additional Info
        ABBR_NAME as abbreviated_name,
        MEMO1 as memo_line_1,
        MEMO2 as memo_line_2,
        
        -- Metadata
        {{ add_source_metadata('jenzabar_cx_archive', 'fac_rec') }}
        
    from source
    where ID is not null
)
select * from renamed