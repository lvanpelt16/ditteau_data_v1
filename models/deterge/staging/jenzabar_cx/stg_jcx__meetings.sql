{{ 
    config(
        materialized='view',
        tags=['staging', 'jenzabar', 'schedule', 'meetings']
    )
}}
with source as (
    select * from {{ source('jenzabar_cx_archive', 'mtg_rec') }}
),
renamed as (
    select 
        -- Primary Key
        MTG_NO as meeting_number,  -- Note: Not MEETING_NO!
        
        -- Term Context
        YR as academic_year,
        SESS as term_code,
        
        -- Location
        CAMPUS as campus_code,
        BLDG as building_code,
        ROOM as room_code,
        
        -- Time Schedule
        BEG_TM as begin_time,
        END_TM as end_time,
        DAYS as meeting_days_code,
        
        -- Dates
        cast(BEG_DATE as date) as begin_date,
        cast(END_DATE as date) as end_date,
        
        -- Hours
        HRS as contact_hours,
        CALC_TOT_HRS as calculated_total_hours,
        TOT_HRS as total_hours,
        
        -- Instructional Method
        IM as instructional_method_code,
        
        -- Flags
        case when RESERVE = 'Y' then true when RESERVE = 'N' then false else null end as is_reserved,
        case when SCHD_PRINT = 'Y' then true when SCHD_PRINT = 'N' then false else null end as prints_on_schedule,
        
        -- Status
        STAT as status_code,
        
        -- S25 Integration
        case when S25_EXPORT = 'Y' then true when S25_EXPORT = 'N' then false else null end as exports_to_s25,
        case when S25_ASSIGN = 'Y' then true when S25_ASSIGN = 'N' then false else null end as assigned_in_s25,
        
        -- Metadata
        {{ add_source_metadata('jenzabar_cx_archive', 'mtg_rec') }}
        
    from source
    where MTG_NO is not null
)
select * from renamed