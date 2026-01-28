{{
    config(
        materialized='table',
        tags=['intermediate', 'academic', 'enrollment']
    )
}}

/*
Purpose: Course registrations with registration event history
Source: CW_REC (via deterge_cx_cw_rec_enrich


ed), REG_REC (via stg_jcx__registrations)
Grain: One row per course work record (student course enrollment)
Key: course_work_number
*/

with course_work as (
    select * from {{ ref('deterge_cx_cw_rec_enriched') }}
),

registration_history as (
    select * from {{ ref('stg_jcx__registrations') }}
),

-- Aggregate registration events per course work record
registration_summary as (
    select
        course_work_number,
        
        -- Count of registration events
        count(*) as total_registration_events,
        
        -- Event dates (using actual columns from REG_REC)
        min(event_date) as first_event_date,
        max(event_date) as latest_event_date,
        max(system_date) as latest_system_date,
        
        -- Current/final status from most recent record
        max_by(registration_status_code, event_date) as latest_registration_status,
        
        -- Status tracking
        count(distinct registration_status_code) as unique_status_count
        
    from registration_history
    where course_work_number is not null
    group by course_work_number
),

-- Combine course work with registration summary
integrated as (
    select
        -- Primary identifiers
        cw.CW_NO as course_work_number,
        cw.ID as student_id,
        cw.YR as academic_year,
        cw.SESS as term_code,
        cw.SUBSESS as subsession_code,
        
        -- Course identification
        cw.PROG as program_code,
        cw.CRS_NO as course_number,
        cw.CAT as catalog_code,
        cw.SEC as section_number,
        
        -- Academic performance
        cw.GRD as final_grade,
        cw.MIDSESS_GRD as midsession_grade,
        
        -- Credit information
        cw.HRS as credit_hours,
        cw.CLOCK_HRS as clock_hours,
        
        -- Dates
        cast(cw.BEG_DATE as date) as begin_date,
        cast(cw.END_DATE as date) as end_date,
        
        -- Status
        cw.STAT as course_work_status,
        reg.latest_registration_status,
        
        -- Registration history summary
        coalesce(reg.total_registration_events, 0) as total_registration_events,
        reg.first_event_date,
        reg.latest_event_date,
        reg.latest_system_date,
        
        -- Enriched fields from course work
        cw.first_event_type,
        cw.first_event_timestamp,
        cw.latest_event_type_final as latest_event_type,
        cw.latest_event_timestamp,
        
        -- Business logic
        case
            when reg.total_registration_events > 1 then true
            else false
        end as has_multiple_registration_attempts,
        
        -- Metadata
        cw.meta_ingest_ts as _dbt_loaded_at
        
    from course_work cw
    left join registration_summary reg
        on cw.CW_NO = reg.course_work_number
)

select * from integrated