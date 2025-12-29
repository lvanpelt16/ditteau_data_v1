{{ 
    config(
        materialized='view',
        tags=['staging', 'jenzabar', 'schedule', 'sections']
    )
}}
-- Note: SECMTG_REC is just a join table with only 6 columns
-- It links sections to meetings - no additional data
with source as (
    select * from {{ source('jenzabar_cx_archive', 'secmtg_rec') }}
),
renamed as (
    select 
        -- Section Identifiers
        CRS_NO as course_number,
        CAT as catalog_code,
        YR as academic_year,
        SESS as term_code,
        SEC_NO as section_number,
        
        -- Meeting Identifier
        MTG_NO as meeting_number,
        
        -- Metadata
        {{ add_source_metadata('jenzabar_cx_archive', 'secmtg_rec') }}
        
    from source
)
select * from renamed