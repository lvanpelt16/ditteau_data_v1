{{
    config(
        materialized='table',
        schema='distribute',
        tags=['dimension']
    )
}}

select
    -- Surrogate key (primary key for dimension)
    {{ dbt_utils.generate_surrogate_key(['major']) }} as major_key,
    
    -- Natural key (business identifier)
    major as major_code,
    
    -- Descriptive attributes
    txt as major_descr,
    
    -- Status tracking
    active_date as source_active_date,
    inactive_date as source_inactive_date,
    case 
        when inactive_date is null then true 
        else false 
    end as is_active,
    
    -- Metadata
    true as is_current,  -- For SCD Type 2 if needed later
    current_timestamp() as creation_timestamp,
    current_timestamp() as last_modified_timestamp
    
from {{ source('jenzabar_cx_archive', 'major_table') }}