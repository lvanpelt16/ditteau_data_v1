{{
    config(
        materialized='table',
        schema='distribute',
        tags=['dimension']
    )
}}

select
    -- Surrogate key (primary key for dimension)
    {{ dbt_utils.generate_surrogate_key(['DEPT']) }} as dept_key,

    -- Natural key (business identifier)
    DEPT as dept_code,

    -- Descriptive attributes
    TXT as dept_descr,

    -- Status tracking
    ACTIVE_DATE as source_active_date,
    INACTIVE_DATE as source_inactive_date,
    case
        when INACTIVE_DATE is null then true
        else false
    end as is_active,

    -- Metadata
    true as is_current,  -- For SCD Type 2 if needed later
    current_timestamp() as creation_timestamp,
    current_timestamp() as last_modified_timestamp

from {{ source('jenzabar_cx_archive', 'dept_table') }}
