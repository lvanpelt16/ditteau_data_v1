{{ config(
    materialized = 'table',
    schema = 'DISTRIBUTE'
) }}
select
    MAJOR as major_code,  -- Uppercase column names
    TXT as major_descr,
    ACTIVE_DATE as source_active_date,
    INACTIVE_DATE as source_inactive_date,
    'Y' as is_current,
    current_timestamp() as creation_timestamp
from {{ source('jenzabar_cx_archive', 'major_table') }}
