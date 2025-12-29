{{ config(
    materialized = 'table',
    schema = 'DISTRIBUTE'
) }}
select
    DEPT as dept_code,  -- Uppercase column names
    TXT as dept_descr,
    ACTIVE_DATE as source_active_date,
    INACTIVE_DATE as source_inactive_date,
    'Y' as is_current,
    current_timestamp() as creation_timestamp
from {{ source('jenzabar_cx_archive', 'dept_table') }}
