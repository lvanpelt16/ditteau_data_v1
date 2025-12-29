{{ config(
    materialized = 'table',
    schema = 'DISTRIBUTE'
) }}

select
    deg as degree_code,
    txt as degree_descr,
    active_date as source_active_date,
    inactive_date as source_inactive_date,
    'Y' as is_current,
    current_timestamp() as creation_timestamp
from {{ source('jenzabar_cx_archive', 'deg_table') }}
