{{ config(
    materialized = 'table',
    schema = 'DISTRIBUTE'
) }}

select
    row_number() over (order by ethnic) as ethnicity_key,
    ethnic as ethnicity_code,
    txt as ethnicity_descr,
    active_date as source_active_date,
    inactive_date as source_inactive_date,
    'Y' as is_current,
    current_timestamp() as creation_timestamp
from {{ source('jenzabar_cx_archive', 'ethnic_table') }}
