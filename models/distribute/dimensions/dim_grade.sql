{{ config(
    materialized = 'table',
    schema = 'DISTRIBUTE'
) }}
select
    row_number() over (order by GRD) as grade_key,  -- ✅ ADD (uppercase source)
    GRD as grade_code,  -- Uppercase
    TXT as grade_descr,  -- Uppercase
    CTGRY as grade_category,  -- Uppercase
    PTS as grade_pts,  -- Uppercase
    ACTIVE_DATE as source_active_date,  -- Uppercase
    INACTIVE_DATE as source_inactive_date,  -- Uppercase
    'Y' as is_current,
    current_timestamp() as creation_timestamp
from {{ source('jenzabar_cx_archive', 'grd_table') }}