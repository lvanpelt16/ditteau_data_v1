{{
    config(
        materialized='table',
        tags=['dimension', 'reference_data']
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['gender_code']) }} as gender_key,
    gender_code,
    gender_name,
    gender_category,
    is_active,
    sort_order,
    
    -- Metadata
    current_timestamp() as _dbt_loaded_at,
    'SEED' as _source_system,
    'PUBLIC' as _data_classification,
    'COMMON' as _data_owner
    
from {{ ref('dim_gender_seed') }}
where is_active = true