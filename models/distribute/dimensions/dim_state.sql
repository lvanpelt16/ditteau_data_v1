{{
    config(
        materialized='table',
        tags=['dimension', 'reference_data']
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['state_code']) }} as state_key,
    state_code,
    state_name
    
    
    -- Metadata
    current_timestamp() as _dbt_loaded_at,
    'SEED' as _source_system,
    'PUBLIC' as _data_classification,
    'COMMON' as _data_owner
    
from {{ ref('dim_state_seed') }}