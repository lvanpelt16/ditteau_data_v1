{% macro add_source_metadata_with_timestamp(source_name, table_name, loaded_at_field='_loaded_at') %}
    {#- 
        Enhanced version that includes the source system's load timestamp.
        Use this for sources with a _loaded_at or similar timestamp column.
        
        Args:
            source_name: Name of the source
            table_name: Name of the table
            loaded_at_field: Name of the timestamp column in source (default: _loaded_at)
        
        Usage:
            select
                student_id,
                {{ add_source_metadata_with_timestamp('powerfaids', 'bronze_pf_awards', '_loaded_at') }}
            from source
    -#}
    
    {%- set meta = source_meta(source_name, table_name) -%}
    {%- set defaults = metadata_defaults() -%}
    
    -- Source System Identification
    '{{ meta.get("source_system", defaults["meta_source_system"]) }}' as _source_system,
    '{{ table_name | upper }}' as _source_table,
    
    {%- if meta.get("source_file") %}
    '{{ meta.get("source_file") }}' as _source_file,
    {%- endif %}
    
    -- Data Governance
    '{{ meta.get("data_class", defaults["meta_data_class"]) }}' as _data_classification,
    '{{ meta.get("data_owner", defaults["meta_data_owner"]) }}' as _data_owner,
    
    -- Ingestion Method
    '{{ meta.get("ingest_type", defaults["meta_ingest_type"]) }}' as _ingest_type,
    
    -- Source System Load Timestamp (from ingestion layer)
    {{ loaded_at_field }} as _source_loaded_at,
    
    -- dbt Processing Timestamp
    current_timestamp() as _dbt_loaded_at

{% endmacro %}