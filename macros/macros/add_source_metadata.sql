{% macro add_source_metadata(source_name, table_name) %}
    {#- 
        Adds standardized metadata columns to staging models.
        Pulls metadata from source definition when available, uses defaults otherwise.
        
        Args:
            source_name: Name of the source (from sources.yml)
            table_name: Name of the table within that source
        
        Returns:
            SQL column definitions (without trailing comma)
        
        Usage:
            select
                id_num as student_id,
                {{ add_source_metadata('jenzabar_cx_archive', 'prog_enr_rec') }}
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
    
    -- dbt Processing Timestamp
    current_timestamp() as _dbt_loaded_at

{% endmacro %}