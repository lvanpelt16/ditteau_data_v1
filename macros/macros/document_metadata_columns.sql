{% macro metadata_column_descriptions() %}
    {#- 
        Returns standard descriptions for metadata columns.
        Use in _models.yml to avoid repeating descriptions.
        
        Usage:
            columns:
              - name: _source_system
                description: {{ metadata_column_descriptions()['_source_system'] }}
    -#}
    
    {% set descriptions = {
        "_source_system": "Source system identifier (e.g., JENZABAR_CX, POWERFAIDS)",
        "_source_table": "Source table name in UPPERCASE",
        "_source_file": "Source file name (for CSV/file-based ingestion)",
        "_data_classification": "Data classification level (PII, RESTRICTED, INTERNAL, PUBLIC)",
        "_data_owner": "Business owner of the data (department or office)",
        "_ingest_type": "Method of data ingestion (SNOWFLAKE_DATA_SHARE, CSV_STAGE, API, etc.)",
        "_source_loaded_at": "Timestamp when data was loaded to source system",
        "_dbt_loaded_at": "Timestamp when dbt last processed this record"
    } %}
    
    {{ return(descriptions) }}
{% endmacro %}