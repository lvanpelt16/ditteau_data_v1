{% macro apply_metadata(this, meta_source_system, meta_data_owner, meta_data_class) %}
    {% set sql %}
        ALTER TABLE {{ this }} 
        SET TAG "META_SOURCE_SYSTEM" = '{{ meta_source_system }}',
                "META_DATA_OWNER" = '{{ meta_data_owner }}',
                "META_DATA_CLASS" = '{{ meta_data_class }}';
    {% endset %}
    {{ run_query(sql) }}
{% endmacro %}
