{% macro metadata_defaults() %}
    {#- 
        Default values for metadata columns when not specified in source.
        Returns a dictionary of default values.
    -#}
    {% set defaults = {
        "meta_source_system": "UNKNOWN",
        "meta_source_file": "UNKNOWN",
        "meta_ingest_type": "UNKNOWN",
        "meta_data_owner": "UNKNOWN",
        "meta_data_class": "INTERNAL"
    } %}
    {{ return(defaults) }}
{% endmacro %}