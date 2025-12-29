{% macro source_meta(source_name, table_name) %}
    {% set src = source(source_name, table_name) %}
    {% if src.meta is defined %}
        {{ return(src.meta) }}
    {% else %}
        {{ return({}) }}
    {% endif %}
{% endmacro %}
