{% macro safe_cast(column_name, data_type, default_value='null') %}
    case
        when {{ column_name }} is null then {{ default_value }}
        when trim({{ column_name }}) = '' then {{ default_value }}
        else cast({{ column_name }} as {{ data_type }})
    end
{% endmacro %}