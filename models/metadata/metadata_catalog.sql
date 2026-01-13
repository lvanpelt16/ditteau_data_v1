{{ config(materialized='view') }}

SELECT
    table_catalog,
    table_schema,
    table_name,
    column_name,
    data_type,
    ordinal_position
FROM {{ target.database }}.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema IN ('DEPOSIT', 'DETERGE', 'DISTRIBUTE')
ORDER BY table_schema, table_name, ordinal_position