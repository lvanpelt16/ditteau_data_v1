{{ config(materialized='table') }}
-- Counts per table for audit tracking
SELECT
    table_schema,
    table_name,
    COUNT(*) AS column_count,
    CURRENT_TIMESTAMP() AS audit_ts
FROM {{ ref('metadata_catalog') }}
GROUP BY table_schema, table_name
ORDER BY table_schema, table_name