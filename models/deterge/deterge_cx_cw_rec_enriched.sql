{{ config(
    materialized='table',
    schema='deterge',
    alias='CX_CW_REC_ENRICHED'
) }}
WITH reg_events AS (
    SELECT
        cw_no,
        stat AS event_type,
        TO_TIMESTAMP(sys_date || LPAD(tm::STRING, 4, '0'), 'YYYY-MM-DDHH24MI') AS event_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY cw_no 
            ORDER BY beg_date ASC, TO_TIMESTAMP(sys_date || LPAD(tm::STRING, 4, '0'), 'YYYY-MM-DDHH24MI') ASC
        ) AS event_rank_asc,
        ROW_NUMBER() OVER (
            PARTITION BY cw_no 
            ORDER BY beg_date DESC, TO_TIMESTAMP(sys_date || LPAD(tm::STRING, 4, '0'), 'YYYY-MM-DDHH24MI') DESC
        ) AS event_rank_desc,
        COUNT(*) OVER (
            PARTITION BY cw_no, beg_date, TO_TIMESTAMP(sys_date || LPAD(tm::STRING, 4, '0'), 'YYYY-MM-DDHH24MI')
        ) AS timestamp_event_count
    FROM {{ source('jenzabar_cx_archive', 'reg_rec') }}
),
first_last_events AS (
    SELECT
        cw_no,
        MAX(CASE WHEN event_rank_asc = 1 THEN event_type END) AS first_event_type,
        MAX(CASE WHEN event_rank_asc = 1 THEN event_timestamp END) AS first_event_timestamp,
        MAX(CASE WHEN event_rank_desc = 1 THEN event_type END) AS latest_event_type_from_reg,
        MAX(CASE WHEN event_rank_desc = 1 THEN event_timestamp END) AS latest_event_timestamp,
        MAX(CASE WHEN timestamp_event_count > 1 THEN TRUE ELSE FALSE END) AS is_event_timestamp_ambiguous
    FROM reg_events
    GROUP BY cw_no
)
SELECT
    cw.*,
    e.first_event_type,
    e.first_event_timestamp,
    e.latest_event_type_from_reg,
    e.latest_event_timestamp,
    e.is_event_timestamp_ambiguous,
    CASE 
        WHEN e.is_event_timestamp_ambiguous THEN cw.stat
        ELSE e.latest_event_type_from_reg
    END AS latest_event_type_final,
    -- metadata columns (added in SELECT, not UPDATE)
    CURRENT_TIMESTAMP() AS meta_ingest_ts,
    'TRANSFORM' AS meta_source_system,
    'DBT_MODEL' AS meta_source_file,
    '001' AS meta_batch_id,
    'ENRICHED' AS meta_ingest_type,
    TRUE AS meta_load_stat,
    'Student' AS meta_data_owner,
    'Internal' AS meta_data_class
FROM {{ source('jenzabar_cx_archive', 'cw_rec') }} cw
LEFT JOIN first_last_events e
    ON cw.cw_no = e.cw_no