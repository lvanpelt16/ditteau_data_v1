{{ config(
    materialized='table'
    alias='DIM_TERM',
    tags=['gold', 'dimension']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        "TRIM(a.sess)",
        "TRIM(REPLACE(a.subsess, ' ', '-'))",
        "a.yr",
        "TRIM(a.prog)"
    ]) }} AS TERM_KEY,

    TRIM(a.sess) || TRIM(REPLACE(a.subsess, ' ', '-')) || a.yr || TRIM(a.prog) AS TERM_CODE,
    'Session: ' || TRIM(a.sess) || 
        ' Year: ' || a.yr || 
        ' Subsession: ' || TRIM(REPLACE(a.subsess, ' ', '-')) || 
        ' Program: ' || TRIM(a.prog) AS TERM_DESCR,
    a.acyr AS TERM_ACAD_YR,
    a.sess AS TERM_SESS,
    a.yr AS TERM_YEAR,
    a.subsess AS TERM_SUBSESS,
    a.prog AS TERM_PROGRAM_LEVEL,
    a.acyr AS TERM_AWARD_YR,
    a.acyr AS TERM_FISCAL_YR,
    a.beg_date AS TERM_BEG_DATE,
    a.end_date AS TERM_END_DATE,
    a.beg_date AS SOURCE_ACTIVE_DATE,
    TO_DATE('9999-12-31') AS SOURCE_INACTIVE_DATE,
    'Y' AS IS_CURRENT,
    CURRENT_TIMESTAMP() AS CREATION_TIMESTAMP
FROM {{ source('jenzabar_cx_archive', 'acad_cal_rec') }} a
