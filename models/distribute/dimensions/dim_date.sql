{{ config(
    materialized = 'table'
) }}

WITH RECURSIVE date_series AS (
    SELECT TO_DATE('1980-01-01') AS dt
    UNION ALL
    SELECT DATEADD(day, 1, dt)
    FROM date_series
    WHERE dt < TO_DATE('2035-12-31')
),

primary_terms_per_range AS (
    SELECT
        term_key,
        term_code,
        term_descr,
        term_acad_yr,
        term_sess,
        term_year,
        term_subsess,
        term_program_level,
        term_beg_date,
        term_end_date,
        ROW_NUMBER() OVER (
            PARTITION BY term_beg_date, term_end_date
            ORDER BY
                CASE
                    WHEN term_program_level = 'UNDG' THEN 1
                    WHEN term_program_level = 'GRAD' THEN 2
                    ELSE 3
                END,
                term_key
        ) AS rn_per_range
    FROM {{ ref('dim_term') }}
    WHERE term_beg_date IS NOT NULL AND term_end_date IS NOT NULL
),

daily_term_assignment AS (
    SELECT
        ds.dt,
        pt.term_key,
        pt.term_code,
        pt.term_descr,
        pt.term_acad_yr,
        pt.term_sess,
        pt.term_year,
        pt.term_subsess,
        pt.term_program_level,
        ROW_NUMBER() OVER (
            PARTITION BY ds.dt
            ORDER BY
                pt.term_program_level,
                pt.term_beg_date DESC,
                pt.term_end_date ASC,
                pt.term_key
        ) AS rn_per_day
    FROM date_series ds
    LEFT JOIN primary_terms_per_range pt
        ON ds.dt BETWEEN pt.term_beg_date AND pt.term_end_date
        AND pt.rn_per_range = 1
)

SELECT
    TO_NUMBER(TO_CHAR(dsa.dt, 'YYYYMMDD')) AS date_key,
    dsa.dt AS date_actual,
    DAYOFWEEK(dsa.dt) AS day_of_week_num,
    DAYNAME(dsa.dt) AS day_of_week_name,
    LEFT(DAYNAME(dsa.dt), 3) AS day_of_week_abbr,
    DAYOFMONTH(dsa.dt) AS day_of_month,
    DAYOFYEAR(dsa.dt) AS day_of_year,
    WEEKOFYEAR(dsa.dt) AS week_of_year,
    MONTH(dsa.dt) AS month_num,
    MONTHNAME(dsa.dt) AS month_name,
    LEFT(MONTHNAME(dsa.dt), 3) AS month_abbr,
    QUARTER(dsa.dt) AS quarter_num,
    'Q' || QUARTER(dsa.dt) AS quarter_name,
    YEAR(dsa.dt) AS year_num,
    CASE WHEN DAYOFWEEK(dsa.dt) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN dsa.dt = CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_current_date,

    CASE
        WHEN MONTH(dsa.dt) >= 8 THEN
            TO_VARCHAR(YEAR(dsa.dt)) || '-' || TO_VARCHAR(YEAR(dsa.dt) + 1)
        ELSE
            TO_VARCHAR(YEAR(dsa.dt) - 1) || '-' || TO_VARCHAR(YEAR(dsa.dt))
    END AS academic_year,

    CASE WHEN dsa.term_key IS NOT NULL THEN TRUE ELSE FALSE END AS is_in_term,
    dsa.term_key AS primary_term_key,
    dsa.term_code AS primary_term_code,
    dsa.term_descr AS primary_term_descr,
    dsa.term_acad_yr AS primary_term_acad_yr,
    dsa.term_sess AS primary_term_sess,
    dsa.term_year AS primary_term_year,
    dsa.term_subsess AS primary_term_subsess,
    dsa.term_program_level AS primary_term_program_level,

    CASE
        WHEN MONTH(dsa.dt) >= 7 THEN YEAR(dsa.dt) + 1
        ELSE YEAR(dsa.dt)
    END AS fiscal_year_num,

    CASE
        WHEN MONTH(dsa.dt) BETWEEN 7 AND 9 THEN 1
        WHEN MONTH(dsa.dt) BETWEEN 10 AND 12 THEN 2
        WHEN MONTH(dsa.dt) BETWEEN 1 AND 3 THEN 3
        WHEN MONTH(dsa.dt) BETWEEN 4 AND 6 THEN 4
        ELSE NULL
    END AS fiscal_quarter_num,

    CASE
        WHEN MONTH(dsa.dt) >= 7 THEN MONTH(dsa.dt) - 6
        ELSE MONTH(dsa.dt) + 6
    END AS fiscal_month_num,

    'FY' || TO_VARCHAR(
        CASE
            WHEN MONTH(dsa.dt) >= 7 THEN YEAR(dsa.dt) + 1
            ELSE YEAR(dsa.dt)
        END
    ) || '-Q' || TO_VARCHAR(
        CASE
            WHEN MONTH(dsa.dt) BETWEEN 7 AND 9 THEN 1
            WHEN MONTH(dsa.dt) BETWEEN 10 AND 12 THEN 2
            WHEN MONTH(dsa.dt) BETWEEN 1 AND 3 THEN 3
            WHEN MONTH(dsa.dt) BETWEEN 4 AND 6 THEN 4
        END
    ) AS fiscal_period_name

FROM daily_term_assignment dsa
WHERE dsa.rn_per_day = 1
ORDER BY date_key
