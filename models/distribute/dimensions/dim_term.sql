{{
    config(
        materialized='table',
        schema='distribute',
        tags=['dimension']
    )
}}

select
    -- Surrogate key (primary key for dimension)
    {{ dbt_utils.generate_surrogate_key([
        "TRIM(a.sess)",
        "TRIM(REPLACE(a.subsess, ' ', '-'))",
        "a.yr",
        "TRIM(a.prog)"
    ]) }} as term_key,

    -- Natural key (business identifier)
    TRIM(a.sess) || TRIM(REPLACE(a.subsess, ' ', '-')) || a.yr || TRIM(a.prog) as term_code,

    -- Descriptive attributes
    'Session: ' || TRIM(a.sess) ||
        ' Year: ' || a.yr ||
        ' Subsession: ' || TRIM(REPLACE(a.subsess, ' ', '-')) ||
        ' Program: ' || TRIM(a.prog) as term_descr,
    a.acyr as term_acad_yr,
    a.sess as term_sess,
    a.yr as term_year,
    a.subsess as term_subsess,
    a.prog as term_program_level,
    a.acyr as term_award_yr,
    a.acyr as term_fiscal_yr,
    a.beg_date as term_beg_date,
    a.end_date as term_end_date,

    -- Status tracking
    a.beg_date as source_active_date,
    a.end_date as source_inactive_date,
    case
        when a.end_date is null or a.end_date >= current_date() then true
        else false
    end as is_active,

    -- Metadata
    true as is_current,  -- For SCD Type 2 if needed later
    current_timestamp() as creation_timestamp,
    current_timestamp() as last_modified_timestamp

from {{ source('jenzabar_cx_archive', 'acad_cal_rec') }} a
