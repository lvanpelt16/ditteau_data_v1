-- models/fact/fact_enrollment.sql
SELECT
    a.CW_NO AS enrollment_source_serial, 
    b.student_key AS enrollment_stu_key, 
    c.course_section_key AS enrollment_course_section_key,
    e.term_key AS enrollment_term_key,
    c.course_section_faculty_key AS enrollment_faculty_key,
    g.grade_key AS enrollment_grade_key,
    a.HRS AS enrollment_hours,  -- Uppercase
    a.STAT AS enrollment_status,  -- Uppercase
    a.PROG AS enrollment_program_level,  -- Uppercase
    a.first_event_type AS enrollment_first_stat,
    h.date_key AS enrollment_first_date_key,
    a.latest_event_type_final AS enrollment_latest_stat,
    i.date_key AS enrollment_latest_date_key,
    a.BEG_DATE AS source_active_date,  -- Uppercase
    a.END_DATE AS source_inactive_date,  -- Uppercase
    'Y' AS is_current,
    CURRENT_TIMESTAMP() AS creation_timestamp
FROM {{ ref('deterge_cx_cw_rec_enriched') }} a
INNER JOIN {{ ref('dim_student') }} b
    ON a.ID = b.student_code  -- Uppercase
    AND a.PROG = b.student_program_level  -- Uppercase
INNER JOIN {{ ref('dim_course_section') }} c
    ON a.CRS_NO = c.course_section_course_number  -- Uppercase
    AND a.SEC = c.course_section_section_number  -- Uppercase
    AND a.CAT = c.course_section_catalog  -- Uppercase
    AND a.PROG = c.course_section_program_level  -- Uppercase
    AND a.SUBSESS = c.course_section_subsess  -- Uppercase
LEFT JOIN {{ ref('dim_term') }} e
    ON a.SESS = e.term_sess  -- Uppercase
    AND a.YR = e.term_year  -- Uppercase
    AND a.PROG = e.term_program_level  -- Uppercase
    AND a.SUBSESS = e.term_subsess  -- Uppercase
LEFT JOIN {{ ref('dim_grade') }} g
    ON a.GRD = g.grade_code  -- Uppercase
    AND a.GRDG = g.grade_category  -- Uppercase
LEFT JOIN {{ ref('dim_date') }} h
    ON a.first_event_timestamp::date = h.date_actual
LEFT JOIN {{ ref('dim_date') }} i
    ON a.latest_event_timestamp::date = i.date_actual