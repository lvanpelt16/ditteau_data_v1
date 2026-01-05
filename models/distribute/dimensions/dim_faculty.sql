{{ config(
    materialized = 'table'
) }}
select
    row_number() over (order by faculty_id) as faculty_key,
    a.faculty_id as faculty_code, 
    b.last_name as faculty_lastname, 
    b.first_name as faculty_firstname, 
    b.middle_name as faculty_middlename, 
    b.title as faculty_prefix, 
    b.suffix as faculty_suffix, 
    a.title_code as faculty_rank, 
    d.dept_code as faculty_dept, 
    a.status_date as source_active_date,
    to_date('9999-12-31') as source_inactive_date,
    'Y' as is_current,
    current_timestamp() as creation_timestamp
from {{ ref('stg_jcx__faculty') }} a
join {{ ref('stg_jcx__id') }} b on a.faculty_id = b.person_id 
join {{ ref('dim_dept') }} d on a.primary_department_code = d.dept_code
