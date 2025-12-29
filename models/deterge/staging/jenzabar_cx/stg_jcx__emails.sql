{{ 
    config(
        materialized='view',
        tags=['staging', 'jenzabar', 'email', 'pii']
    )
}}
with email_addresses as (
    select 
        aa.person_id,
        aa.address_type_code,
        lower(trim(aa.address_line_1)) as email_address,  -- Email is in LINE1
        aa.begin_date,
        aa.end_date,
        aa.address_number
    from {{ ref('stg_jcx__alternate_addresses') }} aa
    inner join {{ source('jenzabar_cx_archive', 'aa_table') }} aa_type
        on aa.address_type_code = aa_type.AA
    where aa_type.EMAIL = 'Y'  -- Only email type addresses
      and aa.address_line_1 is not null
      and trim(aa.address_line_1) != ''
      and address_line_1 like '%@%'  
      and address_line_1 not like '% %'  
),
current_emails as (
    select
        person_id,
        email_address,
        address_type_code,
        begin_date,
        end_date,
        -- Rank by most recent begin_date, preferring records with no end_date
        row_number() over (
            partition by person_id 
            order by 
                case when end_date is null then 0 else 1 end,  -- Nulls first
                begin_date desc  -- Most recent
        ) as email_rank
    from email_addresses
    where end_date is null  -- Only current addresses
       or end_date >= current_date()  -- Or future end dates

)
select
    person_id,
    email_address,
    address_type_code,
    begin_date,
    end_date
from current_emails
where email_rank = 1  -- One email per person