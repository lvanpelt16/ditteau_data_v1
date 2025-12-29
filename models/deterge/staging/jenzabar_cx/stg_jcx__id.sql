{{ 
    config(
        materialized='view',
        tags=['staging', 'jenzabar', 'identity']
    )
}}
with source as (
    select * from {{ source('jenzabar_cx_archive', 'id_rec') }}
),
renamed as (
    select 
        -- Primary Keys
        ID as person_id,
        PRSP_NO as prospect_number,
        
        -- Name Components
        FULLNAME as full_name,
        LASTNAME as last_name,
        FIRSTNAME as first_name,
        MIDDLENAME as middle_name,
        SUFFIXNAME as suffix_name,
        NAME_SNDX as name_soundex,
        PREV_NAME_ID as previous_name_id,
        
        -- Prefix/Title
        TITLE as title,
        SUFFIX as suffix,
        AA as academic_abbreviation,
        
        -- Address
        ADDR_LINE1 as address_line_1,
        ADDR_LINE2 as address_line_2,
        ADDR_LINE3 as address_line_3,
        CITY as city,
        ST as state_code,
        ZIP as postal_code,
        CTRY as country_code,
        
        -- Contact
        PHONE as phone_number,
        PHONE_EXT as phone_extension,
        
        -- Identifiers
        SS_NO as ssn,
        
        -- Flags
        case when MAIL = 'Y' then true when MAIL = 'N' then false else null end as receives_mail,
        case when SOL = 'Y' then true when SOL = 'N' then false else null end as solicitation_flag,
        case when PUB = 'Y' then true when PUB = 'N' then false else null end as publish_flag,
        case when CORRECT_ADDR = 'Y' then true when CORRECT_ADDR = 'N' then false else null end as has_correct_address,
        case when DECSD = 'Y' then true when DECSD = 'N' then false else null end as is_deceased,
        case when VALID = 'Y' then true when VALID = 'N' then false else null end as is_valid,
        
        -- Dates
        cast(ADD_DATE as date) as record_added_date,
        cast(UPD_DATE as date) as last_updated_date,
        cast(PURGE_DATE as date) as purge_date,
        cast(CASS_CERT_DATE as date) as address_certification_date,
        
        -- Admin
        OFC_ADD_BY as added_by_office,
        
        -- Metadata
        {{ add_source_metadata('jenzabar_cx_archive', 'id_rec') }}
        
    from source
    where ID is not null
)
select * from renamed