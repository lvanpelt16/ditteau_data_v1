{{ 
    config(
        materialized='view',
        tags=['staging', 'jenzabar', 'contact', 'pii']
    )
}}
with source as (
    select * from {{ source('jenzabar_cx_archive', 'aa_rec') }}
),
renamed as (
    select 
        -- Primary Keys
        AA_NO as address_number,
        ID as person_id,
        AA as address_type_code,
        
        -- Dates
        cast(BEG_DATE as date) as begin_date,
        cast(END_DATE as date) as end_date,
        
        -- Address Lines
        LINE1 as address_line_1,
        LINE2 as address_line_2,
        LINE3 as address_line_3,
        
        -- Location
        CITY as city,
        ST as state_code,
        ZIP as postal_code,
        CTRY as country_code,
        
        -- Contact
        PHONE as phone_number,
        PHONE_EXT as phone_extension,
        CTRY_PREFIX as country_prefix,
        
        -- Flags
        case when PEREN = 'Y' then true when PEREN = 'N' then false else null end as is_effective_yearly,
        
        -- Admin
        OFC_ADD_BY as added_by_office,
        cast(CASS_CERT_DATE as date) as address_certification_date,
        
        -- Metadata
        {{ add_source_metadata('jenzabar_cx_archive', 'aa_rec') }}
        
    from source
    where ID is not null
)
select * from renamed