{{ 
    config(
        materialized='view',
        tags=['staging', 'jenzabar', 'demographics', 'pii']
    )
}}
with source as (
    select * from {{ source('jenzabar_cx_archive', 'profile_rec') }}
),
renamed as (
    select 
        -- Primary Key
        ID as person_id,
        
        -- Demographics
        ETHNIC_CODE as ethnicity_code,
        ETHNIC_CODE2 as ethnicity_code_2,
        RACE as race_code,
        case when HISPANIC = 'Y' then true when HISPANIC = 'N' then false else null end as is_hispanic,
        SEX as gender_code,
        MRTL as marital_status_code,
        
        -- Birth Information
        cast(BIRTH_DATE as date) as birth_date,
        AGE as age,
        BIRTHPLACE_CITY as birth_city,
        BIRTHPLACE_ST as birth_state,
        
        -- Residency
        RES_ST as residence_state,
        RES_CTY as residence_county,
        RES_CTRY as residence_country,
        PROF_RES_CODE as professional_residence_code,
        cast(PROF_RES_DATE as date) as professional_residence_date,
        
        -- Citizenship
        CITZ as citizenship_code,
        CITZ2 as citizenship_code_2,
        VISA_CODE as visa_code,
        cast(PROF_VISA_DATE as date) as visa_date,
        PROF_VISA_NO as visa_number,
        
        -- Military/Veteran
        case when VET = 'Y' then true when VET = 'N' then false else null end as is_veteran,
        PROF_VET_CHAP as veteran_chapter,
        MILIT_CODE as military_code,
        
        -- Disability
        HANDICAP_CODE as disability_code,
        
        -- Religious
        DENOM_CODE as denomination_code,
        CHURCH_ID as church_id,
        
        -- Occupation
        OCC as occupation_code,
        
        -- News/Media
        NEWS1_ID as news_contact_1_id,
        NEWS2_ID as news_contact_2_id,
        
        -- Privacy
        PRIV_CODE as privacy_code,
        GRP_NO as group_number,
        
        -- Dates
        cast(PROF_LAST_UPD_DATE as date) as last_updated_date,
        cast(DECSD_DATE as date) as deceased_date,
        
        -- Additional
        LANG as language_code,
        PIN as pin_number,
        PHOTO as photo_binary,
        
        -- Metadata
        {{ add_source_metadata('jenzabar_cx_archive', 'profile_rec') }}
        
    from source
    where ID is not null
)
select * from renamed