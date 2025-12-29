{{ 
    config(
        materialized='view',
        tags=['staging', 'powerfaids', 'financial_aid']
    )
}}
with source as (
    select * from {{ source('powerfaids', 'bronze_pf_awards') }}
),
renamed as (
    select 
        -- Primary Keys
        STUDENT_ID as student_id,
        AWARD_YEAR as award_year,  -- Note: Not AID_YEAR
        AWARD_CODE as award_code,
        
        -- Award Details
        AWARD_DESCRIPTION as award_description,
        AWARD_TYPE as award_type,
        AWARD_STATUS as award_status,
        
        -- Award Amounts
        cast(AWARD_AMOUNT_OFFERED as decimal(15,2)) as award_amount_offered,
        cast(AWARD_AMOUNT_ACCEPTED as decimal(15,2)) as award_amount_accepted,
        
        -- Disbursement Information
        DISBURSEMENT_STATUS as disbursement_status,
        
        -- Disbursement 1
        try_cast(DISBURSEMENT_DATE as date) as disbursement_date_1,
        cast(DISBURSEMENT_AMOUNT_1 as decimal(15,2)) as disbursement_amount_1,
        
        -- Disbursement 2
        try_cast(DISBURSEMENT_DATE_SCHEDULED_2 as date) as disbursement_date_2,
        cast(DISBURSEMENT_AMOUNT_2 as decimal(15,2)) as disbursement_amount_2,
        
        -- Audit Dates
        try_cast(CREATION_DATE as timestamp) as record_created_at,
        try_cast(LAST_UPDATE_DATE as timestamp) as last_updated_at,
        
        -- Metadata
        {{ add_source_metadata('powerfaids', 'bronze_pf_awards') }}
        
    from source
    where STUDENT_ID is not null
)
select * from renamed