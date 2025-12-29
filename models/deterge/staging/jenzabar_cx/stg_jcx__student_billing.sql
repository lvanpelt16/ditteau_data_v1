{{ 
    config(
        materialized='view',
        tags=['staging', 'jenzabar', 'billing', 'financial']
    )
}}
with source as (
    select * from {{ source('jenzabar_cx_archive', 'sbcust_rec') }}
),
renamed as (
    select 
        -- Primary Keys
        ID as student_id,
        PROG as program_code,
        SESS as term_code,
        YR as academic_year,
        
        -- Dollar Value (Note: No BAL column, only DOLLAR_VALUE!)
        DOLLAR_VALUE as account_value,
        
        -- Housing
        case when ON_CAMPUS = 'Y' then true when ON_CAMPUS = 'N' then false else null end as lives_on_campus,
        
        -- Fees/Services
        case when TEST = 'Y' then true when TEST = 'N' then false else null end as has_testing_fee,
        case when PARK = 'Y' then true when PARK = 'N' then false else null end as has_parking,
        case when ASB = 'Y' then true when ASB = 'N' then false else null end as has_asb_fee,
        case when INS = 'Y' then true when INS = 'N' then false else null end as has_insurance,
        case when ACCIDENT = 'Y' then true when ACCIDENT = 'N' then false else null end as has_accident_insurance,
        case when HEALTH = 'Y' then true when HEALTH = 'N' then false else null end as has_health_insurance,
        
        -- Exemptions
        case when EXEMPT = 'Y' then true when EXEMPT = 'N' then false else null end as is_exempt,
        
        -- Account Status
        case when ACCT_CLR = 'Y' then true when ACCT_CLR = 'N' then false else null end as account_cleared,
        
        -- Payment Gateway
        case when CASHNET_MPP = 'Y' then true when CASHNET_MPP = 'N' then false else null end as uses_cashnet_mpp,
        
        -- Metadata
        {{ add_source_metadata('jenzabar_cx_archive', 'sbcust_rec') }}
        
    from source
    where ID is not null
)
select * from renamed