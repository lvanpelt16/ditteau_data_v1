{{ 
    config(
        materialized='view',
        tags=['staging', 'jenzabar', 'student_services', 'housing']
    )
}}
with source as (
    select * from {{ source('jenzabar_cx_archive', 'stu_serv_rec') }}
),
renamed as (
    select 
        -- Primary Keys
        ID as student_id,
        SESS as term_code,
        YR as academic_year,
        STUSV_NO as student_services_number,
        
        -- Emergency Contact
        EMER_PHONE as emergency_phone,
        EMER_PHONE_EXT as emergency_phone_extension,
        EMER_CTC_NAME as emergency_contact_name,
        
        -- Housing Information
        case when INTEND_HSG = 'Y' then true when INTEND_HSG = 'N' then false else null end as intends_housing,
        case when OFFCAMPUS_RES_APPR = 'Y' then true when OFFCAMPUS_RES_APPR = 'N' then false else null end as off_campus_approved,
        CAMPUS as campus_code,
        BLDG as building_code,
        ROOM as room_code,
        SUITE as suite_code,
        NO_PER_ROOM as number_per_room,
        case when REQ_SINGLE = 'Y' then true when REQ_SINGLE = 'N' then false else null end as requests_single_room,
        PREF_RM_TYPE as preferred_room_type,
        ROOMMATE_STS as roommate_status,
        case when RES_ASST = 'Y' then true when RES_ASST = 'N' then false else null end as is_resident_assistant,
        
        -- Campus Services
        CAMPUS_BOX as campus_box_number,
        
        -- Fees/Waivers
        case when ASB_FEE_WVD = 'Y' then true when ASB_FEE_WVD = 'N' then false else null end as asb_fee_waived,
        case when HLTH_INS_WVD = 'Y' then true when HLTH_INS_WVD = 'N' then false else null end as health_insurance_waived,
        case when MEAL_PLAN_WVD = 'Y' then true when MEAL_PLAN_WVD = 'N' then false else null end as meal_plan_waived,
        MEAL_PLAN_TYPE as meal_plan_type,
        case when LATE_REG = 'Y' then true when LATE_REG = 'N' then false else null end as is_late_registration,
        
        -- Parking
        PARK_PRMT_NO as parking_permit_number,
        cast(PARK_PRMT_EXP_DATE as date) as parking_permit_expiration_date,
        PARK_LOCATION as parking_location,
        LOT_NO as lot_number,
        
        -- Vehicle Information
        VEH_TYPE as vehicle_type,
        VEH_LICENSE as vehicle_license,
        VEH_LIC_ST as vehicle_license_state,
        VEH_YEAR as vehicle_year,
        VEH_MAKE as vehicle_make,
        VEH_MODEL as vehicle_model,
        
        -- Status
        STAT as status_code,
        RSV_STAT as reservation_status,
        
        -- Dates
        cast(CRM_ADD_DATE as date) as record_added_date,
        cast(CRM_UPD_DATE as date) as record_updated_date,
        case when LTR_SENT = 'Y' then true when LTR_SENT = 'N' then false else null end as letter_sent,
        
        -- Metadata
        {{ add_source_metadata('jenzabar_cx_archive', 'stu_serv_rec') }}
        
    from source
    where ID is not null
)
select * from renamed