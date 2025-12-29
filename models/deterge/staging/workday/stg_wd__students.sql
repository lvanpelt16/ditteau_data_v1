{{
    config(
        materialized='view'
    )
}}

/*
Purpose: Staging model for Workday Student data
Source: DEPOSIT.STA_STUDENT_WD
Grain: One row per student
Transformations: Column renaming, type casting, null standardization
Owner: Data Engineering

Notes:
- Workday uses REFERENCEID_62792180 as primary student ID
- This is the NEW system of record (Jenzabar CX is legacy)
- Students may exist in both systems during migration
*/

with source as (
    select * from {{ source('workday', 'sta_student_wd') }}
),

renamed as (
    select
        -- =====================================================================
        -- PRIMARY KEY
        -- =====================================================================
        referenceid_62792180 as student_id,  -- Workday student ID (primary)
        
        -- =====================================================================
        -- NAME FIELDS
        -- =====================================================================
        -- Use preferred name if available, otherwise legal first name
        coalesce(
            nullif(trim(preferred_first_name), ''),
            nullif(trim(firstname), '')
        ) as first_name,
        
        nullif(trim(lastname), '') as last_name,
        
        -- Middle names (Workday has two middle name fields!)
        coalesce(
            nullif(trim(middle_name_9181921), ''),
            nullif(trim(middle_name_349405), '')
        ) as middle_name,
        
        -- Various name formats (useful for matching)
        nullif(trim(full_legal_name), '') as full_legal_name,
        nullif(trim(full_name_fml), '') as full_name_fml,
        nullif(trim(full_name_lfmi), '') as full_name_lfmi,
        nullif(trim(legal_name), '') as legal_name,
        nullif(trim(preferred_name), '') as preferred_name,
        nullif(trim(prefix), '') as name_prefix,
        
        -- =====================================================================
        -- CONTACT INFORMATION
        -- =====================================================================
        -- Email (key matching field)
        case
            when trim(primary_institutional_email_address) = '' then null
            else lower(trim(primary_institutional_email_address))
        end as email,
        
        -- =====================================================================
        -- ADDRESS
        -- =====================================================================
        nullif(trim(primary_home_address_city), '') as city,
        nullif(trim(primary_home_address_state), '') as state,
        nullif(trim(primary_home_address_postal_code), '') as postal_code,
        nullif(trim(primary_home_address_county), '') as county,
        nullif(trim(primary_home_address_country), '') as country,
        
        -- =====================================================================
        -- DEMOGRAPHICS
        -- =====================================================================
        date_of_birth as birth_date,
        age,
        nullif(trim(age_group), '') as age_group,
        
        -- Gender fields
        nullif(trim(gender), '') as gender,
        nullif(trim(gender_identity), '') as gender_identity,
        nullif(trim(sexual_orientation), '') as sexual_orientation,
        nullif(trim(pronoun), '') as pronoun,
        
        -- Race/Ethnicity
        nullif(trim(race_ethnicities), '') as race_ethnicities,
        nullif(trim(rewrite_race_ethnicity_for_dw), '') as race_ethnicity_dw,
        case when hispanic_or_latino = 1 then true else false end as is_hispanic_or_latino,
        
        -- Citizenship/Nationality
        nullif(trim(citizenship_status), '') as citizenship_status,
        nullif(trim(citizenship_countries), '') as citizenship_countries,
        nullif(trim(primary_nationality), '') as primary_nationality,
        nullif(trim(primary_nationality_citizenship_status), '') as nationality_citizenship_status,
        nullif(trim(countries_of_residence), '') as countries_of_residence,
        nullif(trim(region_of_birth), '') as region_of_birth,
        
        -- Other demographics
        nullif(trim(marital_status), '') as marital_status,
        nullif(trim(religion), '') as religion,
        nullif(trim(languages), '') as languages,
        
        -- =====================================================================
        -- ACADEMIC INFORMATION
        -- =====================================================================
        nullif(trim(academic_unit), '') as academic_unit,
        nullif(trim(primary_academic_level), '') as primary_academic_level,
        nullif(trim(primary_program_of_study), '') as primary_program_of_study,
        
        -- Academic records
        nullif(trim(academicrecords), '') as academic_records,
        nullif(trim(academic_record_status), '') as academic_record_status,
        nullif(trim(active_academic_records), '') as active_academic_records,
        nullif(trim(active_reporting_records), '') as active_reporting_records,
        
        -- Class information
        classrank as class_rank,
        classsize as class_size,
        
        -- =====================================================================
        -- STUDENT STATUS FLAGS
        -- =====================================================================
        -- Active status
        case when active_matriculated_student = 1 then true else false end as is_active_matriculated,
        case when matriculated_student = 1 then true else false end as is_matriculated,
        case when activeworker = 1 then true else false end as is_active_worker,
        case when graduated = 1 then true else false end as is_graduated,
        case when deceased = 1 then true else false end as is_deceased,
        
        -- Special statuses
        case when first_generation_college_student = 1 then true else false end as is_first_generation,
        case when has_national_identifier = 1 then true else false end as has_national_identifier,
        case when privacy_block = 1 then true else false end as has_privacy_block,
        
        -- =====================================================================
        -- MILITARY
        -- =====================================================================
        case when military_relationship = 1 then true else false end as has_military_relationship,
        case when parent_on_active_duty = 1 then true else false end as parent_on_active_duty,
        nullif(trim(military_service), '') as military_service,
        nullif(trim(military_status_for_benefits), '') as military_status_for_benefits,
        
        -- =====================================================================
        -- HISTORICAL/MERGE TRACKING
        -- =====================================================================
        nullif(trim(activated_historical_student), '') as activated_historical_student,
        historical_student_activation_date_and_time as historical_activation_datetime,
        nullif(trim(matching_historical_student), '') as matching_historical_student,
        nullif(trim(match_status), '') as match_status,
        case when is_unmerged = 1 then true else false end as is_unmerged,
        nullif(trim(primary_merged_student), '') as primary_merged_student,
        nullif(trim(former_worker), '') as former_worker,
        
        -- =====================================================================
        -- OTHER RECORDS/ACTIVITIES
        -- =====================================================================
        nullif(trim(active_student_applications), '') as active_student_applications,
        nullif(trim(awards_and_activities), '') as awards_and_activities,
        nullif(trim(extracurricular_activity), '') as extracurricular_activities,
        nullif(trim(current_housing_status), '') as current_housing_status,
        nullif(trim(all_appointments), '') as all_appointments,
        
        -- =====================================================================
        -- FINANCIAL AID
        -- =====================================================================
        nullif(trim(financial_aid_award_years), '') as financial_aid_award_years,
        nullif(trim(financial__aid_record), '') as financial_aid_record,
        
        -- =====================================================================
        -- IDENTIFIERS
        -- =====================================================================
        referenceid_54572908 as reference_id_alt,  -- Alternate reference ID
        nullif(trim(other_ids), '') as other_ids,
        regexp_substr(other_ids, 'Slate Person ID/([0-9]+)', 1, 1, 'e', 1) as slate_person_id,
        nullif(trim(additional_names), '') as additional_names,
        
        -- =====================================================================
        -- ADMINISTRATIVE
        -- =====================================================================
        nullif(trim(institution), '') as institution,
        created_on_date,
        date_of_death,
        nullif(trim(official_transcript_note), '') as official_transcript_note,
        
        -- =====================================================================
        -- METADATA
        -- =====================================================================
        {{ add_source_metadata('workday', 'sta_student_wd') }}
        
    from source
    where referenceid_62792180 is not null  -- Must have student ID
)

select * from renamed