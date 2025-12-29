{{ 
    config(
        materialized='view',
        tags=['staging', 'jenzabar', 'admissions', 'recruiting']
    )
}}

with source as (
    select * from {{ source('jenzabar_cx_archive', 'adm_rec') }}
),

renamed as (
    select 
        -- Primary Keys (from adm_rec table)
        id as student_id,
        prog as program_code,
        
        -- Application Information
        primary_app as is_primary_application,
        case when reapp = 'Y' then true when reapp = 'N' then false else null end as is_reapplication,
        
        -- Program/Academic Plans
        subprog as subprogram_code,
        major as intended_major_code,
        major2 as intended_major_2_code,
        minor1 as intended_minor_1_code,
        minor2 as intended_minor_2_code,
        
        -- Certificates & Disciplines
        sta_cert1 as certificate_1_code,
        sta_cert2 as certificate_2_code,
        sta_preprof as pre_professional_code,
        sta_disc as undeclared_discipline_code,
        
        -- Enrollment Plans
        plan_enr_yr as planned_enrollment_year,
        plan_enr_sess as planned_enrollment_session_code,
        intend_hrs_enr as intended_credit_hours,
        
        -- Transfer Status
        case when trnsfr = 'Y' then true when trnsfr = 'N' then false else null end as is_transfer_student,
        
        -- Benefits
        case when vet_ben = 'Y' then true when vet_ben = 'N' then false else null end as receives_veteran_benefits,
        case when ss_ben = 'Y' then true when ss_ben = 'N' then false else null end as receives_social_security_benefits,
        
        -- Enrollment Status History
        prev_enrstat as previous_enrollment_status_code,
        cast(prev_enr_date as date) as previous_enrollment_status_date,
        
        -- Reference/Recruitment
        ref_source as reference_source_code,
        ref_id as reference_person_id,
        cast(ref_date as date) as reference_added_date,
        
        -- Contact Tracking - Resources (Outgoing)
        last_resrc as last_outgoing_contact_code,
        cast(last_resrc_date as date) as last_outgoing_contact_date,
        next_resrc as next_expected_outgoing_contact_code,
        cast(next_resrc_date as date) as next_expected_outgoing_contact_date,
        
        -- Contact Tracking - Responses (Incoming)
        last_resp as last_incoming_contact_code,
        cast(last_resp_date as date) as last_incoming_contact_date,
        next_resp as next_expected_incoming_contact_code,
        cast(next_resp_date as date) as next_expected_incoming_contact_date,
        
        -- Program Movement
        cast(move_date as date) as program_enrollment_created_date,
        
        -- Admissions Assessment
        predict as admission_prediction_gpa,
        predict_type as prediction_type_code,
        nonacad as admissions_rating_code,
        
        -- Waitlist
        rank as waitlist_rank,
        
        -- Financial
        parent_contr as parent_contribution_amount,
        
        -- Special Status
        matric_stat as matriculation_status_code,
        voc_ed as vocational_education_status_code,
        case when use_score = 'Y' then true when use_score = 'N' then false else null end as use_test_scores_for_admission,
        
        -- Interest Level
        stuint_wt as student_interest_weight,
        
        -- Administrative
        pref_name as preferred_name,
        case when international = 'Y' then true when international = 'N' then false else null end as is_international,
        case when jics_candidate = 'Y' then true when jics_candidate = 'N' then false else null end as is_jics_candidate,
        
        -- Audit Fields
        cast(upd_date as date) as last_updated_date,
        upd_uid as updated_by_user_id,
        
        -- Metadata
        {{ add_source_metadata('jenzabar_cx_archive', 'adm_rec') }}
        
    from source
    where id is not null
)

select * from renamed