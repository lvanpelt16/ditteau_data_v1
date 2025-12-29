{{ 
    config(
        materialized='view',
        tags=['staging', 'jenzabar', 'student_academic', 'terms']
    )
}}
with source as (
    select * from {{ source('jenzabar_cx_archive', 'stu_acad_rec') }}
),
renamed as (
    select 
        -- Primary Keys
        ID as student_id,
        PROG as program_code,
        SITE as site_code,
        SUBPROG as subprogram_code,
        SESS as term_code,
        YR as academic_year,
        
        -- Credit Hours - Current Term
        INTEND_HRS as intended_hours,
        WAIT_HRS as waitlist_hours,
        REG_HRS as registered_hours,
        REG_AU_HRS as registered_audit_hours,
        ATT_HRS as attempted_hours,
        EARN_HRS as earned_hours,
        PASS_HRS as passed_hours,
        QUAL_HRS as quality_hours,
        AU_HRS as audit_hours,
        WD_HRS as withdrawn_hours,
        
        -- Financial Aid Hours
        FA_REG_HRS as fa_registered_hours,
        FA_ATT_HRS as fa_attempted_hours,
        FA_EARN_HRS as fa_earned_hours,
        FA_PASS_HRS as fa_passed_hours,
        FA_QUAL_HRS as fa_quality_hours,
        FA_AU_HRS as fa_audit_hours,
        FA_ELIG_HRS as fa_eligible_hours,
        FA_R2T4_HRS as fa_r2t4_hours,
        
        -- GPA - Current Term
        QUAL_PTS as quality_points,
        GPA as term_gpa,
        FA_QUAL_PTS as fa_quality_points,
        FA_GPA as fa_gpa,
        
        -- Cumulative Hours
        CUM_ATT_HRS as cumulative_attempted_hours,
        CUM_EARN_HRS as cumulative_earned_hours,
        CUM_PASS_HRS as cumulative_passed_hours,
        CUM_QUAL_HRS as cumulative_quality_hours,
        CUM_AU_HRS as cumulative_audit_hours,
        
        -- Cumulative GPA
        CUM_QUAL_PTS as cumulative_quality_points,
        CUM_GPA as cumulative_gpa,
        
        -- Transfer Hours
        TRNSFR_ATT_HRS as transfer_attempted_hours,
        TRNSFR_EARN_HRS as transfer_earned_hours,
        TRNSFR_PASS_HRS as transfer_passed_hours,
        TRNSFR_QUAL_HRS as transfer_quality_hours,
        TRNSFR_AU_HRS as transfer_audit_hours,
        TRNSFR_QUAL_PTS as transfer_quality_points,
        TRNSFR_GPA as transfer_gpa,
        
        -- Residence Hours
        RES_ATT_HRS as residence_attempted_hours,
        RES_EARN_HRS as residence_earned_hours,
        RES_PASS_HRS as residence_passed_hours,
        RES_QUAL_HRS as residence_quality_hours,
        RES_AU_HRS as residence_audit_hours,
        RES_QUAL_PTS as residence_quality_points,
        RES_GPA as residence_gpa,
        
        -- Academic Standing
        ACST as academic_standing_code,  -- Note: Not ACAD_STAT!
        
        -- Classification
        CL as class_level_code,
        CL_END as end_class_level_code,
        CL_RANK as class_rank,
        CL_SIZE as class_size,
        
        -- Program Info
        MAJOR1 as major_1_code,
        MAJOR2 as major_2_code,
        DEGGRP as degree_group_code,
        GOAL as educational_goal_code,
        
        -- Status Codes
        GRD_STAT as grade_status_code,
        REG_STAT as registration_status_code,
        case when FIN_CLR = 'Y' then true when FIN_CLR = 'N' then false else null end as financially_cleared,
        case when STUBILL_CLR = 'Y' then true when STUBILL_CLR = 'N' then false else null end as student_billing_cleared,
        
        -- Withdrawal
        WD_CODE as withdrawal_code,
        cast(WD_DATE as date) as withdrawal_date,
        cast(LAST_ATTENDED_DATE as date) as last_attended_date,
        
        -- Dates
        cast(REG_UPD_DATE as date) as registration_update_date,
        cast(DETERMINATION_DATE as date) as determination_date,
        cast(FAELIG_DATE as timestamp) as fa_eligibility_date,
        cast(FA_R2T4_LDOA as date) as fa_r2t4_last_date_of_attendance,
        
        -- Flags
        case when CRS_FEES = 'Y' then true when CRS_FEES = 'N' then false else null end as has_course_fees,
        case when NON_PROG_CRS = 'Y' then true when NON_PROG_CRS = 'N' then false else null end as has_non_program_courses,
        case when RPT_IPEDS = 'Y' then true when RPT_IPEDS = 'N' then false else null end as report_to_ipeds,
        case when RESTART = 'Y' then true when RESTART = 'N' then false else null end as is_restart,
        case when R2T4 = 'Y' then true when R2T4 = 'N' then false else null end as has_r2t4,
        
        -- Additional
        NO_ADRP as number_add_drop,
        BLK_CODE as block_code,
        RND as round_number,
        IPEDS_HRS as ipeds_hours,
        SESS_ORD as session_order,
        RES_ST as residence_state,
        RET_INTENT as retention_intent,
        STA_CRSCNT as state_course_count,
        
        -- Transcript Counts
        OFCL_TRANS_NUM as official_transcript_number,
        OFCL_TRANS_CHG as official_transcript_charge,
        UNOFCL_TRANS_NUM as unofficial_transcript_number,
        UNOFCL_TRANS_CHG as unofficial_transcript_charge,
        
        -- State Reporting Flags
        case when APPRENT = 'Y' then true when APPRENT = 'N' then false else null end as is_apprentice,
        case when VOC_ED = 'Y' then true when VOC_ED = 'N' then false else null end as is_vocational_education,
        case when JTPA_STAT = 'Y' then true when JTPA_STAT = 'N' then false else null end as jtpa_status,
        case when GAIN_STAT = 'Y' then true when GAIN_STAT = 'N' then false else null end as gain_status,
        case when PBS_WAIVER = 'Y' then true when PBS_WAIVER = 'N' then false else null end as pbs_waiver,
        
        -- Metadata
        {{ add_source_metadata('jenzabar_cx_archive', 'stu_acad_rec') }}
        
    from source
    where ID is not null
        and SESS is not null
        and YR is not null
)
select * from renamed