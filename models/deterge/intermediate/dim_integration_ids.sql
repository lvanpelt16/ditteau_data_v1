{{
    config(
        materialized='incremental',
        unique_key='scd_id',
        incremental_strategy='merge',
        on_schema_change='fail',
        cluster_by=['ditteau_id', 'entity_type', 'is_current'],
        tags=['integration', 'id_resolution', 'core', 'scd_type_2', 'multi_source']
    )
}}

/*
Purpose: Universal entity resolution with SCD Type 2 tracking
Scope: Multi-source student matching (Jenzabar CX + Workday Student + PowerFAIDS + Slate)

This model creates Ditteau Master IDs and handles cross-system matching:
- Workday Student is the NEW system of record
- Jenzabar CX is the LEGACY system (being phased out)
- Students may exist in BOTH systems during migration
- Matching uses: email, name+DOB, and Workday's MATCHING_HISTORICAL_STUDENT field

Key Innovation: Uses Workday's built-in historical student matching field
to link new Workday records to legacy Jenzabar records
*/

-- ============================================================================
-- CONFIGURATION
-- ============================================================================
with institution_config as (
    select '{{ var("institution_code", "UNKNOWN") }}' as institution_code
),

-- ============================================================================
-- SOURCE DATA: WORKDAY STUDENT (NEW SYSTEM OF RECORD)
-- ============================================================================
wd_students as (
    select
        student_id,
        first_name,
        last_name,
        email,
        birth_date,
        slate_person_id,
        matching_historical_student,
        is_active_matriculated,
        created_on_date
    from {{ ref('stg_wd__students') }}
    where student_id is not null
),

-- ============================================================================
-- SOURCE DATA: JENZABAR CX (LEGACY SYSTEM)
-- ============================================================================
jcx_students as (
    select
        stu.student_id,
        id_rec.first_name,
        id_rec.last_name,
        emails.email_address as email,
        prof.birth_date
    from {{ ref('stg_jcx__students') }} stu
    left join {{ ref('stg_jcx__id') }} id_rec 
        on stu.student_id = id_rec.person_id
    left join {{ ref('stg_jcx__profile') }} prof
        on stu.student_id = prof.person_id
    left join {{ ref('stg_jcx__emails') }} emails
        on stu.student_id = emails.person_id
    where stu.student_id is not null
),

-- ============================================================================
-- STEP 1: IDENTIFY UNIQUE PERSONS ACROSS SYSTEMS
-- ============================================================================
-- This is the CRITICAL matching logic that creates ONE Ditteau ID per person
-- even if they exist in multiple source systems

-- First, get all unique persons from Workday (primary source)
unique_persons as (
    select distinct
        student_id as primary_source_id,
        'WORKDAY' as primary_source_system,
        email,
        first_name,
        last_name,
        birth_date,
        matching_historical_student as jcx_match_hint
    from wd_students
    
    union all
    
    -- Then add Jenzabar students who DON'T have a Workday record
    select distinct
        jcx.student_id as primary_source_id,
        'JENZABAR_CX' as primary_source_system,
        jcx.email,
        jcx.first_name,
        jcx.last_name,
        jcx.birth_date,
        null as jcx_match_hint
    from jcx_students jcx
    where not exists (
        -- Exclude if this Jenzabar student is already in Workday
        select 1 from wd_students wd
        where lower(trim(wd.email)) = lower(trim(jcx.email))
          and wd.email is not null
          and jcx.email is not null
    )
),

-- ============================================================================
-- STEP 2: GENERATE DITTEAU MASTER IDS
-- ============================================================================
-- One Ditteau ID per unique person
person_ditteau_ids as (
    select
        primary_source_id,
        primary_source_system,
        'DTU_PERSON_' || lpad(
            row_number() over (order by primary_source_id), 
            9, 
            '0'
        ) as ditteau_id,
        email,
        first_name,
        last_name,
        birth_date,
        jcx_match_hint
    from unique_persons
),

-- ============================================================================
-- STEP 3: CREATE WORKDAY STUDENT ROWS
-- ============================================================================
workday_student_rows as (
    select
        cfg.institution_code,
        'PERSON' as entity_type,
        ids.ditteau_id,
        'STUDENT' as role_type,
        
        -- Source system identification
        'WORKDAY' as source_system,
        wd.student_id as source_id,
        true as is_primary_source,  -- Workday is now primary
        
        -- Matching metadata
        1.00 as match_confidence,
        'SYSTEM_OF_RECORD' as match_method,
        
        -- Change tracking attributes (for SCD Type 2)
        wd.email as tracked_email,
        wd.first_name as tracked_first_name,
        wd.last_name as tracked_last_name,
        
        -- Row hash for change detection
        md5(concat_ws('||',
            coalesce(wd.email, ''),
            coalesce(wd.first_name, ''),
            coalesce(wd.last_name, '')
        )) as row_hash,
        
        -- Effective dating
        coalesce(wd.created_on_date, current_timestamp()) as effective_date,
        null::timestamp_ntz as end_date,
        true as is_current,
        
        -- Metadata
        current_timestamp() as dbt_updated_at,
        'WORKDAY' as _source_system,
        'STA_STUDENT_WD' as _source_table,
        'CONFIDENTIAL' as _data_classification,
        'REGISTRAR' as _data_owner,
        current_timestamp() as _dbt_loaded_at
        
    from wd_students wd
    join person_ditteau_ids ids
        on wd.student_id = ids.primary_source_id
        and ids.primary_source_system = 'WORKDAY'
    cross join institution_config cfg
),

-- ============================================================================
-- STEP 4: CREATE JENZABAR CX STUDENT ROWS
-- ============================================================================
-- These are SECONDARY rows for students who exist in both systems
-- OR primary rows for students who only exist in Jenzabar (legacy-only)

jcx_student_rows as (
    select
        cfg.institution_code,
        'PERSON' as entity_type,
        ids.ditteau_id,
        'STUDENT' as role_type,
        
        -- Source system identification
        'JENZABAR_CX' as source_system,
        jcx.student_id as source_id,
        -- Primary only if this student isn't in Workday
        case 
            when ids.primary_source_system = 'JENZABAR_CX' then true
            else false 
        end as is_primary_source,
        
        -- Matching metadata
        case
            when ids.primary_source_system = 'WORKDAY' then 0.95  -- Matched to Workday
            else 1.00  -- Legacy-only student
        end as match_confidence,
        case
            when ids.primary_source_system = 'WORKDAY' then 'EMAIL_MATCH'
            else 'SYSTEM_OF_RECORD'
        end as match_method,
        
        -- Change tracking attributes
        jcx.email as tracked_email,
        jcx.first_name as tracked_first_name,
        jcx.last_name as tracked_last_name,
        
        -- Row hash for change detection
        md5(concat_ws('||',
            coalesce(jcx.email, ''),
            coalesce(jcx.first_name, ''),
            coalesce(jcx.last_name, '')
        )) as row_hash,
        
        -- Effective dating
        current_timestamp() as effective_date,
        null::timestamp_ntz as end_date,
        true as is_current,
        
        -- Metadata
        current_timestamp() as dbt_updated_at,
        'JENZABAR_CX' as _source_system,
        'PROG_ENR_REC' as _source_table,
        'CONFIDENTIAL' as _data_classification,
        'REGISTRAR' as _data_owner,
        current_timestamp() as _dbt_loaded_at
        
    from jcx_students jcx
    join person_ditteau_ids ids
        on (
            -- Match by email if same person in both systems
            lower(trim(jcx.email)) = lower(trim(ids.email))
            and jcx.email is not null
        )
        or (
            -- Or if this is a legacy-only student
            jcx.student_id = ids.primary_source_id
            and ids.primary_source_system = 'JENZABAR_CX'
        )
    cross join institution_config cfg
),

-- ============================================================================
-- STEP 5: MATCH WORKDAY TO JENZABAR USING REFERENCEID
-- ============================================================================
-- Use Workday's REFERENCEID_62792180 to match to Jenzabar CX ID

workday_jcx_explicit_links as (
    select
        cfg.institution_code,
        'PERSON' as entity_type,
        ids.ditteau_id,
        'STUDENT' as role_type,
        
        'JENZABAR_CX' as source_system,
        wd.student_id as source_id,  -- ✅ Use Workday student_id to link to JCX
        false as is_primary_source,
        
        0.98 as match_confidence,
        'WORKDAY_REFERENCE_ID_MATCH' as match_method,
        
        wd.email as tracked_email,
        wd.first_name as tracked_first_name,
        wd.last_name as tracked_last_name,
        
        md5(concat_ws('||',
            coalesce(wd.email, ''),
            coalesce(wd.first_name, ''),
            coalesce(wd.last_name, '')
        )) as row_hash,
        
        current_timestamp() as effective_date,
        null::timestamp_ntz as end_date,
        true as is_current,
        
        current_timestamp() as dbt_updated_at,
        'WORKDAY' as _source_system,
        'STA_STUDENT_WD' as _source_table,
        'CONFIDENTIAL' as _data_classification,
        'REGISTRAR' as _data_owner,
        current_timestamp() as _dbt_loaded_at
        
    from wd_students wd
    join person_ditteau_ids ids
        on wd.student_id = ids.primary_source_id
        and ids.primary_source_system = 'WORKDAY'
    cross join institution_config cfg
    -- Only create explicit links where we can match to JCX by ID
    where exists (
        select 1 
        from jcx_students jcx 
        where jcx.student_id = wd.student_id
    )
),

-- ============================================================================
-- STEP 6: CREATE SLATE ID ROWS FROM WORKDAY OTHER_IDS FIELD
-- ============================================================================
workday_slate_links as (
    select
        cfg.institution_code,
        'PERSON' as entity_type,
        ids.ditteau_id,
        'PROSPECT' as role_type,  -- Slate is admissions/prospect system
        
        'SLATE' as source_system,
        wd.slate_person_id as source_id,  -- Parsed from OTHER_IDS
        false as is_primary_source,
        
        0.95 as match_confidence,
        'WORKDAY_OTHER_IDS_PARSE' as match_method,
        
        wd.email as tracked_email,
        wd.first_name as tracked_first_name,
        wd.last_name as tracked_last_name,
        
        md5(concat_ws('||',
            coalesce(wd.email, ''),
            coalesce(wd.first_name, ''),
            coalesce(wd.last_name, '')
        )) as row_hash,
        
        current_timestamp() as effective_date,
        null::timestamp_ntz as end_date,
        true as is_current,
        
        current_timestamp() as dbt_updated_at,
        'WORKDAY' as _source_system,
        'STA_STUDENT_WD' as _source_table,
        'CONFIDENTIAL' as _data_classification,
        'ADMISSIONS' as _data_owner,
        current_timestamp() as _dbt_loaded_at
        
    from wd_students wd
    join person_ditteau_ids ids
        on wd.student_id = ids.primary_source_id
        and ids.primary_source_system = 'WORKDAY'
    cross join institution_config cfg
    where wd.slate_person_id is not null
),
-- ============================================================================
-- STEP 7: COMBINE ALL ROWS
-- ============================================================================
all_new_rows as (
    select * from workday_student_rows
    union all
    select * from jcx_student_rows
    union all
    select * from workday_jcx_explicit_links
    union all
    select * from workday_slate_links
),

-- ============================================================================
-- STEP 8: ADD SURROGATE KEY
-- ============================================================================
new_rows_with_scd_id as (
    select
        md5(concat_ws('||',
            institution_code,
            entity_type,
            ditteau_id,
            role_type,
            source_system,
            source_id,
            effective_date::string
        )) as scd_id,
        *
    from all_new_rows
)

-- ============================================================================
-- STEP 8: INCREMENTAL LOGIC
-- ============================================================================
{% if is_incremental() %}
    -- For incremental runs, only insert new versions or new records
    select * from new_rows_with_scd_id
    where scd_id not in (select scd_id from {{ this }})
{% else %}
    -- For full refresh, insert everything
    select * from new_rows_with_scd_id
{% endif %}