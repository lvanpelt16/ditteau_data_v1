{% docs deterge_overview %}

# Deterge Layer (Silver Schema)

## Overview

The **Deterge layer** represents the **Silver tier** of the Ditteau Data architecture. This layer is the heart of dbt transformation work, where raw data from the Deposit layer is cleaned, conformed, and integrated into a consistent analytical structure. The name "Deterge" (meaning "to cleanse") reflects its primary purpose: transforming messy source data into trustworthy, analytics-ready datasets.

## Purpose

The Deterge layer serves these critical functions:

- **Data Cleansing**: Fix data quality issues, standardize formats, handle nulls and anomalies
- **Conformance**: Apply consistent naming, typing, and business rules across all source systems
- **Integration**: Join and harmonize data from multiple sources into unified views
- **Business Logic**: Implement institutional business rules and calculations
- **Deduplication**: Resolve duplicate records and establish "golden records"
- **Type 2 Dimensions**: Track historical changes for slowly changing dimensions

## Architectural Position

```
┌─────────────────┐
│ DEPOSIT         │ ← Raw data loaded externally
│ (Bronze)        │   • Source system tables/views
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│ DETERGE         │ ← You are here (Silver schema)
│ (Silver)        │   • staging/ - Source conformance
└────────┬────────┘   • intermediate/ - Integration logic
         │            • dim_integration_ids - ID resolution
         ↓
┌─────────────────┐
│ DISTRIBUTE      │ ← Analytics-ready star schema
│ (Gold)          │   • Dimensions and facts
└─────────────────┘
```

## dbt Project Structure

### Deterge Folder Organization

```
models/
└── deterge/
    ├── _deterge_overview.md           # This file
    ├── staging/                       # Source system conformance
    │   ├── _jcx_sources.yml          # Jenzabar source definitions
    │   ├── _pf_sources.yml           # PowerFaids source definitions
    │   ├── stg_jcx__students.sql     # Jenzabar students staging
    │   ├── stg_jcx__courses.sql      # Jenzabar courses staging
    │   ├── stg_pf__awards.sql        # PowerFaids awards staging
    │   └── ...
    ├── intermediate/                  # Cross-source integration
    │   ├── int_students.sql          # Integrated student view
    │   ├── int_enrollments.sql       # Integrated enrollment view
    │   ├── int_awards.sql            # Integrated awards view
    │   └── ...
    └── dim_integration_ids.sql        # Master ID resolution table
```

## Two-Stage Transformation Pattern

The Deterge layer uses a **two-stage pattern** for clarity and maintainability:

### Stage 1: Staging Models (stg_)
**Purpose**: Source-specific conformance and light cleaning

**Characteristics**:
- One staging model per source table
- Minimal transformation logic
- Rename columns to institutional standards
- Cast to appropriate data types
- Add metadata using macros
- **No joins** to other sources

**Example**: `stg_jcx__students.sql`
```sql
{{
    config(
        materialized='view',
        schema='deterge'
    )
}}

select
    -- Primary key (renamed)
    id_rec as student_id,
    
    -- Demographic fields (standardized names)
    first_name,
    last_name,
    case 
        when email = '' then null 
        else lower(email) 
    end as email,
    
    -- Dates (cast to proper type)
    to_date(birth_dte, 'YYYY-MM-DD') as birth_date,
    to_date(admit_dte, 'YYYY-MM-DD') as admit_date,
    
    -- Codes (standardized)
    upper(trim(sts_cod)) as enrollment_status_code,
    
    -- Metadata
    {{ add_metadata_from_source(source('jcx', 'students')) }}
    
from {{ source('jcx', 'students') }}
where id_rec is not null  -- Basic quality filter
```

### Stage 2: Intermediate Models (int_)
**Purpose**: Cross-source integration and business logic

**Characteristics**:
- Combine data from multiple staging models
- Resolve entity IDs across systems (using dim_integration_ids)
- Apply complex business rules
- Deduplicate and establish golden records
- Create derived/calculated fields
- Build dimensional history (Type 2 SCD)

**Example**: `int_students.sql`
```sql
{{
    config(
        materialized='table',
        schema='deterge'
    )
}}

with jcx_students as (
    select * from {{ ref('stg_jcx__students') }}
),

pf_students as (
    select * from {{ ref('stg_pf__students') }}
),

id_mapping as (
    select * from {{ ref('dim_integration_ids') }}
    where entity_type = 'STUDENT'
),

integrated as (
    select
        -- Use master student_id from integration table
        coalesce(map.master_student_id, jcx.student_id) as student_id,
        
        -- Prefer JCX data (system of record for demographics)
        jcx.first_name,
        jcx.last_name,
        jcx.email,
        jcx.birth_date,
        
        -- Enrich with PowerFaids data where available
        pf.fafsa_received_date,
        pf.efc_amount,
        
        -- Business logic: calculate full name
        trim(jcx.first_name || ' ' || jcx.last_name) as full_name,
        
        -- Business logic: determine adult status
        case 
            when datediff(year, jcx.birth_date, current_date()) >= 24 
            then true 
            else false 
        end as is_independent_student,
        
        -- Metadata (prefer most recent)
        greatest(jcx.meta_ingest_time, pf.meta_ingest_time) as meta_ingest_time,
        'INTEGRATED' as meta_source_system
        
    from jcx_students jcx
    left join id_mapping map
        on jcx.student_id = map.source_student_id
        and map.source_system = 'JENZABAR'
    left join pf_students pf
        on coalesce(map.master_student_id, jcx.student_id) = pf.student_id
)

select * from integrated
```

## Master ID Resolution: dim_integration_ids

### Purpose
The `dim_integration_ids` table is the cornerstone of cross-system integration. It maps source system identifiers to master institutional identifiers, enabling accurate joins across systems that may use different ID schemes.

### Structure
```sql
-- models/deterge/dim_integration_ids.sql
{{
    config(
        materialized='table',
        schema='deterge'
    )
}}

select
    entity_type,              -- 'STUDENT', 'COURSE', 'INSTRUCTOR', etc.
    master_id,                -- Institutional master ID
    source_system,            -- 'JENZABAR', 'POWERFAIDS', etc.
    source_id,                -- ID in source system
    effective_date,           -- When mapping became active
    end_date,                 -- When mapping ended (null if current)
    is_current,               -- Boolean flag for active mappings
    match_confidence,         -- 'HIGH', 'MEDIUM', 'LOW'
    match_method,             -- 'EXACT', 'FUZZY', 'MANUAL'
    meta_ingest_time
from {{ ref('int_id_resolution') }}
```

### Usage Pattern
```sql
-- Join to get master ID
from {{ ref('stg_jcx__students') }} jcx
left join {{ ref('dim_integration_ids') }} ids
    on jcx.student_id = ids.source_id
    and ids.source_system = 'JENZABAR'
    and ids.entity_type = 'STUDENT'
    and ids.is_current = true
```

## Naming Conventions

### Staging Models
**Pattern**: `stg_<source_system>__<entity>.sql`

- `stg_` prefix indicates staging model
- Source system abbreviation (jcx, pf, csv)
- Double underscore separator
- Entity name (plural preferred)

**Examples**:
- `stg_jcx__students.sql`
- `stg_jcx__courses.sql`
- `stg_pf__awards.sql`
- `stg_csv__test_scores.sql`

### Intermediate Models
**Pattern**: `int_<entity>.sql`

- `int_` prefix indicates intermediate model
- Entity name describes integrated concept
- No source system in name (integrates across sources)

**Examples**:
- `int_students.sql`
- `int_enrollments.sql`
- `int_financial_aid.sql`
- `int_academic_history.sql`

### Column Naming Standards
```sql
-- Good column names (consistent across all models)
student_id              -- Clear entity reference
first_name              -- Descriptive, snake_case
birth_date              -- Use _date suffix for dates
enrollment_status_code  -- Use _code suffix for codes
is_active               -- Boolean prefix with is_/has_
total_credits_earned    -- Calculated fields clearly named

-- Avoid
ID                      -- Too generic
FName                   -- Abbreviations unclear
DOB                     -- Acronyms without context
Status                  -- Ambiguous
```

## Data Quality and Testing

### Staging Model Tests
Focus on **technical quality**:
```yaml
# models/deterge/staging/_jcx_models.yml
models:
  - name: stg_jcx__students
    columns:
      - name: student_id
        tests:
          - not_null
          - unique
      
      - name: email
        tests:
          - not_null
          - dbt_utils.email_format  # Custom test
```

### Intermediate Model Tests
Focus on **business logic**:
```yaml
# models/deterge/intermediate/_int_models.yml
models:
  - name: int_students
    tests:
      - dbt_utils.recency:
          datepart: day
          field: meta_ingest_time
          interval: 2
    
    columns:
      - name: student_id
        tests:
          - not_null
          - unique
          - relationships:
              to: ref('dim_integration_ids')
              field: master_id
              where: "entity_type = 'STUDENT'"
      
      - name: is_independent_student
        tests:
          - accepted_values:
              values: [true, false]
```

## Metadata Propagation

### Using add_source_metadata()
In staging models, use the macro to automatically include metadata:

```sql
select
    student_id,
    first_name,
    last_name,
    {{ add_source_metadata('jcx', 'students') }}
from {{ source('jcx', 'students') }}
```

This generates:
```sql
'JENZABAR_CX' as _source_system,
'STUDENTS' as _source_table,
'INTERNAL' as _data_classification,
'REGISTRAR' as _data_owner,
'FULL_LOAD' as _ingest_type,
current_timestamp() as _dbt_loaded_at
```

### Metadata in Intermediate Models
For intermediate models combining multiple sources:
```sql
select
    student_id,
    first_name,
    last_name,
    -- Use greatest() to get most recent load time
    greatest(jcx._dbt_loaded_at, pf._dbt_loaded_at) as _dbt_loaded_at,
    'INTEGRATED' as _source_system,
    'MULTIPLE' as _source_table
from jcx_students jcx
left join pf_students pf on jcx.student_id = pf.student_id
```

## Common Transformation Patterns

### 1. Standardizing Null Representations
```sql
-- Source systems may use '', 'NULL', '0', etc. for nulls
case 
    when trim(field) in ('', 'NULL', 'N/A', '0') then null
    else field
end as field
```

### 2. Code Standardization
```sql
-- Standardize codes to uppercase, trimmed
upper(trim(status_code)) as status_code
```

### 3. Date Parsing
```sql
-- Handle various date formats from sources
case 
    when date_field ~ '^\d{4}-\d{2}-\d{2}$' 
        then to_date(date_field, 'YYYY-MM-DD')
    when date_field ~ '^\d{2}/\d{2}/\d{4}$'
        then to_date(date_field, 'MM/DD/YYYY')
    else null
end as parsed_date
```

### 4. Name Parsing
```sql
-- Split full names consistently
split_part(full_name, ',', 1) as last_name,
trim(split_part(full_name, ',', 2)) as first_name
```

### 5. Deduplication
```sql
-- Use row_number() to deduplicate
qualify row_number() over (
    partition by student_id 
    order by meta_ingest_time desc, source_priority desc
) = 1
```

### 6. Slowly Changing Dimensions (Type 2)
```sql
-- Track historical changes
select
    student_id,
    enrollment_status,
    meta_ingest_time as effective_date,
    lead(meta_ingest_time) over (
        partition by student_id 
        order by meta_ingest_time
    ) as end_date,
    case 
        when lead(meta_ingest_time) over (
            partition by student_id 
            order by meta_ingest_time
        ) is null then true
        else false
    end as is_current
from student_status_changes
```

## Best Practices

### ✅ DO

**Staging Models**:
- Keep transformations minimal and source-specific
- Rename columns to institutional standards
- Document every column with business meaning
- Include metadata columns via macro
- Test for primary key uniqueness
- Use views for efficiency (unless very large)

**Intermediate Models**:
- Apply complex business logic here
- Use CTEs for readability
- Comment complex transformations
- Materialize as tables for performance
- Test business rule validity
- Create one model per major entity

**General**:
- Use consistent naming conventions
- Document assumptions and decisions
- Add inline comments for complex logic
- Version control all changes
- Test thoroughly before promoting

### ❌ DON'T

**Staging Models**:
- Join to other sources
- Apply complex business rules
- Filter records (except obvious invalids)
- Create derived metrics
- Hardcode values (use seed files instead)

**Intermediate Models**:
- Replicate staging transformations
- Create overly complex joins
- Mix too many concerns in one model
- Skip documentation
- Ignore data quality issues

**General**:
- Use SELECT * in final models
- Create circular dependencies
- Hardcode dates or IDs
- Skip testing
- Leave TODO comments in production code

## Incremental Models in Deterge

For large tables, consider incremental materialization:

```sql
{{
    config(
        materialized='incremental',
        unique_key='student_id',
        on_schema_change='append_new_columns'
    )
}}

select
    student_id,
    first_name,
    last_name,
    meta_ingest_time
from {{ ref('stg_jcx__students') }}

{% if is_incremental() %}
    -- Only process new/updated records
    where meta_ingest_time > (select max(meta_ingest_time) from {{ this }})
{% endif %}
```

## Documentation Standards

### Model-Level Documentation
```sql
-- At the top of each .sql file
/*
Purpose: Staging model for Jenzabar student demographic data
Source: DEPOSIT.JCX_STUDENTS (Jenzabar CX)
Grain: One row per student
Key: student_id
Updates: Daily full refresh via nightly ETL
Owner: Institutional Research
*/
```

### YAML Documentation
```yaml
models:
  - name: stg_jcx__students
    description: |
      Staging model for student demographic information from Jenzabar CX.
      Includes basic cleaning and standardization of names, dates, and codes.
      Does not include enrollment or academic history (see other staging models).
    
    columns:
      - name: student_id
        description: Unique student identifier (Jenzabar ID_REC)
        tests:
          - not_null
          - unique
      
      - name: first_name
        description: Student's legal first name
      
      - name: birth_date
        description: Student's date of birth (YYYY-MM-DD format)
```

## Performance Considerations

### Materialization Strategy
```yaml
# dbt_project.yml
models:
  ditteau_data:
    deterge:
      staging:
        +materialized: view      # Staging is lightweight
      intermediate:
        +materialized: table     # Integration logic benefits from materialization
```

### Warehouse Sizing
```sql
-- For large transformations, use larger warehouse
{{
    config(
        materialized='table',
        snowflake_warehouse='TRANSFORM_LARGE'
    )
}}
```

### Clustering
```sql
-- For large tables with common filter patterns
{{
    config(
        materialized='table',
        cluster_by=['student_id', 'term_code']
    )
}}
```

## Governance and Security

### Access Control
- **Data Engineers**: Full read/write to Deterge schema
- **Analysts**: Read-only access to Deterge
- **End Users**: No direct access (use Distribute layer)

### Data Classification
Deterge inherits classifications from Deposit but may refine them:
```yaml
meta:
  contains_pii: true
  sensitivity_level: CONFIDENTIAL
  compliance_type: FERPA
```

### No Masking at Deterge
Data masking is applied in the Distribute layer. Deterge contains unmasked data for transformation purposes.

## Integration with Distribute Layer

The Distribute layer consumes Deterge intermediate models to create analytics-ready dimensional models:

```
Deterge (Silver)              Distribute (Gold)
─────────────────────────     ──────────────────────
int_students             →    dim_student
int_enrollments          →    fact_enrollment
int_courses              →    dim_course
int_financial_aid        →    fact_financial_aid
dim_integration_ids      →    Used for joins, not exposed
```

## Conclusion

The Deterge layer is where raw data becomes trustworthy analytical data. Through its two-stage pattern (staging → intermediate), it cleanses source-specific data and integrates across systems to create a consistent, high-quality foundation for analytics. Proper implementation of Deterge principles ensures that downstream consumers in the Distribute layer can focus on analysis rather than data wrangling.

**Key Takeaways**:
- Staging models conform individual sources
- Intermediate models integrate across sources
- dim_integration_ids enables cross-system joins
- Metadata propagates through transformations
- Testing ensures data quality
- Documentation makes models maintainable

{% enddocs %}