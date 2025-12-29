{% docs distribute_overview %}

# Distribute Layer (Gold Schema)

## Overview

The **Distribute layer** represents the **Gold tier** of the Ditteau Data architecture. This layer contains analytics-ready dimensional models designed for direct consumption by business intelligence tools, reports, and end users. The name "Distribute" reflects its purpose: distributing clean, governed, and performant data models to stakeholders across the institution.

## Purpose

The Distribute layer serves these critical functions:

- **Analytics-Ready Models**: Provide pre-joined, denormalized views optimized for reporting and analysis
- **Dimensional Modeling**: Implement star schema patterns (facts and dimensions) for intuitive querying
- **Governed Access**: Apply masking policies, row-level security, and data classification
- **Performance Optimization**: Pre-aggregate and materialize for fast query response times
- **Business-Friendly Naming**: Use clear, institutional terminology that stakeholders understand
- **Single Source of Truth**: Establish authoritative datasets for enterprise-wide consistency

## Architectural Position

```
┌─────────────────┐
│ DEPOSIT         │ ← Raw data (Bronze)
│ (Bronze)        │
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│ DETERGE         │ ← Cleaned and integrated (Silver)
│ (Silver)        │   • Staging models
└────────┬────────┘   • Intermediate models
         │
         ↓
┌─────────────────┐
│ DISTRIBUTE      │ ← You are here (Gold schema)
│ (Gold)          │   • Dimensional models (dim_, fact_)
└─────────────────┘   • Governed access
         │            • Optimized for queries
         ↓
┌─────────────────┐
│ BI TOOLS        │ ← Tableau, PowerBI, Looker, etc.
│ & END USERS     │
└─────────────────┘
```

## dbt Project Structure

### Distribute Folder Organization

```
models/
└── distribute/
    ├── _distribute_overview.md        # This file
    ├── dimensions/                    # Dimension tables
    │   ├── dim_student.sql           # Student dimension
    │   ├── dim_course.sql            # Course dimension
    │   ├── dim_instructor.sql        # Instructor dimension
    │   ├── dim_term.sql              # Term/period dimension
    │   ├── dim_date.sql              # Date dimension
    │   └── ...
    ├── facts/                         # Fact tables
    │   ├── fact_enrollment.sql       # Enrollment transactions
    │   ├── fact_financial_aid.sql    # Financial aid awards
    │   ├── fact_grade.sql            # Course grades
    │   ├── fact_application.sql      # Admissions applications
    │   └── ...
    └── marts/                         # Business-specific data marts
        ├── enrollment/               # Enrollment analytics
        ├── finance/                  # Financial analytics
        ├── retention/                # Student success
        └── ...
```

## Dimensional Modeling Principles

### Star Schema Pattern
The Distribute layer implements **star schema** designs:

```
         ┌──────────────┐
         │ dim_student  │
         └──────┬───────┘
                │
         ┌──────────────┐
         │ dim_course   │
         └──────┬───────┘
                │
    ┌───────────┴───────────┐
    │  fact_enrollment      │  ← Grain: One row per student-course-term
    │  - student_key         │
    │  - course_key          │
    │  - term_key            │
    │  - enrollment_date     │
    │  - credits             │
    │  - grade               │
    └───────────┬───────────┘
                │
         ┌──────────────┐
         │ dim_term     │
         └──────┬───────┘
                │
         ┌──────────────┐
         │ dim_date     │
         └──────────────┘
```

### Dimension Tables (dim_)
**Purpose**: Descriptive business entities

**Characteristics**:
- Contain descriptive attributes about business entities
- Include surrogate keys (e.g., `student_key`) for stable relationships
- Include natural keys (e.g., `student_id`) for business user reference
- Support Slowly Changing Dimensions (SCD Type 2) where needed
- Denormalized for query simplicity

**Example**: `dim_student.sql`
```sql
{{
    config(
        materialized='table',
        schema='distribute'
    )
}}

select
    -- Surrogate key (auto-incrementing)
    {{ dbt_utils.generate_surrogate_key(['student_id', 'effective_date']) }} as student_key,
    
    -- Natural key (business identifier)
    student_id,
    
    -- Descriptive attributes
    first_name,
    last_name,
    full_name,
    email,
    birth_date,
    age,
    
    -- Categorical attributes
    gender,
    ethnicity,
    state_of_residence,
    country_of_residence,
    
    -- Status flags
    is_international,
    is_first_generation,
    is_independent_student,
    current_enrollment_status,
    
    -- SCD Type 2 tracking
    effective_date,
    end_date,
    is_current,
    
    -- Metadata
    meta_ingest_time,
    meta_source_system
    
from {{ ref('int_students') }}
where is_current = true  -- Or include full history for Type 2
```

### Fact Tables (fact_)
**Purpose**: Measure business processes/events

**Characteristics**:
- Contain numeric measures and metrics
- Reference dimensions via foreign keys (surrogate keys)
- Define clear grain (one row = one business event)
- Optimized for aggregation
- Include transaction dates/timestamps

**Example**: `fact_enrollment.sql`
```sql
{{
    config(
        materialized='incremental',
        unique_key='enrollment_key',
        schema='distribute'
    )
}}

select
    -- Surrogate key for this fact
    {{ dbt_utils.generate_surrogate_key(['student_id', 'course_id', 'term_code']) }} as enrollment_key,
    
    -- Foreign keys to dimensions
    {{ dbt_utils.generate_surrogate_key(['student_id']) }} as student_key,
    {{ dbt_utils.generate_surrogate_key(['course_id']) }} as course_key,
    {{ dbt_utils.generate_surrogate_key(['term_code']) }} as term_key,
    {{ dbt_utils.generate_surrogate_key(['enrollment_date']) }} as enrollment_date_key,
    
    -- Degenerate dimensions (IDs kept in fact)
    section_number,
    
    -- Numeric measures
    credits_attempted,
    credits_earned,
    grade_points,
    
    -- Status indicators
    enrollment_status,
    grade_code,
    
    -- Dates
    enrollment_date,
    drop_date,
    completion_date,
    
    -- Metadata
    meta_ingest_time,
    meta_source_system
    
from {{ ref('int_enrollments') }}

{% if is_incremental() %}
    where meta_ingest_time > (select max(meta_ingest_time) from {{ this }})
{% endif %}
```

## Naming Conventions

### Dimension Tables
**Pattern**: `dim_<entity>.sql`

**Examples**:
- `dim_student.sql` - Student master dimension
- `dim_course.sql` - Course catalog dimension
- `dim_instructor.sql` - Faculty/instructor dimension
- `dim_term.sql` - Academic term dimension
- `dim_date.sql` - Date dimension (calendar)
- `dim_program.sql` - Academic program dimension

### Fact Tables
**Pattern**: `fact_<business_process>.sql`

**Examples**:
- `fact_enrollment.sql` - Course enrollments
- `fact_financial_aid.sql` - Financial aid awards
- `fact_grade.sql` - Course grades
- `fact_application.sql` - Admissions applications
- `fact_payment.sql` - Student payments
- `fact_attendance.sql` - Class attendance

### Data Marts
**Pattern**: `mart_<business_area>_<purpose>.sql`

**Examples**:
- `mart_enrollment_summary.sql` - Pre-aggregated enrollment metrics
- `mart_retention_cohort.sql` - Cohort retention analysis
- `mart_finance_revenue.sql` - Revenue reporting
- `mart_ir_ipeds.sql` - IPEDS reporting dataset

### Column Naming Standards
```sql
-- Primary/Surrogate Keys
student_key              -- Surrogate key (dimension primary key)
enrollment_key           -- Surrogate key (fact primary key)

-- Foreign Keys
student_key              -- Foreign key to dim_student
course_key               -- Foreign key to dim_course
term_key                 -- Foreign key to dim_term

-- Business Keys
student_id               -- Natural/business identifier
course_id                -- Natural/business identifier

-- Measures
credits_attempted        -- Numeric measure
credits_earned           -- Numeric measure
total_amount             -- Monetary measure

-- Dates
enrollment_date          -- Transaction date
enrollment_date_key      -- Foreign key to dim_date
```

## Data Governance and Security

### Masking Policies
Apply masking to sensitive data in Distribute:

```sql
-- Example: Masking SSN
{{
    config(
        materialized='table',
        schema='distribute',
        tags=['contains_pii']
    )
}}

select
    student_key,
    student_id,
    first_name,
    last_name,
    
    -- Apply masking to SSN column
    case 
        when current_role() in ('ROLE_ADMIN', 'ROLE_REGISTRAR')
        then ssn
        else 'XXX-XX-' || right(ssn, 4)
    end as ssn,
    
    -- Other fields...
    
from {{ ref('int_students') }}
```

### Row-Level Security
Implement row access policies for departmental data:

```sql
-- Example: Department-level access
{{
    config(
        materialized='table',
        schema='distribute',
        tags=['row_level_security']
    )
}}

select
    *,
    -- Add department for RLS policy
    department_code
    
from {{ ref('int_courses') }}

-- Apply RLS policy in post-hook:
-- Only show records where user has access to department
```

### Data Classification Tags
Apply Snowflake tags to tables and columns:

```yaml
# models/distribute/dimensions/_dim_models.yml
models:
  - name: dim_student
    meta:
      contains_pii: true
      sensitivity_level: CONFIDENTIAL
      compliance_type: FERPA
      data_owner: REGISTRAR
    
    columns:
      - name: ssn
        meta:
          contains_pii: true
          pii_type: SSN
      
      - name: email
        meta:
          contains_pii: true
          pii_type: EMAIL
```

## Data Mart Patterns

### Aggregated Marts
Pre-aggregate data for common queries:

```sql
-- mart_enrollment_summary.sql
{{
    config(
        materialized='table',
        schema='distribute'
    )
}}

select
    term_code,
    term_name,
    college_name,
    department_name,
    
    -- Aggregated metrics
    count(distinct student_id) as student_count,
    count(distinct course_id) as course_count,
    sum(credits_attempted) as total_credits_attempted,
    sum(credits_earned) as total_credits_earned,
    avg(gpa) as average_gpa,
    
    -- Success rates
    sum(case when grade_code in ('A', 'B', 'C') then 1 else 0 end) * 100.0 
        / nullif(count(*), 0) as success_rate_pct
    
from {{ ref('fact_enrollment') }} enr
join {{ ref('dim_term') }} trm on enr.term_key = trm.term_key
join {{ ref('dim_course') }} crs on enr.course_key = crs.course_key
join {{ ref('dim_student') }} std on enr.student_key = std.student_key

group by 1, 2, 3, 4
```

### Cohort Analysis Marts
Enable longitudinal analysis:

```sql
-- mart_retention_cohort.sql
{{
    config(
        materialized='table',
        schema='distribute'
    )
}}

with first_term as (
    select
        student_id,
        min(term_code) as cohort_term,
        min(term_year) as cohort_year
    from {{ ref('fact_enrollment') }}
    group by 1
),

term_enrollment as (
    select
        f.student_id,
        f.cohort_term,
        e.term_code,
        e.term_sequence,
        e.is_enrolled
    from first_term f
    join {{ ref('fact_enrollment') }} e on f.student_id = e.student_id
)

select
    cohort_term,
    cohort_year,
    term_sequence,
    count(distinct student_id) as students_enrolled,
    count(distinct student_id) * 100.0 / 
        max(count(distinct student_id)) over (partition by cohort_term) as retention_rate_pct
    
from term_enrollment
where is_enrolled = true

group by 1, 2, 3
order by 1, 3
```

## Performance Optimization

### Materialization Strategy
```yaml
# dbt_project.yml
models:
  ditteau_data:
    distribute:
      dimensions:
        +materialized: table        # Dimensions as tables
        +tags: ['dimension']
      facts:
        +materialized: incremental  # Facts incremental where possible
        +tags: ['fact']
      marts:
        +materialized: table        # Marts as tables
        +tags: ['mart']
```

### Clustering Keys
Optimize for common query patterns:

```sql
{{
    config(
        materialized='table',
        cluster_by=['term_code', 'department_code'],
        tags=['fact']
    )
}}
```

### Incremental Loading
Use incremental patterns for large fact tables:

```sql
{{
    config(
        materialized='incremental',
        unique_key='enrollment_key',
        on_schema_change='fail'
    )
}}

select * from {{ ref('int_enrollments') }}

{% if is_incremental() %}
    where meta_ingest_time > (select max(meta_ingest_time) from {{ this }})
{% endif %}
```

## Testing Strategy

### Dimension Tests
```yaml
models:
  - name: dim_student
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - student_id
            - effective_date
    
    columns:
      - name: student_key
        tests:
          - not_null
          - unique
      
      - name: student_id
        tests:
          - not_null
      
      - name: email
        tests:
          - dbt_utils.not_null_proportion:
              at_least: 0.95
```

### Fact Tests
```yaml
models:
  - name: fact_enrollment
    tests:
      - dbt_utils.recency:
          datepart: day
          field: meta_ingest_time
          interval: 1
    
    columns:
      - name: enrollment_key
        tests:
          - not_null
          - unique
      
      - name: student_key
        tests:
          - not_null
          - relationships:
              to: ref('dim_student')
              field: student_key
      
      - name: credits_attempted
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 20
              inclusive: true
```

## Documentation Standards

### Model Documentation
```yaml
models:
  - name: dim_student
    description: |
      Student dimension containing demographic, contact, and enrollment 
      information. One row per student with SCD Type 2 tracking for 
      historical changes in major, enrollment status, etc.
      
      **Grain**: One row per student per version (SCD Type 2)
      **Source**: deterge.int_students
      **Updates**: Daily via incremental refresh
      **Owner**: Institutional Research
    
    columns:
      - name: student_key
        description: Surrogate key for student dimension (primary key)
      
      - name: student_id
        description: |
          Natural business key for student. This is the institutional 
          student ID displayed to end users and used for reporting.
      
      - name: effective_date
        description: Date when this version of the student record became active
      
      - name: end_date
        description: Date when this version became inactive (null for current)
      
      - name: is_current
        description: Flag indicating if this is the current version (true/false)
```

## Best Practices

### ✅ DO

**Dimensional Modeling**:
- Clearly define grain for every fact table
- Use surrogate keys for all primary/foreign keys
- Include natural/business keys for user reference
- Denormalize dimensions for query simplicity
- Document SCD types clearly

**Performance**:
- Materialize dimensions as tables
- Use incremental for large fact tables
- Apply clustering on common filter columns
- Pre-aggregate where appropriate
- Monitor query performance regularly

**Governance**:
- Apply masking policies to PII
- Tag tables and columns appropriately
- Implement row-level security where needed
- Document data ownership clearly
- Enforce testing standards

**User Experience**:
- Use clear, business-friendly names
- Add comprehensive descriptions
- Create logical data marts
- Provide example queries
- Maintain data dictionaries

### ❌ DON'T

**Dimensional Modeling**:
- Create snowflake schemas (normalize dimensions)
- Mix multiple grains in one fact table
- Use natural keys as primary keys
- Skip SCD tracking when history matters
- Create overly wide dimensions

**Performance**:
- Materialize everything as views
- Skip clustering on large tables
- Create deeply nested CTEs
- Ignore incremental opportunities
- Forget to monitor query costs

**Governance**:
- Expose unmasked PII to all users
- Skip data classification
- Grant overly permissive access
- Ignore compliance requirements
- Leave security as an afterthought

**User Experience**:
- Use cryptic abbreviations
- Skip documentation
- Create unintuitive joins
- Ignore user feedback
- Assume users understand the model

## Access Patterns

### BI Tool Consumption
Optimize for typical BI queries:

```sql
-- Example: Enrollment dashboard query
select
    trm.term_name,
    trm.term_year,
    crs.department_name,
    crs.college_name,
    count(distinct enr.student_key) as student_count,
    sum(enr.credits_attempted) as total_credits
from {{ ref('fact_enrollment') }} enr
join {{ ref('dim_term') }} trm on enr.term_key = trm.term_key
join {{ ref('dim_course') }} crs on enr.course_key = crs.course_key
where trm.term_year >= 2020
  and enr.enrollment_status = 'ENROLLED'
group by 1, 2, 3, 4
```

### Self-Service Analytics
Enable end users to create their own queries:

```sql
-- Data mart with pre-joined dimensions
-- mart_enrollment_detail.sql
{{
    config(
        materialized='table',
        schema='distribute'
    )
}}

select
    -- Student attributes
    std.student_id,
    std.full_name,
    std.email,
    std.major_name,
    std.class_level,
    
    -- Course attributes
    crs.course_code,
    crs.course_title,
    crs.department_name,
    crs.credits,
    
    -- Term attributes
    trm.term_code,
    trm.term_name,
    trm.term_year,
    trm.term_season,
    
    -- Enrollment facts
    enr.enrollment_date,
    enr.grade_code,
    enr.credits_earned,
    enr.grade_points

from {{ ref('fact_enrollment') }} enr
join {{ ref('dim_student') }} std on enr.student_key = std.student_key
join {{ ref('dim_course') }} crs on enr.course_key = crs.course_key
join {{ ref('dim_term') }} trm on enr.term_key = trm.term_key

where std.is_current = true
  and enr.enrollment_status in ('ENROLLED', 'COMPLETED')
```

## Migration from Deterge to Distribute

### Transformation Example
```
Deterge (Silver)              Distribute (Gold)
─────────────────────────     ──────────────────────
int_students             →    dim_student
  - Clean                       - Add surrogate key
  - Integrated                  - Add SCD tracking
  - Unmasked                    - Apply masking
                                - Add business logic

int_enrollments          →    fact_enrollment
  - Detailed                    - Add surrogate keys
  - All records                 - Add date keys
  - Unmasked                    - Optimize clustering
                                - Add aggregations
```

## Conclusion

The Distribute layer transforms clean, integrated data from Deterge into analytics-ready dimensional models. By implementing star schemas, applying governance policies, and optimizing for query performance, the Distribute layer enables self-service analytics while maintaining data quality and security.

**Key Takeaways**:
- Dimensional models (star schema) for intuitive queries
- Governance applied through masking, tagging, and RLS
- Performance optimized via materialization and clustering
- Business-friendly naming and documentation
- Pre-aggregated marts for common use cases
- Single source of truth for enterprise reporting

The Distribute layer is where data science meets business value, providing the foundation for data-driven decision making across the institution.

{% enddocs %}