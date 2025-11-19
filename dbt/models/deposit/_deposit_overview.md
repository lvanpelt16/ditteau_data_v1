{% docs deposit_overview %}

# Deposit Layer (Bronze Schema)

## Overview

The **Deposit layer** represents the **Bronze tier** of the Ditteau Data architecture. This schema serves as the landing zone for raw data ingested from external source systems into Snowflake. The Deposit layer is intentionally minimal in the dbt project structure, as it primarily contains data loaded through external processes (Snowflake stages, data shares, or ETL tools) rather than dbt-managed transformations.

## Purpose

The Deposit layer serves these critical functions:

- **Raw Data Preservation**: Maintains an unaltered copy of source system data as it enters Snowflake
- **Data Lineage Foundation**: Establishes the starting point for all downstream dbt transformations
- **Source System Insulation**: Creates a buffer between volatile operational systems and the analytics environment
- **Historical Record**: Preserves the complete history of source data states for audit and recovery

## Architectural Position

```
┌─────────────────┐
│ External        │
│ Source Systems  │ → Data loaded via stages, shares, or external ETL
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│ DEPOSIT         │ ← You are here (Bronze schema in Snowflake)
│ (Bronze)        │   • Tables/views loaded externally
└────────┬────────┘   • Minimal dbt models
         │            • Source definitions in deterge/staging
         ↓
┌─────────────────┐
│ DETERGE         │ ← dbt staging models consume from Deposit
│ (Silver)        │   • Source definitions: _jcx_sources.yml, _pf_sources.yml
└────────┬────────┘   • Transformation and cleaning
         │
         ↓
┌─────────────────┐
│ DISTRIBUTE      │ ← Analytics-ready models
│ (Gold)          │
└─────────────────┘
```

## dbt Project Structure

### Minimal Deposit Folder
The `models/deposit/` folder in the dbt project contains **only documentation**:

```
models/
└── deposit/
    └── _deposit_overview.md    # This file (documentation only)
```

**No SQL models exist in models/deposit/** because:
- Data is loaded into the DEPOSIT schema via external processes
- Source definitions are managed in `models/deterge/staging/`
- The first dbt transformations occur in the Deterge (staging) layer

### Source Definitions Location
All source definitions that reference Deposit tables are located in:

```
models/
└── deterge/
    └── staging/
        ├── _jcx_sources.yml     # Jenzabar sources
        ├── _pf_sources.yml      # PowerFaids sources
        └── [other]_sources.yml  # Additional source systems
```

This organization keeps source definitions close to the staging models that consume them.

## Data Ingestion into Deposit

### External Load Methods
Data arrives in the Deposit schema through:

1. **Snowflake Stages**: CSV/Parquet files uploaded to named stages and loaded via COPY commands
2. **Snowflake Data Shares**: Direct data sharing from partner systems
3. **External ETL Tools**: Third-party tools (Fivetran, Airbyte, custom scripts) loading data
4. **API Integrations**: Custom extraction processes pulling from REST APIs
5. **Database Replication**: Change data capture (CDC) from operational databases

### Deposit Schema Characteristics
- **Schema Name**: `DEPOSIT` (or `<INSTITUTION>_DATA.DEPOSIT`)
- **Materialization**: Tables and views created outside dbt
- **Mutability**: Varies by table (some append-only, others full refresh)
- **dbt Role**: dbt reads from Deposit but does not manage table creation

## Metadata Standards

### Standardized Metadata Columns
All tables in Deposit should include these metadata columns (added during external load processes):

```sql
_loaded_at TIMESTAMP_NTZ        -- When record entered Snowflake (from external load)
_source_system VARCHAR          -- System of origin (e.g., 'JENZABAR_CX', 'POWERFAIDS')
_source_table VARCHAR           -- Source table name
_source_file VARCHAR            -- Source file name (optional)
_data_classification VARCHAR    -- Classification (e.g., 'INTERNAL', 'CONFIDENTIAL')
_data_owner VARCHAR             -- Data steward or owning department
_ingest_type VARCHAR            -- Load method (e.g., 'FULL_LOAD', 'INCREMENTAL')
```

### Metadata Defaults
When metadata is not provided by the source system, use these defaults (defined in `metadata_defaults()` macro):

| Column | Default Value |
|--------|---------------|
| `meta_source_system` | `'UNKNOWN'` |
| `meta_source_file` | `'UNKNOWN'` |
| `meta_ingest_type` | `'UNKNOWN'` |
| `meta_data_owner` | `'UNKNOWN'` |
| `meta_data_class` | `'INTERNAL'` |

Note: The macro uses the `meta_` prefix for defaults, but the actual column names use the `_` prefix.

### Metadata in dbt Models
The Deterge staging models use the `add_source_metadata()` macro to add these metadata columns:

```sql
-- In deterge/staging models
{{ 
    config(
        materialized='view'
    )
}}

select
    -- Business columns
    student_id,
    first_name,
    last_name,
    
    -- Metadata columns automatically added
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

## Design Principles

### 1. Preserve Source Fidelity
- Keep all source columns, even if unused initially
- Maintain original data types where possible
- Do not apply business logic or transformations
- Document any technical adjustments (e.g., type casting)

### 2. Add Technical Metadata
- Every record must have `meta_ingest_time`
- Source system identification is required
- Unique record identifiers enable lineage tracking

### 3. No Business Logic
- Filter rules → Applied in Deterge
- Calculated fields → Created in Deterge or Distribute
- Data quality fixes → Handled in Deterge
- Cross-system joins → Performed in Deterge

### 4. Immutability Preferred
- Append-only tables preserve complete history
- Full refresh patterns clearly documented
- Change tracking supports Type 2 dimensions downstream

## Source Configuration in dbt

### Example: JCX Source Definition
File: `models/deterge/staging/_jcx_sources.yml`

```yaml
version: 2

sources:
  - name: jcx
    description: Jenzabar CX source tables in Deposit schema
    database: "{{ env_var('DBT_DATABASE', 'DEV_DITTEAU_DATA') }}"
    schema: deposit
    
    tables:
      - name: students
        description: Student demographic and enrollment information
        meta:
          source_system: JENZABAR
          data_owner: REGISTRAR
          contains_pii: true
        
        columns:
          - name: student_id
            description: Primary student identifier
            tests:
              - not_null
              - unique
          
          - name: meta_ingest_time
            description: Timestamp when record entered Snowflake
          
          - name: meta_source_system
            description: Source system identifier (JENZABAR)
```

### Freshness Monitoring
Define freshness expectations in source definitions:

```yaml
tables:
  - name: students
    freshness:
      warn_after: {count: 24, period: hour}
      error_after: {count: 48, period: hour}
```

This alerts when external load processes fail or fall behind schedule.

## Governance and Security

### Access Control
Typical access patterns for Deposit:

- **ETL Service Accounts**: Write access for data loading
- **Data Engineers**: Read access for pipeline development
- **dbt Service Account**: Read access only
- **Analysts**: No direct access (use Deterge/Distribute layers)

### Data Classification
Raw data often contains sensitive information. Apply appropriate Snowflake tags:

- `CONTAINS_PII`: Flag for personally identifiable information
- `SENSITIVITY_LEVEL`: Public / Internal / Confidential / Restricted
- `COMPLIANCE_TYPE`: FERPA / GDPR / CCPA / etc.

### Masking Policies
While masking is typically applied in Distribute, some institutions mask at Deposit for:
- Regulatory compliance requirements
- Limited data engineer access scenarios
- Extra protection for highly sensitive systems

## Best Practices

### ✅ DO
- Preserve all source columns initially
- Add comprehensive metadata during load
- Document external load processes
- Monitor freshness with dbt source tests
- Use consistent naming conventions
- Include schema documentation

### ❌ DON'T
- Create dbt models in `models/deposit/`
- Apply business logic or transformations
- Filter records based on business rules
- Attempt to "fix" data quality issues
- Join across source systems
- Rename columns for business clarity

## Integration with dbt Workflow

### 1. External Load Process
```
Source System → Load Script → DEPOSIT.STUDENTS (table created in Snowflake)
```

### 2. Source Definition in dbt
```yaml
# models/deterge/staging/_jcx_sources.yml
sources:
  - name: jcx
    schema: deposit
    tables:
      - name: students
```

### 3. Staging Model References Source
```sql
-- models/deterge/staging/stg_jcx__students.sql
select
    student_id,
    first_name,
    last_name,
    {{ add_metadata_from_source(source('jcx', 'students')) }}
from {{ source('jcx', 'students') }}
```

### 4. dbt Lineage
```
source('jcx', 'students') → stg_jcx__students → int_students → dim_students
     [DEPOSIT]                 [DETERGE]         [DETERGE]    [DISTRIBUTE]
```

## Monitoring and Maintenance

### Data Freshness Alerts
Use dbt's source freshness checks:

```bash
dbt source freshness
```

Alerts when external loads haven't run on schedule.

### Schema Evolution
When source systems add/remove columns:

1. External team updates Deposit table schema
2. dbt developer updates source definition in `_[source]_sources.yml`
3. Staging models automatically include new columns (if using `select *`)
4. Downstream impacts assessed and models updated as needed

### Volume Monitoring
Track record counts and load patterns:
- Unexpected spikes may indicate data quality issues
- Missing loads trigger freshness alerts
- Historical volume trends inform capacity planning

## Example: Complete Flow

### Step 1: External Load (Outside dbt)
```sql
-- Snowflake script (run by external ETL process)
COPY INTO DEPOSIT.STUDENTS
FROM @MY_STAGE/students.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- Add metadata
UPDATE DEPOSIT.STUDENTS
SET 
    meta_ingest_time = CURRENT_TIMESTAMP(),
    meta_source_system = 'JENZABAR',
    meta_source_file = 'students.csv'
WHERE meta_ingest_time IS NULL;
```

### Step 2: Source Definition (dbt)
```yaml
# models/deterge/staging/_jcx_sources.yml
sources:
  - name: jcx
    schema: deposit
    tables:
      - name: students
```

### Step 3: Staging Model (dbt)
```sql
-- models/deterge/staging/stg_jcx__students.sql
select
    student_id,
    first_name,
    last_name,
    {{ add_source_metadata('jcx', 'students') }}
from {{ source('jcx', 'students') }}
```

## Conclusion

The Deposit layer is the foundation of Ditteau Data, but it operates primarily outside the dbt framework. By maintaining raw source data with consistent metadata, external load processes create a stable foundation for all downstream dbt transformations. The dbt project's role is to:

1. **Document** Deposit tables via source definitions in `deterge/staging`
2. **Monitor** data freshness and quality via source tests
3. **Consume** Deposit tables as the input to Deterge staging models

This clean separation ensures that dbt focuses on transformation logic while external ETL processes handle the complexities of data extraction and loading.

{% enddocs %}
