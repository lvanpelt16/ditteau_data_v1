# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a dbt project for Ditteau Data, a higher education analytics data warehouse using Snowflake. The project follows a three-layer medallion architecture (Bronze/Silver/Gold) with custom naming:
- **Deposit** (Bronze) - Raw data landing zone, loaded externally via Snowflake Data Shares
- **Deterge** (Silver) - Cleaned, conformed, and integrated data
- **Distribute** (Gold) - Analytics-ready dimensional models (star schema)

## Common Commands

```bash
# Test Snowflake connection
dbt debug

# Install/update packages
dbt deps

# Run all models
dbt run

# Run specific model
dbt run --select model_name

# Run models in a layer
dbt run --select deterge.*
dbt run --select distribute.*

# Run with dependencies
dbt run --select +model_name      # model and upstream
dbt run --select model_name+      # model and downstream

# Run tests
dbt test
dbt test --select model_name

# Check source freshness
dbt source freshness

# Generate and serve docs
dbt docs generate && dbt docs serve

# Run against different environments
dbt run --target dev    # DITTEAU_DATA_DEV (default)
dbt run --target test   # DITTEAU_DATA_TEST
dbt run --target prod   # DITTEAU_DATA_PROD
```

## Architecture

### Layer Organization

```
models/
├── deposit/           # Documentation only - data loaded externally
├── deterge/           # Silver layer (main transformation work)
│   ├── staging/       # Source-specific conformance (views)
│   │   ├── jenzabar_cx/   # stg_jcx__*.sql
│   │   ├── powerfaids/    # stg_pf__*.sql
│   │   └── workday/       # stg_wd__*.sql
│   └── intermediate/  # Cross-source integration (tables)
└── distribute/        # Gold layer (analytics-ready)
    ├── dimensions/    # dim_*.sql (star schema dimensions)
    └── facts/         # fact_*.sql (star schema facts)
```

### Naming Conventions

- **Staging models**: `stg_<source>__<entity>.sql` (e.g., `stg_jcx__students.sql`)
- **Intermediate models**: `int_<entity>.sql` (e.g., `int_students.sql`)
- **Dimension tables**: `dim_<entity>.sql` (e.g., `dim_student.sql`)
- **Fact tables**: `fact_<process>.sql` (e.g., `fact_enrollment.sql`)

### Source Systems

| Prefix | System | Description |
|--------|--------|-------------|
| `jcx` | Jenzabar CX | Student Information System (via Snowflake Data Share) |
| `pf` | PowerFaids | Financial Aid |
| `wd` | Workday | HR/Finance |

### Materialization Strategy

- Staging models: `view` (lightweight, source-specific)
- Intermediate models: `table` (integration logic)
- Distribute models: `table` (optimized for analytics)

## Key Macros

### add_source_metadata()

Used in staging models to add standardized metadata columns:

```sql
select
    student_id,
    first_name,
    {{ add_source_metadata('jenzabar_cx_archive', 'prog_enr_rec') }}
from {{ source('jenzabar_cx_archive', 'prog_enr_rec') }}
```

Generates columns: `_source_system`, `_source_table`, `_data_classification`, `_data_owner`, `_ingest_type`, `_dbt_loaded_at`

## Source Configuration

Sources are defined in `models/deterge/staging/<system>/_<prefix>_sources.yml`. The primary source is the Jenzabar CX Data Share:
- Database: `ORGDATACLOUD$INTERNAL$DITTEAU_DEMO_CX_DATA`
- Schema: `DITTEAU_ARCHIVE`

## Project Variables

Key variables in `dbt_project.yml`:
- `current_academic_year`: Current academic year (e.g., '2024-2025')
- `current_term_code`: Current term code (e.g., 'FA24')
- `enable_masking_policies`: Feature flag for PII masking (false in dev, true in prod)

## Environment Configuration

Uses `profiles.yml` with three targets:
- `dev` - DITTEAU_DATA_DEV database, TRANSFORM_DEV warehouse
- `test` - DITTEAU_DATA_TEST database, TRANSFORM_TEST warehouse
- `prod` - DITTEAU_DATA_PROD database, TRANSFORM_PROD warehouse

Local development requires Snowflake credentials via environment variables (see `.env.example`) or `~/.dbt/profiles.yml`.

## dbt Packages

- `dbt_utils` (1.1.1) - Common utilities
- `dbt_expectations` (0.10.1) - Data quality testing
- `audit_helper` (0.9.0) - Audit column helpers
- `codegen` (0.12.1) - Code generation
