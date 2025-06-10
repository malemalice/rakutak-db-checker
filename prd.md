# Product Requirements Document (PRD)

## Project Title: AI-Extensible Python Tool for Data Validation in DLT-Based Database Migration

---

## 1. Purpose

This project aims to automate data validation between source and target databases during migration using [DLT (dlthub)](https://github.com/dlt-hub/dlt). The tool will be built with Python and structured for ease of extension using AI code generation agents (e.g., ChatGPT, Copilot). It specifically handles ETL metadata columns and focuses on business data validation.

---

## 2. Goals

- Automate validation of table structure and content between source and target databases.
- Eliminate manual row-by-row comparison.
- Enable row count check, hash-based validation, and sampling comparison.
- **Handle ETL metadata columns**: Ignore DLT and other ETL-added columns during validation.
- Provide comprehensive summary reports for stakeholders.
- Ensure code is modular, readable, and compatible with generative AI code suggestions.

---

## 3. Functional Requirements

### 3.1 Configuration

- Configuration file (`settings.yaml`) must support:
  - Database URIs or credentials.
  - Table include/exclude list.
  - Validation types (row count, hash check, sample comparison).
  - **Ignored columns list**: Configurable columns to skip during hash validation.
  - Output format and destination.
  - Logging configuration with clean summary formatting.

### 3.2 Database Connectivity

- Modular database connectors using SQLAlchemy.
- Initial support for:
  - PostgreSQL
  - MySQL/MariaDB
- Easy to extend for other engines (e.g., BigQuery, Snowflake).

### 3.3 Table Matching

- Automatically detect common tables between source and target.
- Allow table name mapping overrides.
- **Schema flexibility**: Handle target tables with additional ETL metadata columns.

### 3.4 Row Count Validation

- Compare row counts for each table.
- Output mismatches with metadata (e.g., delta, percentage difference).

### 3.5 Hash-Based Validation

- Create consistent row-level hashes using MD5 or SHA256.
- **Column filtering**: Ignore specified columns (e.g., `_dlt_load_id`, `created_at`) during hash generation.
- Compare hashes between source and target for matching row identifiers.
- Allow chunking for large tables.
- **Schema validation**: Detect column differences while allowing ignored columns to exist only in target.
- **Detailed mismatch logging**: Log complete source and target row data when hash mismatches occur.
- **Flexible row identification**: Support primary keys, unique constraints, or all-column comparison.
- **Composite primary key support**: Handle multi-column primary keys seamlessly.

### 3.6 Sample Data Comparison

- Simplified row count comparison for lightweight validation.
- Focus on count matching rather than content comparison.

### 3.7 Reporting

- Output summary in clean, readable format:
  - Console output with emojis and progress indicators
  - Structured logs with timestamps for debugging
- Include total tables compared, pass/fail stats, mismatched rows, etc.
- **ETL-aware reporting**: Show which columns are being ignored per table.
- Log all validation processes and results with timestamps.
- **Detailed mismatch logs**: For hash validation failures, log complete row data to help identify specific differences.

### 3.8 Detailed Debugging Features

- **Row-by-row comparison**: When hash mismatches occur, log complete source and target row data
- **Difference identification**: Highlight specific columns that differ between source and target
- **Data type visibility**: Use `repr()` to show exact data types and precision differences
- **Primary key tracking**: Include primary key values for easy manual verification
- **Structured logging**: All components use `loguru` for consistent log formatting

### 3.9 ETL Integration Features

- **Configurable ignored columns**: Global list of columns to ignore during hash validation
- **DLT compatibility**: Default ignore list includes `_dlt_load_id`, `_dlt_id`, `_extracted_at`, `_loaded_at`
- **Audit column handling**: Optional ignoring of `created_at`, `updated_at`, `modified_at`
- **Clear reporting**: Show ignored columns in validation summaries

### 3.10 Advanced Primary Key Handling

- **No Primary Key Support**: Automatic fallback to unique constraints when primary keys are missing
- **Composite Primary Key Support**: Full support for multi-column primary keys of any size
- **Unique Constraint Fallback**: Use unique constraints as row identifiers when no primary key exists
- **All-Column Comparison**: Last resort option for tables without any unique identifiers
- **Performance Warnings**: Clear alerts when using performance-impacting fallback methods
- **Configurable Behavior**: Settings to control fallback strategies and validation approaches

---

## 4. Non-Functional Requirements

- Python 3.8 or newer.
- CLI-first tool, with potential for Web UI in future.
- AI-extensible:
  - Modular architecture.
  - Clear function boundaries.
  - Descriptive docstrings and filenames.
- Comprehensive logging system:
  - Clean console output for user experience
  - Detailed file logging with `loguru` for debugging
  - Row-by-row mismatch analysis for hash validation
  - Structured logging for machine processing.
  - Human-readable logs for debugging.
  - Log rotation and retention policies.
- Docker support for containerized deployment.
- **ETL-friendly**: Handle common ETL tool patterns and metadata.

---

## 5. Configuration Example

```yaml
# Database connections
source_db:
  type: postgresql
  host: localhost
  port: 5432
  user: source_user
  password: source_password
  database: source_db

target_db:
  type: postgresql
  host: localhost
  port: 5432
  user: target_user
  password: target_password
  database: target_db

# Validation settings
validation:
  types:
    - row_count
    - hash_check
    - sample_comparison
  sample_size: 1000
  chunk_size: 10000
  
  # ETL metadata columns to ignore during hash validation
  ignored_columns:
    - _dlt_load_id      # DLT load identifier
    - _dlt_id           # DLT unique row identifier
    - _extracted_at     # ETL extraction timestamp
    - _loaded_at        # ETL load timestamp
    - created_at        # Record creation timestamp
    - updated_at        # Record modification timestamp
  
  # Primary key handling configuration
  allow_tables_without_pk: true          # Allow validation of tables without primary keys
  fallback_to_unique_constraints: true   # Use unique constraints if no primary key
  allow_all_columns_identifier: false    # Use all columns as identifier (performance impact)

# Table configuration
tables:
  include: []           # Empty means all tables
  exclude: []          # Tables to skip
  name_mapping: {}     # Source to target table mapping

# Logging
logging:
  level: INFO
  file: logs/validator.log
  format: "{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
```

---

## 6. Folder Structure

```plaintext
data_checker/
├── config/
│   └── settings.yaml          # Configuration with ignored columns
│
├── connectors/
│   ├── postgres.py            # PostgreSQL connector using SQLAlchemy
│   ├── mysql.py               # MySQL/MariaDB connector
│   └── factory.py             # Connector factory based on DB type
│
├── validators/
│   ├── row_count.py           # Row count comparison logic
│   ├── hash_compare.py        # Hash-based validation with column filtering
│   └── sample_compare.py      # Simple row count validation
│
├── reports/
│   └── report_generator.py    # Generate clean summary reports
│
├── utils/
│   ├── hashing.py             # Utility to create hashes from filtered rows
│   └── table_utils.py         # Table discovery, schema matching, etc.
│
├── logs/
│   └── .gitkeep              # Directory for log files
│
├── main.py                    # Entry point with summary reporting
├── Dockerfile                 # Docker configuration
├── docker-compose.yml         # Docker Compose configuration
├── run.sh                     # Shell script for running the validator
└── README.md                  # Updated documentation with ETL features
```

---

## 7. Sample Output

### Console Output
```
🚀 Starting HashValidator for 5 tables
[1/5] Validating table: users ... ✅ PASSED
[2/5] Validating table: orders ... ❌ FAILED (3 hash mismatches)
[3/5] Validating table: products ... ✅ PASSED
[4/5] Validating table: categories ... ✅ PASSED
[5/5] Validating table: reviews ... ✅ PASSED
✅ Completed HashValidator validation

============================================================
VALIDATION SUMMARY - HashValidator
============================================================
Total tables validated: 5
✅ Passed: 4 tables
❌ Failed: 1 tables
⚠️  Errors: 0 tables

✅ PASSED TABLES:
   • categories
   • products
   • reviews
   • users

❌ FAILED TABLES:
   • orders (3 hash mismatches, ignored: ['_dlt_load_id', 'created_at'])
   • user_sessions (no primary key, using unique constraint: ['session_token'])
   • temp_data (no unique identifiers, using all-column comparison - performance impact)

📋 DETAILED LOGS:
   For row-by-row data comparison of mismatched records,
   check the detailed logs at: logs/data_checker.log
============================================================
```

### Log Output
```
2025-05-29 10:30:00 | INFO | Hash validation will ignore these columns: ['_dlt_load_id', '_dlt_id', '_extracted_at', '_loaded_at']
2025-05-29 10:30:01 | INFO | Ignored columns for orders: ['_dlt_load_id', 'created_at']
2025-05-29 10:30:01 | INFO | Generating hashes for table orders using columns: ['order_id', 'user_id', 'amount', 'status']
2025-05-29 10:30:02 | INFO | === HASH MISMATCH #1 IN TABLE 'orders' ===
2025-05-29 10:30:02 | INFO | Primary key: {'order_id': '123'}
2025-05-29 10:30:02 | INFO | SOURCE ROW DATA:
2025-05-29 10:30:02 | INFO |   order_id: '123'
2025-05-29 10:30:02 | INFO |   user_id: '456'
2025-05-29 10:30:02 | INFO |   amount: Decimal('150.00')
2025-05-29 10:30:02 | INFO |   status: 'completed'
2025-05-29 10:30:02 | INFO | TARGET ROW DATA:
2025-05-29 10:30:02 | INFO |   order_id: '123'
2025-05-29 10:30:02 | INFO |   user_id: '456'
2025-05-29 10:30:02 | INFO |   amount: Decimal('150.0')
2025-05-29 10:30:02 | INFO |   status: 'completed'
2025-05-29 10:30:02 | INFO | DIFFERENCES FOUND:
2025-05-29 10:30:02 | INFO |   - amount: 'Decimal('150.00')' != 'Decimal('150.0')'
2025-05-29 10:30:02 | INFO | ============================================================
2025-05-29 10:30:02 | INFO | 📋 For detailed row-by-row comparison of mismatched data, check: logs/data_checker.log
```

---

## 8. Deployment

### 8.1 Direct Installation
- Python virtual environment setup
- Manual dependency installation
- Direct execution via Python

### 8.2 Docker Deployment
- Docker image build
- Docker Compose for service orchestration
- Volume mounting for configuration and logs
- Shell script for execution

---

## 9. ETL/DLT Integration Benefits

- **Seamless DLT integration**: Works out-of-the-box with DLT-migrated data
- **Flexible schema handling**: Target can have additional ETL metadata columns
- **Business data focus**: Validates actual data while ignoring technical metadata
- **Configurable per environment**: Easy to adjust ignored columns per deployment
- **Clear reporting**: Transparency about what's being validated vs. ignored

---
