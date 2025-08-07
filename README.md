# Database Validator

A Python-based tool for validating data between source and target databases during migration using DLT (dlthub).

## Features

- Automated validation of table structure and content
- Multiple validation methods:
  - Row count comparison
  - Hash-based validation (with configurable column ignoring)
  - Sample data comparison
- **Auto-generated MySQL UPDATE and INSERT queries** for fixing data differences between source and target
- ETL-friendly: Automatically ignores metadata columns added by tools like DLT
- Comprehensive logging with clean summary reports
- Support for PostgreSQL and MySQL databases

## Requirements

- Python 3.8 or newer
- PostgreSQL or MySQL database
- Docker and Docker Compose (for Docker installation)

## Installation

### Direct Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd data-checker
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

### Docker Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd data-checker
```

2. Create necessary directories:
```bash
mkdir -p config logs
```

3. Create your configuration file:
```bash
cp config/settings.yaml.example config/settings.yaml
# Edit config/settings.yaml with your database settings
```

4. Build the Docker image:
```bash
docker compose build
```

5. Start the services using Docker Compose:
```bash
docker compose up -d
```

## Configuration

1. Edit `config/settings.yaml` to configure:
   - Database connections
   - Validation settings
   - Table mappings
   - Ignored columns for hash validation
   - Logging configuration

Example configuration:
```yaml
source_db:
  type: postgresql
  host: localhost
  port: 5432
  user: source_user
  password: source_password
  database: source_db

target_db:
  type: mysql
  host: localhost
  port: 3306
  user: target_user
  password: target_password
  database: target_db

validation:
  types:
    - row_count
    - hash_check
    - sample_comparison
  sample_size: 1000
  chunk_size: 10000
  
  # Maximum number of detailed hash mismatches to log (prevents excessive logging)
  max_detailed_mismatches: 20
  
  # Fix query generation settings
  generate_fix_queries: true         # Enable generation of MySQL UPDATE queries for differences
  fix_queries_file: logs/fix-query.sql  # File to save generated fix queries
  
  # Row count validation settings
  row_count_missing_detection:
    enabled: true                    # Enable detection of missing rows when count mismatch occurs
    max_missing_rows_to_log: 50     # Maximum number of missing rows to log in detail
    max_table_size_for_detection: 1000000  # Skip missing row detection for tables larger than this
  
  # Columns to ignore during hash validation (ETL metadata)
  ignored_columns:
    - _dlt_load_id
    - _dlt_id
    - _extracted_at
    - _loaded_at
    - created_at
    - updated_at

tables:
  include: []  # Empty means all tables
  exclude: []  # Tables to skip
  name_mapping: {}  # Source to target table name mapping

logging:
  level: INFO
  file: logs/validator.log
  max_size: 10MB
  backup_count: 5
  format: "{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
```

### Ignored Columns Feature

The hash validator can ignore specific columns that are commonly added by ETL tools:

- **DLT metadata**: `_dlt_load_id`, `_dlt_id`, `_extracted_at`, `_loaded_at`
- **Audit columns**: `created_at`, `updated_at`, `modified_at`
- **ETL timestamps**: `insert_timestamp`, `etl_timestamp`, `processed_at`

This allows the target database to have additional metadata columns without causing validation failures.

## Usage

### Direct Usage

1. Run the validator:
```bash
python main.py
```

2. The application will:
   - Load the configuration
   - Connect to source and target databases
   - Run the configured validations
   - Log results to the configured log file

### Docker Usage

1. Make sure Docker Compose is running:
```bash
docker compose ps
```

2. Run the validator using Docker Compose:
```bash
docker compose exec app /app/run.sh run
```

3. To view logs:
```bash
docker compose logs -f app
```

4. To stop the services:
```bash
docker compose down
```

## Validation Methods

### Row Count Validation
Compares the number of rows in each table between source and target databases:
- **Basic counting**: Direct COUNT(*) queries for fast comparison
- **Missing row detection**: When count mismatch occurs, identifies which specific rows are missing
- **Primary key support**: Uses primary keys or unique constraints to identify missing rows  
- **Configurable limits**: Limits detailed logging to prevent excessive output (default: 50 missing rows)
- **Performance aware**: Skips detection for very large tables to maintain performance

### Hash-Based Validation
Creates MD5 hashes of row data and compares them between databases:
- Ignores configured metadata columns
- Detects schema differences
- Identifies data mismatches with specific row references
- Handles chunking for large tables
- **Detailed mismatch logging**: Logs complete source and target row data for mismatched records
- **Configurable logging limit**: Limits detailed logging to prevent excessive output (default: 20 mismatches)

### Sample Data Comparison
Performs simple row count comparison (lightweight version of row count validation).

## Detailed Logging

### Hash Validation Logging
When hash validation finds mismatches, detailed row-by-row comparison data is logged to `logs/data_checker.log`. This includes:

- **Primary key values** of mismatched rows
- **Complete source row data** with all column values
- **Complete target row data** with all column values
- **Specific differences** showing which columns differ and their values

**Note**: To prevent excessive log files, detailed logging is limited to the first 20 mismatches per table by default. This can be configured using the `max_detailed_mismatches` setting. All mismatches are still counted and reported in the summary.

### Example Detailed Log Output
```
2025-05-29 10:30:15 | INFO | === HASH MISMATCH #1 IN TABLE 'orders' ===
2025-05-29 10:30:15 | INFO | Primary key: {'order_id': '123'}
2025-05-29 10:30:15 | INFO | SOURCE ROW DATA:
2025-05-29 10:30:15 | INFO |   order_id: '123'
2025-05-29 10:30:15 | INFO |   amount: Decimal('150.00')
2025-05-29 10:30:15 | INFO |   status: 'completed'
2025-05-29 10:30:15 | INFO | TARGET ROW DATA:
2025-05-29 10:30:15 | INFO |   order_id: '123'
2025-05-29 10:30:15 | INFO |   amount: Decimal('150.0')
2025-05-29 10:30:15 | INFO |   status: 'completed'
2025-05-29 10:30:15 | INFO | DIFFERENCES FOUND:
2025-05-29 10:30:15 | INFO |   - amount: 'Decimal('150.00')' != 'Decimal('150.0')'
2025-05-29 10:30:15 | INFO | ============================================================
```

This detailed logging helps identify:
- **Data type differences** (Decimal precision, timestamp microseconds)
- **Encoding issues** (character encoding differences)
- **Null handling** (NULL vs empty string)
- **Precision mismatches** (floating point precision)

### Missing Row Detection Logging
When row count validation detects count mismatches, missing row identifiers are logged to `logs/data_checker.log`. This includes:

- **Missing row identifiers** using primary keys or unique constraints
- **Identifier type used** (primary_key, unique_constraint, all_columns)

## Fix Query Generation

When hash validation finds differences between source and target data, the tool can automatically generate MySQL UPDATE and INSERT queries to fix the differences. This feature is particularly useful for data synchronization and migration scenarios.

### Configuration

Enable fix query generation in your `config/settings.yaml`:

```yaml
validation:
  # ... other settings ...
  
  # Fix query generation settings
  generate_fix_queries: true         # Enable generation of MySQL UPDATE queries for differences
  fix_queries_file: logs/fix-query.sql  # File to save generated fix queries
```

### How It Works

1. **Automatic Detection**: When hash validation finds mismatched or missing rows, the tool automatically:
   - Retrieves the complete row data from both source and target databases
   - Compares all column values (excluding ignored columns)
   - Identifies which specific columns have different values
   - Detects rows that exist in source but not in target

2. **Query Generation**: For each issue found, generates appropriate MySQL queries:
   - **UPDATE queries**: For rows that exist in both databases but have different values
   - **INSERT queries**: For rows that exist in source but are missing in target
   - Uses the primary key or unique constraint to identify specific rows
   - Updates only the columns that have different values
   - Properly escapes string values and handles NULL values
   - Respects the `ignored_columns` configuration
   - Uses source data as the valid reference for all queries

3. **File Output**: All generated queries are saved to `logs/fix-query.sql` with:
   - Clear comments indicating the purpose
   - Numbered queries for easy reference
   - Proper SQL syntax ready for execution

### Example Generated Queries

```sql
-- Auto-generated fix queries for data differences
-- Generated by db-checker
-- Execute these queries on the TARGET database to fix differences
-- Source data is used as the valid reference for all queries

-- Fix query #1 (UPDATE - different values)
UPDATE users SET name = 'John Doe', email = 'john@example.com' WHERE id = 1;

-- Fix query #2 (INSERT - missing row)
INSERT INTO orders (order_id, user_id, total_amount, status) VALUES (123, 5, 299.99, 'completed');

-- Fix query #3 (UPDATE - different values)
UPDATE products SET stock_quantity = 5, price = 1299.99 WHERE product_id = 456;
```

### Usage Guidelines

- **Review Before Execution**: Always review the generated queries before executing them on your target database
- **Backup First**: Create a backup of your target database before applying fix queries
- **Test Environment**: Consider testing the queries in a staging environment first
- **Batch Execution**: The generated file contains all queries and can be executed as a batch
- **Selective Application**: You can manually select specific queries to apply based on your needs

### Limitations

- **Primary Key Required**: Fix queries are only generated for tables with primary keys or unique constraints
- **MySQL Syntax**: Currently generates MySQL-compatible UPDATE and INSERT queries
- **Column-Level Updates**: Only updates columns that actually differ between source and target
- **Source as Reference**: Uses source data as the valid reference for all queries
- **No DELETE**: Currently only generates UPDATE and INSERT queries, not DELETE statements
- **Separate sections** for rows missing in target vs. missing in source
- **Count summaries** showing total numbers of missing rows

**Note**: To prevent excessive log files, missing row logging is limited to the first 50 missing rows per table by default. This can be configured using the `max_missing_rows_to_log` setting. All missing rows are still counted and reported in the summary.

#### Example Missing Row Log Output
```
2025-07-22 16:30:15 | INFO | === ROWS MISSING IN TARGET - TABLE 'orders' ===
2025-07-22 16:30:15 | INFO | Identifier type: primary_key
2025-07-22 16:30:15 | INFO | Identifier columns: ['order_id']
2025-07-22 16:30:15 | INFO | Total missing rows: 5
2025-07-22 16:30:15 | INFO |   Missing row #1: 12345
2025-07-22 16:30:15 | INFO |   Missing row #2: 12346
2025-07-22 16:30:15 | INFO |   Missing row #3: 12347
2025-07-22 16:30:15 | INFO |   Missing row #4: 12348
2025-07-22 16:30:15 | INFO |   Missing row #5: 12349
2025-07-22 16:30:15 | INFO | ============================================================
```

## Sample Output

```
🚀 Starting RowCountValidator for 3 tables
[1/3] Validating table: users ... ✅ PASSED
[2/3] Validating table: orders ... ❌ FAILED (Row count diff: 5)
[3/3] Validating table: products ... ✅ PASSED
✅ Completed RowCountValidator validation

============================================================
VALIDATION SUMMARY - RowCountValidator
============================================================
Total tables validated: 3
✅ Passed: 2 tables
❌ Failed: 1 tables
⚠️  Errors: 0 tables

✅ PASSED TABLES:
   • products
   • users

❌ FAILED TABLES:
   • orders (source: 1000, target: 995, diff: 5, 5 missing in target)

📋 DETAILED LOGS:
   For detailed missing row identifiers,
   check the detailed logs at: logs/data_checker.log
============================================================

🚀 Starting HashValidator for 3 tables
[1/3] Validating table: users ... ✅ PASSED
[2/3] Validating table: orders ... ❌ FAILED (2 hash mismatches)
[3/3] Validating table: products ... ✅ PASSED
✅ Completed HashValidator validation

============================================================
VALIDATION SUMMARY - HashValidator
============================================================
Total tables validated: 3
✅ Passed: 2 tables
❌ Failed: 1 tables
⚠️  Errors: 0 tables

✅ PASSED TABLES:
   • products
   • users

❌ FAILED TABLES:
   • orders (2 hash mismatches, ignored: ['_dlt_load_id', 'created_at'])

📋 DETAILED LOGS:
   For row-by-row data comparison of mismatched records,
   check the detailed logs at: logs/data_checker.log
============================================================

============================================================
OVERALL VALIDATION SUMMARY
============================================================
Total tables: 3
✅ Fully matched: 2 tables (66.7%)
❌ Mismatched: 1 tables (33.3%)
⚠️  Errors: 0 tables (0.0%)

✅ FULLY MATCHED TABLES:
   • products
   • users

❌ TABLES WITH MISMATCHES:
   • orders
============================================================
```

## Logging

Logs are written to:
- **Console (stdout)**: Clean formatting for summaries with progress indicators
- **File (logs/data_checker.log)**: Detailed logs with timestamps including:
  - Process information and progress
  - Row-by-row mismatch details for hash validation
  - Complete source and target row data for debugging
- **Log rotation**: Based on size and retention settings in configuration

The application uses `loguru` for consistent, structured logging across all components.

## Development

The project structure follows a modular design:
```
data_checker/
├── config/          # Configuration files
├── connectors/      # Database connectors
├── validators/      # Validation methods (all using loguru)
├── reports/         # Report generation
├── utils/          # Utility functions
└── logs/           # Log files with detailed mismatch data
```

## ETL/DLT Integration

This tool is specifically designed for DLT and other ETL scenarios:

- **Handles metadata columns**: Automatically ignores ETL-added columns
- **Schema flexibility**: Target can have additional columns without errors  
- **Business data focus**: Validates actual data while ignoring technical metadata
- **Configurable**: Easy to add/remove ignored columns per environment

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Your chosen license] 