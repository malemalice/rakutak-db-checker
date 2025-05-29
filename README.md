# Database Validator

A Python-based tool for validating data between source and target databases during migration using DLT (dlthub).

## Features

- Automated validation of table structure and content
- Multiple validation methods:
  - Row count comparison
  - Hash-based validation (with configurable column ignoring)
  - Sample data comparison
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
Compares the number of rows in each table between source and target databases.

### Hash-Based Validation
Creates MD5 hashes of row data and compares them between databases:
- Ignores configured metadata columns
- Detects schema differences
- Identifies data mismatches with specific row references
- Handles chunking for large tables

### Sample Data Comparison
Performs simple row count comparison (lightweight version of row count validation).

## Sample Output

```
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
- Console (stdout) with clean formatting for summaries
- File (configured in settings.yaml) with timestamps
- Rotated based on size and retention settings

## Development

The project structure follows a modular design:
```
data_checker/
├── config/          # Configuration files
├── connectors/      # Database connectors
├── validators/      # Validation methods
├── reports/         # Report generation
├── utils/          # Utility functions
└── logs/           # Log files
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