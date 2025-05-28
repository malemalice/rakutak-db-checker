# Product Requirements Document (PRD)

## Project Title: AI-Extensible Python Tool for Data Validation in DLT-Based Database Migration

---

## 1. Purpose

This project aims to automate data validation between source and target databases during migration using [DLT (dlthub)](https://github.com/dlt-hub/dlt). The tool will be built with Python and structured for ease of extension using AI code generation agents (e.g., ChatGPT, Copilot).

---

## 2. Goals

- Automate validation of table structure and content between source and target databases.
- Eliminate manual row-by-row comparison.
- Enable row count check, hash-based validation, and sampling comparison.
- Provide report outputs for stakeholders.
- Ensure code is modular, readable, and compatible with generative AI code suggestions.

---

## 3. Functional Requirements

### 3.1 Configuration

- Configuration file (`.env` or `settings.yaml`) must support:
  - Database URIs or credentials.
  - Table include/exclude list.
  - Validation types (row count, hash check, sample comparison).
  - Output format and destination.
  - Execution interval time (in minutes/seconds).
  - HTTP server port for health check.

### 3.2 Database Connectivity

- Modular database connectors using SQLAlchemy.
- Initial support for:
  - PostgreSQL
  - MySQL/MariaDB
- Easy to extend for other engines (e.g., BigQuery, Snowflake).

### 3.3 Table Matching

- Automatically detect common tables between source and target.
- Allow table name mapping overrides.

### 3.4 Row Count Validation

- Compare row counts for each table.
- Output mismatches with metadata (e.g., delta, percentage difference).

### 3.5 Hash-Based Validation

- Create consistent row-level hashes using MD5 or SHA256.
- Compare hashes between source and target for matching primary keys.
- Allow chunking for large tables.

### 3.6 Sample Data Comparison

- Randomly sample N rows per table.
- Compare corresponding fields and log mismatches.

### 3.7 Reporting

- Output summary in:
  - JSON (machine-readable)
  - Markdown or HTML (human-readable)
- Include total tables compared, pass/fail stats, mismatched rows, etc.
- Log all validation processes and results with timestamps.
- Store logs in both file and structured format (e.g., JSON) for analysis.

### 3.8 Health Check and Monitoring

- Expose HTTP endpoint for health check (e.g., `/health`).
- Return service status, last execution time, and basic metrics.
- Support readiness and liveness probes for container environments.

### 3.9 Scheduled Execution

- Run validation checks at configurable intervals.
- Support cron-like scheduling patterns.
- Maintain execution history and track performance metrics.
- Handle concurrent executions gracefully.

---

## 4. Non-Functional Requirements

- Python 3.8 or newer.
- CLI-first tool, with potential for Web UI in future.
- AI-extensible:
  - Modular architecture.
  - Clear function boundaries.
  - Descriptive docstrings and filenames.
- Comprehensive logging system:
  - Structured logging for machine processing.
  - Human-readable logs for debugging.
  - Log rotation and retention policies.
- HTTP server for health monitoring.
- Configurable execution intervals.

---

## 5. Folder Structure

```plaintext
data_checker/
├── config/
│   └── settings.yaml          # Configuration file (DB credentials, validation options)
│
├── connectors/
│   ├── postgres.py            # PostgreSQL connector using SQLAlchemy
│   ├── mysql.py               # MySQL/MariaDB connector
│   └── factory.py             # Connector factory based on DB type
│
├── validators/
│   ├── row_count.py           # Row count comparison logic
│   ├── hash_compare.py        # Hash-based validation logic
│   └── sample_compare.py      # Random sample comparison logic
│
├── reports/
│   └── report_generator.py    # Generate JSON/HTML/Markdown reports
│
├── utils/
│   ├── hashing.py             # Utility to create hashes from rows
│   └── table_utils.py         # Table discovery, schema matching, etc.
│
├── server/
│   ├── health.py              # Health check endpoint implementation
│   └── metrics.py             # Metrics collection and exposure
│
├── scheduler/
│   └── executor.py            # Interval-based execution logic
│
├── logs/
│   └── .gitkeep              # Directory for log files
│
├── main.py                    # Entry point script
└── README.md                  # Project documentation
