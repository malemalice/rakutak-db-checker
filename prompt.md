# AI Prompt Library for Python-Based Database Validation Tool

This document contains ready-to-use prompts for generative AI agents to help build or extend the `data_checker` Python tool used during database migration with DLT.

---

## 🔌 Database Connection

### PostgreSQL
> Write a Python function to connect to a PostgreSQL database using SQLAlchemy and return the engine.

### MySQL
> Generate a Python connector for MySQL using SQLAlchemy that returns a working engine.

### Connector Factory
> Create a factory function that returns the correct SQLAlchemy engine based on a config dictionary with keys: `type`, `host`, `user`, `password`, `database`.

---

## 📋 Table Discovery

### Get Common Tables
> Write a function that returns a list of table names present in both source and target databases using SQLAlchemy metadata.

### Match Table Names with Mapping
> Generate a function that matches source and target table names using a mapping config. Use the source name as default if not mapped.

---

## 🔢 Row Count Comparison

> Write a function called `compare_row_counts` that accepts two SQLAlchemy engines and a table name, and returns a dictionary with the source and target row counts and whether they match.

---

## 🔐 Hash-Based Validation

### Hash Generator
> Write a function to generate an MD5 hash from a dictionary of row values. Sort keys before hashing to ensure consistency.

### Hash Row-by-Row
> Create a function that computes a hash for each row in a table using SQLAlchemy. Use the primary key as the matching field.

### ETL-Friendly Hash Validation
> Write a hash validation function that can ignore specific columns (like `_dlt_load_id`, `created_at`) during hash generation. Accept a list of ignored columns as parameter.

### Column Filtering
> Create a function that filters out specified columns from a table's column list before generating hashes. This is useful for ignoring ETL metadata columns.

### Schema Validation with Ignored Columns
> Write a function that compares table schemas between source and target databases, but allows target to have additional columns that are in the ignored list.

### Detailed Mismatch Logging
> Write a function that logs complete source and target row data when hash mismatches occur. Include primary key values, all column data, and specific differences between the rows.

### Row Data Fetcher for Debugging
> Create a function that retrieves complete row data from a database given a table name and primary key values. This should be used for detailed logging of mismatched rows.

---

## 🎲 Sample Data Comparison

> Write a Python function that compares row counts between two tables using SQLAlchemy. This should be a lightweight alternative to complex row-by-row comparison.

---

## 📊 Reporting

### JSON Report
> Generate a function that takes a dictionary of validation results and writes it to a JSON file.

### Clean Console Summary
> Write a function that formats validation results into a clean console output with emojis, progress indicators, and summary statistics. Include information about ignored columns.

### ETL-Aware Summary
> Create a summary function that shows which columns were ignored during hash validation and includes this information in the final report.

### Detailed Log Instructions
> Write a function that displays instructions to users about where to find detailed mismatch logs when hash validation failures occur.

---

## ⚙️ Configuration Management

### Settings Loader with Ignored Columns
> Write a function to load a `settings.yaml` file that includes an `ignored_columns` list under the validation section. Handle missing ignored_columns gracefully.

### Column Ignore Configuration
> Create a configuration structure in YAML format that includes a list of columns to ignore during hash validation, specifically for ETL tools like DLT.

---

## ⚙️ CLI Entry Point

> Create a Python CLI using `argparse` that accepts source and target DB configs, a validation mode (row_count, hash, sample), and a table name or `--all` flag.

---

## 🧪 Test Stubs

> Write a Pytest unit test for `compare_row_counts()` with mocked SQLAlchemy engines returning known row counts.

### Hash Validation Tests
> Write unit tests for hash validation with ignored columns. Test that ignored columns don't affect hash generation.

### Detailed Logging Tests
> Write unit tests that verify detailed mismatch logging functionality, ensuring that source and target row data is properly logged when hash mismatches occur.

---

## 📝 Logging

### Structured Logger
> Create a logging configuration that writes to both console and file with rotation.

### Clean Console Output
> Write a logging setup that uses print() for clean summary output and logger for detailed process information.

### ETL-Aware Logging
> Create logging functions that specifically mention which columns are being ignored during validation and why.

### Loguru Integration
> Write a logging setup using loguru that ensures all validators and components use the same logging configuration for consistent output.

### Detailed Mismatch Logger
> Create a logging function that writes detailed row-by-row comparison data to the log file when hash mismatches occur. Include source data, target data, and differences.

### Row Difference Analyzer
> Write a function that compares two rows field by field and logs the specific differences, showing both the source and target values for each differing column.

---

## 🐳 Docker Support

### Dockerfile
> Create a Dockerfile that sets up the Python environment and installs dependencies.

### Docker Compose
> Write a docker-compose.yml file that includes the validator service and any required dependencies.

---

## 🔧 ETL Integration Prompts

### DLT Metadata Handler
> Write a function that automatically detects and handles DLT metadata columns (`_dlt_load_id`, `_dlt_id`, etc.) during database validation.

### Dynamic Column Ignoring
> Create a function that can dynamically detect ETL metadata columns based on naming patterns and add them to the ignored list.

### ETL-Friendly Validation
> Design a validation workflow that focuses on business data while automatically handling technical metadata added by ETL processes.

### Configuration for ETL Tools
> Generate a configuration template specifically designed for DLT and other ETL tool integrations, including common metadata columns to ignore.

---

## 🏗️ Architecture Prompts

### Modular Validator Design
> Create a base validator class that can be extended for different validation types (row count, hash, schema) with support for column filtering.

### Plugin Architecture
> Design a plugin system that allows easy addition of new validation methods and column filtering strategies.

### Config-Driven Validation
> Write a validation framework that is entirely driven by YAML configuration, including which columns to ignore and which validation methods to use.

---

## 🔍 Debugging and Diagnostics

### Validation Diff Reporter
> Create a function that shows exactly which columns and rows differ between source and target, excluding ignored columns.

### Schema Comparison Tool
> Write a utility that compares table schemas and clearly shows which differences are acceptable (ignored columns) vs. problematic.

### Hash Mismatch Analyzer
> Create a function that takes hash mismatches and helps identify the specific data differences, ignoring configured columns.

### Detailed Row Comparison
> Write a function that fetches and compares specific rows from source and target databases, logging the complete data and highlighting differences.

### Data Type Difference Detector
> Create a function that identifies data type mismatches between source and target rows, such as decimal precision differences or timestamp variations.

### Primary Key Tracker
> Write a function that logs primary key information for mismatched rows to make manual verification easier.

### Precision Mismatch Identifier
> Create a function that specifically identifies precision differences in numeric fields (e.g., Decimal('150.00') vs Decimal('150.0')).

---

## 🧠 Meta: Prompt Guide Generator

> Generate a Markdown prompt library for a Python-based data validation tool with sections like: connection, row check, hashing with column filtering, ETL integration, and clean reporting.

---
