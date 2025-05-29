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

---

## 🎲 Sample Data Comparison

> Write a Python function that randomly samples N rows from a table using SQLAlchemy and compares field values between two databases. Return any mismatched rows.

---

## 📊 Reporting

### JSON Report
> Generate a function that takes a dictionary of validation results and writes it to a JSON file.

### Markdown Report
> Write a function that formats a dictionary of validation results into a Markdown table with summary at the top.

---

## ⚙️ CLI Entry Point

> Create a Python CLI using `argparse` that accepts source and target DB configs, a validation mode (row_count, hash, sample), and a table name or `--all` flag.

---

## 🧪 Test Stubs

> Write a Pytest unit test for `compare_row_counts()` with mocked SQLAlchemy engines returning known row counts.

---

## 📂 Configuration Loader

> Write a function to load a `settings.yaml` file and return a dictionary of config values for DB connections and validation settings.

---

## 📝 Logging

### Structured Logger
> Create a logging configuration that writes to both console and file with rotation.

### Log Formatter
> Write a custom log formatter that includes timestamp, level, and context information.

---

## 🐳 Docker Support

### Dockerfile
> Create a Dockerfile that sets up the Python environment and installs dependencies.

### Docker Compose
> Write a docker-compose.yml file that includes the validator service and any required dependencies.

---

## 🧠 Meta: Prompt Guide Generator

> Generate a Markdown prompt library for a Python-based data validation tool with sections like: connection, row check, hashing, reporting, and CLI.

---
