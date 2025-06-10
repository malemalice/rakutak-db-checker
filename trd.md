# Technical Requirements Document (TRD)

## Project Title: AI-Extensible Python Tool for Data Validation in DLT-Based Database Migration

---

## 1. Technical Overview

### 1.1 System Architecture
The system follows a modular, layered architecture designed for extensibility and AI code generation compatibility:

- **Configuration Layer**: YAML-based configuration management
- **Database Connectivity Layer**: SQLAlchemy-based database adapters
- **Validation Engine Layer**: Pluggable validation algorithms
- **Reporting Layer**: Multi-format output generation
- **Utilities Layer**: Shared functionality and helpers

### 1.2 Technology Stack

#### Core Technologies
- **Language**: Python 3.8+
- **Database ORM**: SQLAlchemy 2.0+
- **Configuration**: PyYAML
- **Logging**: Loguru
- **CLI Framework**: Click 8.0+
- **Hashing**: hashlib (MD5/SHA256)
- **Environment Management**: python-dotenv

#### Database Drivers
- **PostgreSQL**: psycopg2-binary
- **MySQL/MariaDB**: PyMySQL or mysql-connector-python
- **SQLite**: sqlite3 (built-in, for testing)

#### Development & Deployment
- **Containerization**: Docker & Docker Compose
- **Dependency Management**: pip + requirements.txt
- **Code Quality**: Black, isort, flake8
- **Testing**: pytest + pytest-cov

---

## 2. System Architecture

### 2.1 Component Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                     CLI Entry Point                        │
│                      (main.py)                             │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                Configuration Manager                        │
│                 (config/settings.py)                       │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                Database Factory                             │
│               (connectors/factory.py)                      │
└──────────┬──────────────────────────────────────────┬──────┘
           │                                          │
┌──────────▼──────────┐                    ┌──────────▼──────────┐
│   Source Connector  │                    │   Target Connector  │
│  (connectors/*.py)  │                    │  (connectors/*.py)  │
└──────────┬──────────┘                    └──────────┬──────────┘
           │                                          │
           └──────────┬───────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                Validation Engine                            │
│                (validators/*.py)                           │
├─────────────────────────────────────────────────────────────┤
│  • RowCountValidator                                        │
│  • HashValidator                                            │
│  • SampleValidator                                          │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                Report Generator                             │
│               (reports/generator.py)                       │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Data Flow Architecture

```
Configuration File → Configuration Manager → Database Factory
                                                    ↓
Source DB ←→ Source Connector ←→ Validation Engine ←→ Target Connector ←→ Target DB
                                        ↓
                            Validation Results
                                        ↓
                              Report Generator
                                        ↓
                         Console Output + Log Files
```

---

## 3. Technical Specifications

### 3.1 Database Connectivity

#### 3.1.1 Connection Management
```python
# SQLAlchemy connection pooling configuration
ENGINE_CONFIG = {
    'pool_size': 10,
    'max_overflow': 20,
    'pool_timeout': 30,
    'pool_recycle': 3600,
    'pool_pre_ping': True
}
```

#### 3.1.2 Connection String Format
```python
# PostgreSQL
postgresql://username:password@host:port/database

# MySQL
mysql+pymysql://username:password@host:port/database

# Future: BigQuery
bigquery://project_id/dataset_id
```

#### 3.1.3 Connection Interface
```python
from abc import ABC, abstractmethod
from sqlalchemy import Engine
from typing import List, Dict, Any

class DatabaseConnector(ABC):
    @abstractmethod
    def get_engine(self) -> Engine:
        """Return SQLAlchemy engine instance"""
        pass
    
    @abstractmethod
    def get_table_names(self) -> List[str]:
        """Return list of table names"""
        pass
    
    @abstractmethod
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Return table schema information"""
        pass
    
    @abstractmethod
    def execute_query(self, query: str) -> List[Dict]:
        """Execute raw SQL query"""
        pass
```

### 3.1.4 Reserved Keyword Handling
```python
# Automatic escaping of reserved keywords
from utils.sql_utils import escape_column_name, build_select_query

# MySQL/MariaDB: backticks for reserved words
escape_column_name("order", "mysql")  # Returns: `order`

# PostgreSQL: double quotes for reserved words  
escape_column_name("order", "postgresql")  # Returns: "order"

# Safe query building
query = build_select_query(
    columns=["id", "order", "type", "status"],
    table_name="program_galleries", 
    db_type="mysql",
    order_by=["id"],
    limit=10000
)
# Result: SELECT id, `order`, `type`, `status` FROM program_galleries ORDER BY id LIMIT 10000
```

### 3.2 Validation Algorithms

#### 3.2.1 Row Count Validation
- **Algorithm**: Direct COUNT(*) queries
- **Performance**: O(1) per table (database optimized)
- **Memory**: Minimal (single integer per table)
- **Error Handling**: Connection timeout, table access permissions

#### 3.2.2 Hash-Based Validation
- **Algorithm**: MD5/SHA256 per row with column filtering
- **Chunking**: Process tables in configurable chunks (default: 10,000 rows)
- **Memory**: O(chunk_size) rows in memory
- **Column Filtering**: Remove ignored columns before hashing
- **Primary Key Strategy**: Use composite primary keys for row matching

```python
# Hash generation pseudocode
def generate_row_hash(row_data: Dict, ignored_columns: List[str]) -> str:
    filtered_data = {k: v for k, v in row_data.items() if k not in ignored_columns}
    sorted_values = [str(filtered_data[k]) for k in sorted(filtered_data.keys())]
    return hashlib.md5('|'.join(sorted_values).encode()).hexdigest()
```

#### 3.2.3 Sample Validation
- **Algorithm**: Statistical sampling with configurable size
- **Sampling Method**: TABLESAMPLE SYSTEM (percentage-based)
- **Fallback**: Random ORDER BY for databases without TABLESAMPLE
- **Performance**: O(sample_size) rather than O(table_size)

### 3.3 Configuration Management

#### 3.3.1 Configuration Schema
```yaml
# settings.yaml structure
source_db:
  type: str  # postgresql, mysql
  host: str
  port: int
  user: str
  password: str  # or env var reference
  database: str
  
target_db:
  # Same structure as source_db

validation:
  types: List[str]  # [row_count, hash_check, sample_comparison]
  sample_size: int  # Default: 1000
  chunk_size: int   # Default: 10000
  hash_algorithm: str  # md5, sha256
  ignored_columns: List[str]  # Global ignored columns
  table_specific_ignored_columns: Dict[str, List[str]]  # Per-table overrides

tables:
  include: List[str]  # Empty means all
  exclude: List[str]
  name_mapping: Dict[str, str]  # source_table: target_table

logging:
  level: str  # DEBUG, INFO, WARNING, ERROR
  file: str   # Log file path
  format: str # Log format string
  max_size: str  # Log rotation size
  backup_count: int  # Number of backup files
```

#### 3.3.2 Environment Variable Support
```python
# Support for environment variable substitution
database_password: ${DB_PASSWORD}
api_key: ${API_KEY:-default_value}
```

---

## 4. Implementation Guidelines

### 4.1 Code Structure Standards

#### 4.1.1 File Organization
```
data_checker/
├── __init__.py
├── main.py                    # CLI entry point
├── config/
│   ├── __init__.py
│   ├── settings.py            # Configuration loader
│   └── schema.py              # Configuration validation
├── connectors/
│   ├── __init__.py
│   ├── base.py                # Abstract base connector
│   ├── factory.py             # Connector factory
│   ├── postgres.py            # PostgreSQL implementation
│   └── mysql.py               # MySQL implementation
├── validators/
│   ├── __init__.py
│   ├── base.py                # Abstract validator
│   ├── row_count.py           # Row count validator
│   ├── hash_compare.py        # Hash-based validator
│   └── sample_compare.py      # Sample validator
├── reports/
│   ├── __init__.py
│   ├── generator.py           # Report generation
│   └── formatters.py          # Output formatters
├── utils/
│   ├── __init__.py
│   ├── hashing.py             # Hash utilities
│   ├── table_utils.py         # Table operations
│   └── logging_config.py      # Logging setup
└── tests/
    ├── __init__.py
    ├── conftest.py            # Pytest configuration
    ├── test_connectors/
    ├── test_validators/
    └── test_utils/
```

#### 4.1.2 Naming Conventions
- **Classes**: PascalCase (e.g., `DatabaseConnector`, `HashValidator`)
- **Functions/Methods**: snake_case (e.g., `get_table_names`, `validate_row_count`)
- **Constants**: UPPER_SNAKE_CASE (e.g., `DEFAULT_CHUNK_SIZE`, `MAX_RETRIES`)
- **Files/Modules**: snake_case (e.g., `hash_compare.py`, `table_utils.py`)

#### 4.1.3 Documentation Standards
```python
def validate_table_hash(
    source_conn: DatabaseConnector,
    target_conn: DatabaseConnector,
    table_name: str,
    ignored_columns: List[str],
    chunk_size: int = 10000
) -> ValidationResult:
    """
    Perform hash-based validation of a table between source and target databases.
    
    This function compares row-level hashes after filtering out ignored columns,
    making it suitable for ETL scenarios where metadata columns are added.
    
    Args:
        source_conn: Database connector for source database
        target_conn: Database connector for target database  
        table_name: Name of table to validate
        ignored_columns: List of column names to exclude from hash calculation
        chunk_size: Number of rows to process in each chunk
        
    Returns:
        ValidationResult containing match status, mismatch details, and statistics
        
    Raises:
        DatabaseConnectionError: If connection to either database fails
        TableNotFoundError: If table doesn't exist in either database
        
    Example:
        >>> result = validate_table_hash(src_conn, tgt_conn, "users", ["_dlt_id"])
        >>> print(f"Validation passed: {result.is_valid}")
    """
```

### 4.2 AI-Extensibility Guidelines

#### 4.2.1 Function Decomposition
- **Single Responsibility**: Each function performs one specific task
- **Clear Interfaces**: Well-defined input/output types
- **Minimal Dependencies**: Reduce coupling between modules
- **Pure Functions**: Prefer functions without side effects where possible

#### 4.2.2 Extension Points
```python
# Plugin-style architecture for new validators
class ValidatorRegistry:
    _validators = {}
    
    @classmethod
    def register(cls, name: str, validator_class: Type[BaseValidator]):
        cls._validators[name] = validator_class
    
    @classmethod
    def get_validator(cls, name: str) -> BaseValidator:
        return cls._validators[name]()

# Usage for AI code generation
@ValidatorRegistry.register("custom_validator")
class CustomValidator(BaseValidator):
    def validate(self, source_conn, target_conn, config) -> ValidationResult:
        # Custom validation logic here
        pass
```

#### 4.2.3 Configuration-Driven Behavior
```python
# Enable new features through configuration
validation:
  types:
    - row_count
    - hash_check
    - custom_validator  # AI can add new validator types
  custom_validator:
    parameter1: value1
    parameter2: value2
```

---

## 5. Data Models

### 5.1 Configuration Models
```python
from dataclasses import dataclass
from typing import List, Dict, Optional

@dataclass
class DatabaseConfig:
    type: str
    host: str
    port: int
    user: str
    password: str
    database: str

@dataclass
class ValidationConfig:
    types: List[str]
    sample_size: int = 1000
    chunk_size: int = 10000
    hash_algorithm: str = "md5"
    ignored_columns: List[str] = None
    table_specific_ignored_columns: Dict[str, List[str]] = None

@dataclass
class AppConfig:
    source_db: DatabaseConfig
    target_db: DatabaseConfig
    validation: ValidationConfig
    tables: Dict = None
    logging: Dict = None
```

### 5.2 Validation Result Models
```python
from enum import Enum
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

class ValidationStatus(Enum):
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"

@dataclass
class TableValidationResult:
    table_name: str
    validation_type: str
    status: ValidationStatus
    source_count: Optional[int] = None
    target_count: Optional[int] = None
    mismatched_rows: int = 0
    error_message: Optional[str] = None
    ignored_columns: List[str] = None
    execution_time: float = 0.0
    
@dataclass
class ValidationSummary:
    total_tables: int
    passed_tables: int
    failed_tables: int
    error_tables: int
    skipped_tables: int
    total_execution_time: float
    results: List[TableValidationResult]
```

---

## 6. Performance & Security Requirements

### 6.1 Performance Requirements
- **Memory Usage**: Maximum 1GB for processing tables up to 100M rows
- **Processing Speed**: 10K-100K rows/second for hash validation
- **Scalability**: Support 1000+ tables, 100M+ rows per table
- **Concurrent Connections**: 5-10 simultaneous database connections

### 6.2 Security Requirements
- **Credential Management**: Environment variable support, no plaintext passwords
- **Data Privacy**: In-memory processing only, no data storage
- **Network Security**: TLS 1.2+ encryption, proxy support
- **Audit Trail**: Comprehensive logging without sensitive data exposure

### 6.3 Error Handling & Resilience
- **Retry Strategy**: Exponential backoff (1s, 2s, 4s, 8s)
- **Circuit Breaker**: Fail fast after consecutive failures
- **Memory Management**: Configurable chunk sizes, garbage collection
- **Graceful Degradation**: Continue validation when individual tables fail
- **Reserved Keywords**: Automatic escaping of SQL reserved keywords (backticks for MySQL, quotes for PostgreSQL)
- **SQL Injection Prevention**: Parameterized queries and proper escaping

---

## 7. Deployment & Testing

### 7.1 Docker Configuration
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENTRYPOINT ["python", "main.py"]
```

### 7.2 Testing Strategy
- **Unit Testing**: >90% code coverage, SQLite in-memory for tests
- **Integration Testing**: Real PostgreSQL/MySQL instances via Docker
- **End-to-End Testing**: Complete validation workflows
- **Performance Testing**: Large dataset validation scenarios

### 7.3 Monitoring & Observability
```python
# Structured logging with loguru
from loguru import logger

logger.add("logs/data_checker.log", 
          format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name} | {message}",
          level="INFO", rotation="10 MB", retention="30 days")
```

---

## 8. Future Extensions

### 8.1 Database Support Roadmap
- **Phase 1**: PostgreSQL, MySQL/MariaDB
- **Phase 2**: SQLite, SQL Server  
- **Phase 3**: BigQuery, Snowflake, Redshift
- **Phase 4**: MongoDB, Cassandra (NoSQL)

### 8.2 Advanced Features
- **Statistical Validation**: Distribution comparison, outlier detection
- **Schema Evolution Tracking**: Monitor schema changes over time
- **Web Interface**: Dashboard for real-time validation status
- **API Endpoints**: RESTful API for programmatic access
- **CI/CD Integration**: Jenkins, GitHub Actions, GitLab CI plugins

---

## 9. Development Environment

### 9.1 Development Setup
```bash
# Virtual environment setup
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements-dev.txt

# Pre-commit hooks
pre-commit install

# Run tests
pytest --cov=data_checker tests/
```

### 9.2 Code Quality Standards
- **Formatting**: Black with line length 88
- **Import Sorting**: isort with Black compatibility
- **Linting**: flake8 with complexity limit 10
- **Type Checking**: mypy strict mode
- **Pre-commit**: Automated checks before commit

---

This Technical Requirements Document provides comprehensive technical specifications for implementing the AI-extensible data validation tool described in the PRD. The architecture emphasizes modularity, extensibility, and maintainability while meeting all functional and performance requirements.