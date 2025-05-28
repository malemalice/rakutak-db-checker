# Database Validator

A Python-based tool for validating data between source and target databases during migration using DLT (dlthub).

## Features

- Automated validation of table structure and content
- Multiple validation methods:
  - Row count comparison
  - Hash-based validation
  - Sample data comparison
- Configurable execution intervals
- HTTP health check endpoint
- Comprehensive logging
- Support for PostgreSQL and MySQL databases

## Requirements

- Python 3.8 or newer
- PostgreSQL or MySQL database

## Installation

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

## Configuration

1. Edit `data_checker/config/settings.yaml` to configure:
   - Database connections
   - Validation settings
   - Table mappings
   - Server settings
   - Logging configuration

## Usage

1. Start the validator:
```bash
python -m data_checker.main
```

2. The application will:
   - Start the health check server (default: http://localhost:8000)
   - Begin scheduled validations based on the configured interval
   - Log all activities to the configured log file

3. Check the health status:
```bash
curl http://localhost:8000/health
```

## Health Check Endpoint

The health check endpoint (`/health`) provides:
- Current service status
- Last execution details
- Timestamp of the last check

## Logging

Logs are written to:
- Console (stdout)
- File (configured in settings.yaml)
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
├── server/         # Health check server
├── scheduler/      # Interval-based execution
└── logs/           # Log files
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Your chosen license] 