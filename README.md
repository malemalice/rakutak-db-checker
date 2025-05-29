# Database Validator

A Python-based tool for validating data between source and target databases during migration using DLT (dlthub).

## Features

- Automated validation of table structure and content
- Multiple validation methods:
  - Row count comparison
  - Hash-based validation
  - Sample data comparison
- Comprehensive logging
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

logging:
  level: INFO
  file: logs/validator.log
  max_size: 10MB
  backup_count: 5
  format: "{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
```

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

The application will:
- Load the configuration from the mounted config directory
- Connect to source and target databases
- Run the configured validations
- Log results to the mounted logs directory

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