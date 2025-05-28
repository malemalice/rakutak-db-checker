import yaml
import logging
from loguru import logger
import sys
from pathlib import Path
from typing import Dict, Any

from connectors.factory import create_db_engine
from server.health import start_server
from scheduler.executor import ValidationScheduler
from validators.factory import ValidatorFactory

def load_config() -> Dict[str, Any]:
    """
    Load configuration from settings.yaml
    """
    config_path = Path(__file__).parent / "config" / "settings.yaml"
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        sys.exit(1)

def setup_logging(config: Dict[str, Any]) -> None:
    """
    Setup logging configuration
    """
    log_config = config['logging']
    logger.remove()  # Remove default handler
    
    # Add file handler
    logger.add(
        log_config['file'],
        rotation=log_config['max_size'],
        retention=log_config['backup_count'],
        format=log_config['format'],
        level=log_config['level']
    )
    
    # Add console handler
    logger.add(
        sys.stderr,
        format=log_config['format'],
        level=log_config['level']
    )

def run_validation(source_engine, target_engine, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run the validation process using configured validators.
    
    Args:
        source_engine: Source database engine
        target_engine: Target database engine
        config (Dict[str, Any]): Configuration dictionary
        
    Returns:
        Dict[str, Any]: Validation results
    """
    try:
        # Create validators
        validators = ValidatorFactory.create_validators(source_engine, target_engine, config)
        
        if not validators:
            return {
                "status": "error",
                "error": "No validators configured"
            }
        
        # Run all validators
        results = {}
        for validator in validators:
            validator_name = validator.__class__.__name__
            logger.info(f"Running {validator_name}")
            results[validator_name] = validator.validate_all()
        
        return {
            "status": "success",
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }

def main():
    # Load configuration
    config = load_config()
    
    # Setup logging
    setup_logging(config)
    
    try:
        # Create database engines
        source_engine = create_db_engine(config['source_db'])
        target_engine = create_db_engine(config['target_db'])
        
        # Create validation function
        validation_func = lambda: run_validation(source_engine, target_engine, config)
        
        # Initialize scheduler
        scheduler = ValidationScheduler(
            interval_minutes=config['validation']['interval_minutes'],
            validation_func=validation_func
        )
        
        # Start scheduler
        scheduler.start()
        
        # Start health check server
        server_config = config['server']
        start_server(server_config['host'], server_config['port'])
        
    except Exception as e:
        logger.error(f"Application failed to start: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 