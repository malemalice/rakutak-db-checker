from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

def create_db_engine(config: Dict[str, Any]) -> Engine:
    """
    Create a SQLAlchemy engine based on the provided configuration.
    
    Args:
        config (Dict[str, Any]): Database configuration dictionary containing:
            - type: Database type (postgresql or mysql)
            - host: Database host
            - port: Database port
            - user: Database user
            - password: Database password
            - database: Database name
    
    Returns:
        Engine: SQLAlchemy engine instance
    
    Raises:
        ValueError: If database type is not supported
    """
    db_type = config.get('type', '').lower()
    
    if db_type == 'postgresql':
        connection_string = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    elif db_type == 'mysql':
        connection_string = f"mysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    try:
        engine = create_engine(connection_string)
        # Test the connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info(f"Successfully connected to {db_type} database")
        return engine
    except Exception as e:
        logger.error(f"Failed to create database engine: {str(e)}")
        raise 