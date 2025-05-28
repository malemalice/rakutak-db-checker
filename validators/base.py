from abc import ABC, abstractmethod
from sqlalchemy.engine import Engine
from sqlalchemy import text
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class BaseValidator(ABC):
    def __init__(self, source_engine: Engine, target_engine: Engine, config: Dict[str, Any]):
        """
        Initialize the base validator.
        
        Args:
            source_engine (Engine): Source database engine
            target_engine (Engine): Target database engine
            config (Dict[str, Any]): Configuration dictionary
        """
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.config = config
        self.tables = self._get_tables_to_validate()

    def _get_tables_to_validate(self) -> List[str]:
        """
        Get the list of tables to validate based on configuration.
        
        Returns:
            List[str]: List of table names to validate
        """
        include_tables = self.config['tables']['include']
        exclude_tables = self.config['tables']['exclude']
        
        # If include_tables is empty, get all tables
        if not include_tables:
            with self.source_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """))
                tables = [row[0] for row in result]
        else:
            tables = include_tables
            
        # Apply exclusions
        tables = [t for t in tables if t not in exclude_tables]
        
        logger.info(f"Found {len(tables)} tables to validate")
        return tables

    @abstractmethod
    def validate_table(self, table_name: str) -> Dict[str, Any]:
        """
        Validate a single table.
        
        Args:
            table_name (str): Name of the table to validate
            
        Returns:
            Dict[str, Any]: Validation results
        """
        pass

    def validate_all(self) -> Dict[str, Any]:
        """
        Validate all configured tables.
        
        Returns:
            Dict[str, Any]: Validation results for all tables
        """
        results = {}
        for table in self.tables:
            try:
                logger.info(f"Validating table: {table}")
                results[table] = self.validate_table(table)
            except Exception as e:
                logger.error(f"Error validating table {table}: {str(e)}")
                results[table] = {
                    "status": "error",
                    "error": str(e)
                }
        return results 