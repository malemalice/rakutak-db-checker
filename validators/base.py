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
        validator_name = self.__class__.__name__
        
        print(f"\n🚀 Starting {validator_name} for {len(self.tables)} tables")
        
        for i, table in enumerate(self.tables, 1):
            try:
                print(f"[{i}/{len(self.tables)}] Validating table: {table}", end=" ... ")
                result = self.validate_table(table)
                results[table] = result
                
                # Print immediate result for this table
                status = result.get("status", "unknown")
                if status == "success":
                    print("✅ PASSED")
                elif status == "mismatch":
                    if validator_name == "SampleValidator":
                        source_count = result.get("source_count", 0)
                        target_count = result.get("target_count", 0)
                        print(f"❌ FAILED (source: {source_count}, target: {target_count})")
                    elif validator_name == "RowCountValidator":
                        source_count = result.get("source_count", 0)
                        target_count = result.get("target_count", 0)
                        diff = result.get("difference", 0)
                        print(f"❌ FAILED (Row count diff: {diff})")
                    elif validator_name == "HashValidator" and "mismatches" in result:
                        mismatch_count = len(result["mismatches"])
                        print(f"❌ FAILED ({mismatch_count} hash mismatches)")
                    else:
                        print("❌ FAILED")
                elif status == "error":
                    error_msg = result.get("error", "Unknown error")
                    print(f"⚠️  ERROR - {error_msg}")
                else:
                    print(f"❓ UNKNOWN STATUS - {status}")
                    
            except Exception as e:
                print(f"⚠️  ERROR - {str(e)}")
                results[table] = {
                    "status": "error",
                    "error": str(e)
                }
        
        print(f"✅ Completed {validator_name} validation")
        return results 