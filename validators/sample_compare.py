from sqlalchemy import text, MetaData, Table
from typing import Dict, Any, List, Tuple
import random
import logging
from validators.base import BaseValidator

logger = logging.getLogger(__name__)

class SampleValidator(BaseValidator):
    def __init__(self, source_engine, target_engine, config):
        super().__init__(source_engine, target_engine, config)

    def validate_table(self, table_name: str) -> Dict[str, Any]:
        """
        Validate table by checking if row counts match.
        
        Args:
            table_name (str): Name of the table to validate
            
        Returns:
            Dict[str, Any]: Validation results
        """
        try:
            # Get source row count
            with self.source_engine.connect() as conn:
                source_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            
            # Get target row count
            with self.target_engine.connect() as conn:
                target_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            
            # Compare counts
            counts_match = source_count == target_count
            
            result = {
                "status": "success" if counts_match else "mismatch",
                "source_count": source_count,
                "target_count": target_count,
                "counts_match": counts_match
            }
            
            if not counts_match:
                logger.warning(
                    f"Row count mismatch in table {table_name}: "
                    f"source={source_count}, target={target_count}"
                )
            else:
                logger.info(f"Row count match in table {table_name}: {source_count} rows")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in row count validation for table {table_name}: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            } 