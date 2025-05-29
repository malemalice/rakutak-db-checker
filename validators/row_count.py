from sqlalchemy import text
from typing import Dict, Any
from loguru import logger
from validators.base import BaseValidator

class RowCountValidator(BaseValidator):
    def validate_table(self, table_name: str) -> Dict[str, Any]:
        """
        Validate row counts between source and target tables.
        
        Args:
            table_name (str): Name of the table to validate
            
        Returns:
            Dict[str, Any]: Validation results including row counts and status
        """
        try:
            # Get source row count
            with self.source_engine.connect() as conn:
                source_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            
            # Get target row count
            with self.target_engine.connect() as conn:
                target_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            
            # Calculate difference
            diff = source_count - target_count
            diff_percentage = (diff / source_count * 100) if source_count > 0 else 0
            
            result = {
                "status": "success" if diff == 0 else "mismatch",
                "source_count": source_count,
                "target_count": target_count,
                "difference": diff,
                "difference_percentage": round(diff_percentage, 2)
            }
            
            if diff != 0:
                logger.warning(
                    f"Row count mismatch in table {table_name}: "
                    f"source={source_count}, target={target_count}, "
                    f"diff={diff} ({diff_percentage:.2f}%)"
                )
            else:
                logger.info(f"Row count match in table {table_name}: {source_count} rows")
            
            return result
            
        except Exception as e:
            logger.error(f"Error validating row count for table {table_name}: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            } 