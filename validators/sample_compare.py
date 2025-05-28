from sqlalchemy import text, MetaData, Table
from typing import Dict, Any, List, Tuple
import random
import logging
from validators.base import BaseValidator

logger = logging.getLogger(__name__)

class SampleValidator(BaseValidator):
    def __init__(self, source_engine, target_engine, config):
        super().__init__(source_engine, target_engine, config)
        self.sample_size = config['validation']['sample_size']

    def _get_table_columns(self, table_name: str, engine) -> List[str]:
        """
        Get all column names for a table.
        
        Args:
            table_name (str): Name of the table
            engine: SQLAlchemy engine
            
        Returns:
            List[str]: List of column names
        """
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)
        return [c.name for c in table.columns]

    def _get_random_rows(self, table_name: str, engine, columns: List[str], count: int) -> List[Tuple]:
        """
        Get random rows from a table.
        
        Args:
            table_name (str): Name of the table
            engine: SQLAlchemy engine
            columns (List[str]): List of column names
            count (int): Number of rows to sample
            
        Returns:
            List[Tuple]: List of sampled rows
        """
        # First, get the total row count
        with engine.connect() as conn:
            total_rows = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
        
        if total_rows == 0:
            return []
        
        # Generate random offsets
        offsets = random.sample(range(total_rows), min(count, total_rows))
        
        # Get rows at random offsets
        rows = []
        for offset in offsets:
            query = text(f"""
                SELECT {', '.join(columns)}
                FROM {table_name}
                OFFSET {offset}
                LIMIT 1
            """)
            
            with engine.connect() as conn:
                row = conn.execute(query).fetchone()
                if row:
                    rows.append(row)
        
        return rows

    def _compare_rows(self, source_row: Tuple, target_row: Tuple, columns: List[str]) -> List[Dict[str, Any]]:
        """
        Compare two rows and return differences.
        
        Args:
            source_row (Tuple): Source row data
            target_row (Tuple): Target row data
            columns (List[str]): List of column names
            
        Returns:
            List[Dict[str, Any]]: List of differences found
        """
        differences = []
        for i, col in enumerate(columns):
            source_val = source_row[i]
            target_val = target_row[i]
            
            if source_val != target_val:
                differences.append({
                    "column": col,
                    "source_value": str(source_val),
                    "target_value": str(target_val)
                })
        
        return differences

    def validate_table(self, table_name: str) -> Dict[str, Any]:
        """
        Validate table data using random sampling.
        
        Args:
            table_name (str): Name of the table to validate
            
        Returns:
            Dict[str, Any]: Validation results
        """
        try:
            # Get column information
            source_columns = self._get_table_columns(table_name, self.source_engine)
            target_columns = self._get_table_columns(table_name, self.target_engine)
            
            if source_columns != target_columns:
                return {
                    "status": "error",
                    "error": f"Column mismatch: source={source_columns}, target={target_columns}"
                }
            
            # Get random samples
            logger.info(f"Sampling {self.sample_size} rows from table {table_name}")
            source_rows = self._get_random_rows(table_name, self.source_engine, source_columns, self.sample_size)
            target_rows = self._get_random_rows(table_name, self.target_engine, target_columns, self.sample_size)
            
            # Compare samples
            mismatches = []
            for i, (source_row, target_row) in enumerate(zip(source_rows, target_rows)):
                differences = self._compare_rows(source_row, target_row, source_columns)
                if differences:
                    mismatches.append({
                        "row_index": i,
                        "differences": differences
                    })
            
            result = {
                "status": "success" if not mismatches else "mismatch",
                "sample_size": len(source_rows),
                "mismatches": mismatches
            }
            
            if mismatches:
                logger.warning(
                    f"Sample validation found {len(mismatches)} mismatches in table {table_name}"
                )
            else:
                logger.info(f"Sample validation successful for table {table_name}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in sample validation for table {table_name}: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            } 