from sqlalchemy import text, MetaData, Table, select
from typing import Dict, Any, List, Tuple
import hashlib
import json
import logging
from validators.base import BaseValidator

logger = logging.getLogger(__name__)

class HashValidator(BaseValidator):
    def __init__(self, source_engine, target_engine, config):
        super().__init__(source_engine, target_engine, config)
        self.chunk_size = config['validation']['chunk_size']

    def _get_primary_key(self, table_name: str, engine) -> List[str]:
        """
        Get the primary key columns for a table.
        
        Args:
            table_name (str): Name of the table
            engine: SQLAlchemy engine
            
        Returns:
            List[str]: List of primary key column names
        """
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)
        return [c.name for c in table.primary_key.columns]

    def _generate_row_hash(self, row: Tuple) -> str:
        """
        Generate a hash for a row of data.
        
        Args:
            row (Tuple): Row data
            
        Returns:
            str: MD5 hash of the row data
        """
        # Convert row to dictionary and sort keys for consistency
        row_dict = {str(i): str(val) for i, val in enumerate(row)}
        row_json = json.dumps(row_dict, sort_keys=True)
        return hashlib.md5(row_json.encode()).hexdigest()

    def _get_table_hashes(self, table_name: str, engine, pk_columns: List[str]) -> Dict[str, str]:
        """
        Get hashes for all rows in a table.
        
        Args:
            table_name (str): Name of the table
            engine: SQLAlchemy engine
            pk_columns (List[str]): List of primary key column names
            
        Returns:
            Dict[str, str]: Dictionary mapping primary key values to row hashes
        """
        hashes = {}
        offset = 0
        
        while True:
            # Get all columns for the table
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=engine)
            all_columns = [c.name for c in table.columns]
            
            # Build the query with all columns
            query = text(f"""
                SELECT {', '.join(all_columns)}
                FROM {table_name}
                ORDER BY {', '.join(pk_columns)}
                LIMIT {self.chunk_size}
                OFFSET {offset}
            """)
            
            with engine.connect() as conn:
                rows = conn.execute(query).fetchall()
                
            if not rows:
                break
                
            for row in rows:
                # Get primary key values by index
                pk_indices = [all_columns.index(col) for col in pk_columns]
                pk_values = tuple(str(row[i]) for i in pk_indices)
                row_hash = self._generate_row_hash(row)
                hashes[pk_values] = row_hash
                
            offset += self.chunk_size
            
        return hashes

    def validate_table(self, table_name: str) -> Dict[str, Any]:
        """
        Validate table data using hash comparison.
        
        Args:
            table_name (str): Name of the table to validate
            
        Returns:
            Dict[str, Any]: Validation results
        """
        try:
            # Get primary key columns
            source_pk = self._get_primary_key(table_name, self.source_engine)
            target_pk = self._get_primary_key(table_name, self.target_engine)
            
            if source_pk != target_pk:
                return {
                    "status": "error",
                    "error": f"Primary key mismatch: source={source_pk}, target={target_pk}"
                }
            
            # Get hashes for both tables
            logger.info(f"Generating hashes for table {table_name}")
            source_hashes = self._get_table_hashes(table_name, self.source_engine, source_pk)
            target_hashes = self._get_table_hashes(table_name, self.target_engine, target_pk)
            
            # Compare hashes
            mismatches = []
            for pk, source_hash in source_hashes.items():
                if pk not in target_hashes:
                    mismatches.append({
                        "primary_key": pk,
                        "status": "missing_in_target",
                        "source_hash": source_hash
                    })
                elif target_hashes[pk] != source_hash:
                    mismatches.append({
                        "primary_key": pk,
                        "status": "hash_mismatch",
                        "source_hash": source_hash,
                        "target_hash": target_hashes[pk]
                    })
            
            # Check for rows in target but not in source
            for pk in target_hashes:
                if pk not in source_hashes:
                    mismatches.append({
                        "primary_key": pk,
                        "status": "missing_in_source",
                        "target_hash": target_hashes[pk]
                    })
            
            result = {
                "status": "success" if not mismatches else "mismatch",
                "total_rows_source": len(source_hashes),
                "total_rows_target": len(target_hashes),
                "mismatches": mismatches
            }
            
            if mismatches:
                logger.warning(
                    f"Hash validation found {len(mismatches)} mismatches in table {table_name}"
                )
            else:
                logger.info(f"Hash validation successful for table {table_name}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in hash validation for table {table_name}: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            } 