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
        self.ignored_columns = config['validation'].get('ignored_columns', [])

    def _filter_columns(self, columns: List[str]) -> List[str]:
        """
        Filter out ignored columns from the column list.
        
        Args:
            columns (List[str]): Original column list
            
        Returns:
            List[str]: Filtered column list without ignored columns
        """
        return [col for col in columns if col not in self.ignored_columns]

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

    def _generate_row_hash(self, row: Tuple, filtered_columns: List[str]) -> str:
        """
        Generate a hash for a row of data using only the filtered columns.
        
        Args:
            row (Tuple): Row data
            filtered_columns (List[str]): Columns to include in hash
            
        Returns:
            str: MD5 hash of the filtered row data
        """
        # Create dictionary with column names as keys for better consistency
        row_dict = {col: str(val) for col, val in zip(filtered_columns, row)}
        row_json = json.dumps(row_dict, sort_keys=True)
        return hashlib.md5(row_json.encode()).hexdigest()

    def _get_table_hashes(self, table_name: str, engine, pk_columns: List[str], filtered_columns: List[str]) -> Dict[str, str]:
        """
        Get hashes for all rows in a table using only filtered columns.
        
        Args:
            table_name (str): Name of the table
            engine: SQLAlchemy engine
            pk_columns (List[str]): List of primary key column names
            filtered_columns (List[str]): Columns to include in hash calculation
            
        Returns:
            Dict[str, str]: Dictionary mapping primary key values to row hashes
        """
        hashes = {}
        offset = 0
        
        # Get all columns for the table
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)
        all_columns = [c.name for c in table.columns]
        
        while True:
            # Build the query with only filtered columns
            query = text(f"""
                SELECT {', '.join(filtered_columns)}
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
                # Get primary key values by finding their indices in filtered_columns
                pk_values = []
                for pk_col in pk_columns:
                    if pk_col in filtered_columns:
                        pk_index = filtered_columns.index(pk_col)
                        pk_values.append(str(row[pk_index]))
                pk_values = tuple(pk_values)
                
                row_hash = self._generate_row_hash(row, filtered_columns)
                hashes[pk_values] = row_hash
                
            offset += self.chunk_size
            
        return hashes

    def validate_table(self, table_name: str) -> Dict[str, Any]:
        """
        Validate table data using hash comparison, ignoring configured columns.
        
        Args:
            table_name (str): Name of the table to validate
            
        Returns:
            Dict[str, Any]: Validation results
        """
        try:
            # First, check if table structures match
            metadata_source = MetaData()
            table_source = Table(table_name, metadata_source, autoload_with=self.source_engine)
            source_columns = [c.name for c in table_source.columns]
            
            metadata_target = MetaData()
            table_target = Table(table_name, metadata_target, autoload_with=self.target_engine)
            target_columns = [c.name for c in table_target.columns]
            
            # Filter out ignored columns for comparison
            source_filtered = self._filter_columns(source_columns)
            target_filtered = self._filter_columns(target_columns)
            
            logger.info(f"Ignored columns for {table_name}: {[col for col in target_columns if col in self.ignored_columns]}")
            
            # Check for column differences (excluding ignored columns)
            if set(source_filtered) != set(target_filtered):
                missing_in_target = set(source_filtered) - set(target_filtered)
                extra_in_target = set(target_filtered) - set(source_filtered)
                
                error_msg = "Schema mismatch detected (excluding ignored columns):"
                if missing_in_target:
                    error_msg += f" Missing in target: {list(missing_in_target)}."
                if extra_in_target:
                    error_msg += f" Extra in target: {list(extra_in_target)}."
                
                return {
                    "status": "error",
                    "error": error_msg,
                    "source_columns": source_filtered,
                    "target_columns": target_filtered,
                    "ignored_columns": self.ignored_columns
                }
            
            # Check if column order is the same for filtered columns (affects hash generation)
            if source_filtered != target_filtered:
                return {
                    "status": "error", 
                    "error": f"Column order mismatch (excluding ignored): source={source_filtered}, target={target_filtered}"
                }
            
            # Get primary key columns
            source_pk = self._get_primary_key(table_name, self.source_engine)
            target_pk = self._get_primary_key(table_name, self.target_engine)
            
            if source_pk != target_pk:
                return {
                    "status": "error",
                    "error": f"Primary key mismatch: source={source_pk}, target={target_pk}"
                }
            
            # Ensure primary key columns are included in filtered columns
            final_columns = source_filtered.copy()
            for pk_col in source_pk:
                if pk_col not in final_columns:
                    final_columns.append(pk_col)
            
            # Get hashes for both tables using filtered columns
            logger.info(f"Generating hashes for table {table_name} using columns: {final_columns}")
            source_hashes = self._get_table_hashes(table_name, self.source_engine, source_pk, final_columns)
            target_hashes = self._get_table_hashes(table_name, self.target_engine, target_pk, final_columns)
            
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
                "mismatches": mismatches,
                "columns_used": final_columns,
                "ignored_columns": [col for col in target_columns if col in self.ignored_columns]
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