from sqlalchemy import text, MetaData, Table, select
from typing import Dict, Any, List, Tuple
import hashlib
import json
import random
from loguru import logger
from validators.base import BaseValidator

class HashValidator(BaseValidator):
    def __init__(self, source_engine, target_engine, config):
        super().__init__(source_engine, target_engine, config)
        self.chunk_size = config['validation']['chunk_size']
        self.ignored_columns = config['validation'].get('ignored_columns', [])
        
        # Hash sampling configuration
        self.hash_sampling = config['validation'].get('hash_sampling', {})
        self.sampling_enabled = self.hash_sampling.get('enabled', False)
        self.max_rows_for_full_scan = self.hash_sampling.get('max_rows_for_full_scan', 100000)
        self.sample_size = self.hash_sampling.get('sample_size', 10000)
        self.sample_method = self.hash_sampling.get('sample_method', 'random')

    def _get_table_row_count(self, table_name: str, engine) -> int:
        """
        Get the total row count for a table.
        
        Args:
            table_name (str): Name of the table
            engine: SQLAlchemy engine
            
        Returns:
            int: Total number of rows in the table
        """
        query = text(f"SELECT COUNT(*) FROM {table_name}")
        with engine.connect() as conn:
            result = conn.execute(query).fetchone()
            return result[0] if result else 0

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

    def _get_table_hashes_with_sampling(self, table_name: str, engine, pk_columns: List[str], filtered_columns: List[str], total_rows: int) -> Dict[str, str]:
        """
        Get hashes for rows in a table using sampling for large tables.
        
        Args:
            table_name (str): Name of the table
            engine: SQLAlchemy engine
            pk_columns (List[str]): List of primary key column names
            filtered_columns (List[str]): Columns to include in hash calculation
            total_rows (int): Total number of rows in the table
            
        Returns:
            Dict[str, str]: Dictionary mapping primary key values to row hashes
        """
        use_sampling = (self.sampling_enabled and 
                       total_rows > self.max_rows_for_full_scan)
        
        if use_sampling:
            logger.info(f"Table {table_name} has {total_rows:,} rows - using random sampling ({self.sample_size:,} samples)")
            return self._get_sampled_table_hashes(table_name, engine, pk_columns, filtered_columns, total_rows)
        else:
            logger.info(f"Table {table_name} has {total_rows:,} rows - using full scan")
            return self._get_all_table_hashes(table_name, engine, pk_columns, filtered_columns)

    def _get_sampled_table_hashes(self, table_name: str, engine, pk_columns: List[str], filtered_columns: List[str], total_rows: int) -> Dict[str, str]:
        """
        Get hashes for a random sample of rows in a table.
        
        Args:
            table_name (str): Name of the table
            engine: SQLAlchemy engine
            pk_columns (List[str]): List of primary key column names
            filtered_columns (List[str]): Columns to include in hash calculation
            total_rows (int): Total number of rows in the table
            
        Returns:
            Dict[str, str]: Dictionary mapping primary key values to row hashes
        """
        db_type = self._get_database_type(engine)
        
        if db_type == 'mysql':
            # MySQL TABLESAMPLE is not widely supported, use ORDER BY RAND()
            query = text(f"""
                SELECT {', '.join(filtered_columns)}
                FROM {table_name}
                ORDER BY RAND()
                LIMIT {self.sample_size}
            """)
        elif db_type == 'postgresql':
            # PostgreSQL supports TABLESAMPLE
            sample_percent = min(100, (self.sample_size / total_rows) * 100)
            if sample_percent < 0.01:  # If percentage is too small, use LIMIT with ORDER BY RANDOM()
                query = text(f"""
                    SELECT {', '.join(filtered_columns)}
                    FROM {table_name}
                    ORDER BY RANDOM()
                    LIMIT {self.sample_size}
                """)
            else:
                query = text(f"""
                    SELECT {', '.join(filtered_columns)}
                    FROM {table_name} TABLESAMPLE BERNOULLI({sample_percent})
                    LIMIT {self.sample_size}
                """)
        else:
            # Fallback: use LIMIT with chunked scanning
            logger.warning(f"Database type {db_type} may not support efficient sampling, using chunked approach")
            return self._get_chunked_sample_hashes(table_name, engine, pk_columns, filtered_columns, total_rows)
        
        hashes = {}
        with engine.connect() as conn:
            rows = conn.execute(query).fetchall()
            
            for row in rows:
                # Get primary key values
                pk_values = []
                for pk_col in pk_columns:
                    if pk_col in filtered_columns:
                        pk_index = filtered_columns.index(pk_col)
                        pk_values.append(str(row[pk_index]))
                pk_values = tuple(pk_values)
                
                row_hash = self._generate_row_hash(row, filtered_columns)
                hashes[pk_values] = row_hash
        
        logger.info(f"Successfully sampled {len(hashes):,} rows from {table_name}")
        return hashes

    def _get_chunked_sample_hashes(self, table_name: str, engine, pk_columns: List[str], filtered_columns: List[str], total_rows: int) -> Dict[str, str]:
        """
        Get hashes using a chunked random sampling approach for databases that don't support native sampling.
        
        Args:
            table_name (str): Name of the table
            engine: SQLAlchemy engine
            pk_columns (List[str]): List of primary key column names
            filtered_columns (List[str]): Columns to include in hash calculation
            total_rows (int): Total number of rows in the table
            
        Returns:
            Dict[str, str]: Dictionary mapping primary key values to row hashes
        """
        hashes = {}
        
        # Calculate how many chunks we need to scan to get our sample
        chunk_sample_size = max(1, self.sample_size // 10)  # Sample from 10 chunks
        chunk_interval = max(1, total_rows // chunk_sample_size // 10)
        
        sampled_count = 0
        offset = 0
        
        while sampled_count < self.sample_size and offset < total_rows:
            # Add some randomness to the offset
            random_offset = offset + random.randint(0, chunk_interval - 1)
            if random_offset >= total_rows:
                break
                
            query = text(f"""
                SELECT {', '.join(filtered_columns)}
                FROM {table_name}
                ORDER BY {', '.join(pk_columns)}
                LIMIT {min(chunk_sample_size, self.sample_size - sampled_count)}
                OFFSET {random_offset}
            """)
            
            with engine.connect() as conn:
                rows = conn.execute(query).fetchall()
                
                for row in rows:
                    pk_values = []
                    for pk_col in pk_columns:
                        if pk_col in filtered_columns:
                            pk_index = filtered_columns.index(pk_col)
                            pk_values.append(str(row[pk_index]))
                    pk_values = tuple(pk_values)
                    
                    row_hash = self._generate_row_hash(row, filtered_columns)
                    hashes[pk_values] = row_hash
                    sampled_count += 1
                    
                    if sampled_count >= self.sample_size:
                        break
            
            offset += chunk_interval
        
        logger.info(f"Chunked sampling collected {len(hashes):,} rows from {table_name}")
        return hashes

    def _get_all_table_hashes(self, table_name: str, engine, pk_columns: List[str], filtered_columns: List[str]) -> Dict[str, str]:
        """
        Get hashes for all rows in a table (original implementation).
        
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
        
        while True:
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

    def _get_row_data_for_logging(self, table_name: str, engine, pk_columns: List[str], final_columns: List[str], pk_values: Tuple) -> Dict[str, Any]:
        """
        Get detailed row data for logging purposes.
        
        Args:
            table_name (str): Name of the table
            engine: SQLAlchemy engine
            pk_columns (List[str]): Primary key column names
            final_columns (List[str]): Columns to include
            pk_values (Tuple): Primary key values to fetch
            
        Returns:
            Dict[str, Any]: Row data as dictionary
        """
        try:
            # Build WHERE clause for primary key
            if len(pk_columns) == 1:
                where_clause = f"{pk_columns[0]} = '{pk_values[0]}'"
            else:
                conditions = []
                for i, col in enumerate(pk_columns):
                    conditions.append(f"{col} = '{pk_values[i]}'")
                where_clause = " AND ".join(conditions)
            
            query = text(f"SELECT {', '.join(final_columns)} FROM {table_name} WHERE {where_clause}")
            
            with engine.connect() as conn:
                row = conn.execute(query).fetchone()
                if row:
                    return {col: row[i] for i, col in enumerate(final_columns)}
                return {}
        except Exception as e:
            logger.error(f"Error fetching row data for logging: {str(e)}")
            return {}

    def _log_detailed_mismatch(self, table_name: str, pk_values: Tuple, source_pk: List[str], final_columns: List[str], mismatch_count: int):
        """
        Log detailed information about hash mismatches.
        
        Args:
            table_name (str): Name of the table
            pk_values (Tuple): Primary key values of mismatched row
            source_pk (List[str]): Primary key column names
            final_columns (List[str]): Columns used for hashing
            mismatch_count (int): Current mismatch count for progress
        """
        logger.info(f"=== HASH MISMATCH #{mismatch_count} IN TABLE '{table_name}' ===")
        logger.info(f"Primary key: {dict(zip(source_pk, pk_values))}")
        
        # Get detailed row data from both databases
        source_row = self._get_row_data_for_logging(table_name, self.source_engine, source_pk, final_columns, pk_values)
        target_row = self._get_row_data_for_logging(table_name, self.target_engine, source_pk, final_columns, pk_values)
        
        if source_row and target_row:
            logger.info("SOURCE ROW DATA:")
            for col in final_columns:
                source_val = source_row.get(col, 'NULL')
                logger.info(f"  {col}: {repr(source_val)}")
            
            logger.info("TARGET ROW DATA:")
            for col in final_columns:
                target_val = target_row.get(col, 'NULL')
                logger.info(f"  {col}: {repr(target_val)}")
            
            # Show differences
            differences = []
            for col in final_columns:
                source_val = source_row.get(col)
                target_val = target_row.get(col)
                if source_val != target_val:
                    differences.append(f"{col}: '{source_val}' != '{target_val}'")
            
            if differences:
                logger.info("DIFFERENCES FOUND:")
                for diff in differences:
                    logger.info(f"  - {diff}")
            else:
                logger.warning("No obvious differences found (possible data type or encoding issue)")
        else:
            logger.error("Could not retrieve row data for detailed comparison")
        
        logger.info("=" * 60)

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
            source_row_count = self._get_table_row_count(table_name, self.source_engine)
            target_row_count = self._get_table_row_count(table_name, self.target_engine)
            
            source_hashes = self._get_table_hashes_with_sampling(table_name, self.source_engine, source_pk, final_columns, source_row_count)
            target_hashes = self._get_table_hashes_with_sampling(table_name, self.target_engine, target_pk, final_columns, target_row_count)
            
            # Determine if sampling was used
            sampling_used = (self.sampling_enabled and 
                           (source_row_count > self.max_rows_for_full_scan or 
                            target_row_count > self.max_rows_for_full_scan))
            
            # Compare hashes
            mismatches = []
            mismatch_count = 0
            
            for pk, source_hash in source_hashes.items():
                if pk not in target_hashes:
                    mismatches.append({
                        "primary_key": pk,
                        "status": "missing_in_target",
                        "source_hash": source_hash
                    })
                    logger.warning(f"Row missing in target - PK: {dict(zip(source_pk, pk))}")
                elif target_hashes[pk] != source_hash:
                    mismatch_count += 1
                    mismatches.append({
                        "primary_key": pk,
                        "status": "hash_mismatch",
                        "source_hash": source_hash,
                        "target_hash": target_hashes[pk]
                    })
                    # Log detailed mismatch information
                    self._log_detailed_mismatch(table_name, pk, source_pk, final_columns, mismatch_count)
            
            # Check for rows in target but not in source
            for pk in target_hashes:
                if pk not in source_hashes:
                    mismatches.append({
                        "primary_key": pk,
                        "status": "missing_in_source",
                        "target_hash": target_hashes[pk]
                    })
                    logger.warning(f"Row missing in source - PK: {dict(zip(source_pk, pk))}")
            
            result = {
                "status": "success" if not mismatches else "mismatch",
                "total_rows_source": source_row_count,
                "total_rows_target": target_row_count,
                "sampled_rows_source": len(source_hashes),
                "sampled_rows_target": len(target_hashes),
                "sampling_used": sampling_used,
                "sample_size": self.sample_size if sampling_used else None,
                "mismatches": mismatches,
                "columns_used": final_columns,
                "ignored_columns": [col for col in target_columns if col in self.ignored_columns]
            }
            
            if mismatches:
                if sampling_used:
                    logger.warning(
                        f"Hash validation found {len(mismatches)} mismatches in {len(source_hashes):,} sampled rows from table {table_name} "
                        f"(total rows: source={source_row_count:,}, target={target_row_count:,})"
                    )
                else:
                    logger.warning(
                        f"Hash validation found {len(mismatches)} mismatches in table {table_name}"
                    )
                # Add instruction for detailed logs
                if mismatch_count > 0:  # Only show instruction if there were hash mismatches (not just missing rows)
                    logger.info(f"📋 For detailed row-by-row comparison of mismatched data, check: logs/data_checker.log")
            else:
                if sampling_used:
                    logger.info(f"Hash validation successful for table {table_name} - {len(source_hashes):,} sampled rows matched")
                else:
                    logger.info(f"Hash validation successful for table {table_name}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in hash validation for table {table_name}: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            } 