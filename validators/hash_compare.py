from sqlalchemy import text, MetaData, Table, select
from typing import Dict, Any, List, Tuple
import hashlib
import json
import random
from loguru import logger
from validators.base import BaseValidator
from utils.sql_utils import (
    get_database_type, build_select_query, build_where_clause_for_pk, escape_column_name,
    get_suitable_row_identifier, create_row_signature, generate_update_query, generate_insert_query
)

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
        
        # Detailed mismatch logging limit
        self.max_detailed_mismatches = config['validation'].get('max_detailed_mismatches', 20)
        
        # Fix query generation
        self.generate_fix_queries = config['validation'].get('generate_fix_queries', True)
        self.fix_queries_file = config['validation'].get('fix_queries_file', 'logs/fix-query.sql')
        self.max_fix_queries = config['validation'].get('max_fix_queries', None)  # None = unlimited

    def _get_table_row_count(self, table_name: str, engine) -> int:
        """
        Get the total row count for a table.
        
        Args:
            table_name (str): Name of the table
            engine: SQLAlchemy engine
            
        Returns:
            int: Total number of rows in the table
        """
        db_type = get_database_type(engine)
        escaped_table = escape_column_name(table_name, db_type)
        query = text(f"SELECT COUNT(*) FROM {escaped_table}")
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

    def _get_row_identifier(self, table_name: str, engine, available_columns: List[str] = None) -> Tuple[List[str], str]:
        """
        Get the best available row identifier for a table.
        
        Args:
            table_name (str): Name of the table
            engine: SQLAlchemy engine
            available_columns: List of available columns to choose from
            
        Returns:
            Tuple[List[str], str]: (identifier_columns, identifier_type)
        """
        return get_suitable_row_identifier(table_name, engine, available_columns)

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
        # Use a more robust conversion that handles None, decimals, dates, etc.
        row_dict = {}
        for col, val in zip(filtered_columns, row):
            if val is None:
                row_dict[col] = None
            elif isinstance(val, (int, float, str, bool)):
                row_dict[col] = val
            else:
                # Handle decimals, dates, and other complex types by converting to string
                row_dict[col] = str(val)
        
        # Sort keys and create a consistent string representation
        sorted_items = sorted(row_dict.items())
        # Use a more deterministic serialization than JSON (which can vary)
        hash_string = '|'.join([f"{k}:{repr(v)}" for k, v in sorted_items])
        return hashlib.md5(hash_string.encode('utf-8')).hexdigest()

    def _get_table_hashes_with_sampling(self, table_name: str, engine, identifier_columns: List[str], filtered_columns: List[str], total_rows: int, identifier_type: str) -> Dict[str, str]:
        """
        Get hashes for rows in a table using sampling for large tables.
        
        Args:
            table_name (str): Name of the table
            engine: SQLAlchemy engine
            identifier_columns (List[str]): List of row identifier column names
            filtered_columns (List[str]): Columns to include in hash calculation
            total_rows (int): Total number of rows in the table
            identifier_type (str): Type of identifier (primary_key, unique_constraint, all_columns)
            
        Returns:
            Dict[str, str]: Dictionary mapping row identifiers to row hashes
        """
        use_sampling = (self.sampling_enabled and 
                       total_rows > self.max_rows_for_full_scan)
        
        if use_sampling:
            logger.info(f"Table {table_name} has {total_rows:,} rows - using random sampling ({self.sample_size:,} samples)")
            return self._get_sampled_table_hashes(table_name, engine, identifier_columns, filtered_columns, total_rows, identifier_type)
        else:
            logger.info(f"Table {table_name} has {total_rows:,} rows - using full scan")
            return self._get_all_table_hashes(table_name, engine, identifier_columns, filtered_columns, identifier_type)

    def _get_sampled_table_hashes(self, table_name: str, engine, identifier_columns: List[str], filtered_columns: List[str], total_rows: int, identifier_type: str) -> Dict[str, str]:
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
        db_type = get_database_type(engine)
        
        if db_type == 'mysql':
            # MySQL TABLESAMPLE is not widely supported, use ORDER BY RAND()
            # RAND() is a function, not a column, so we build the query manually
            from utils.sql_utils import escape_column_list, escape_column_name
            escaped_columns = escape_column_list(filtered_columns, db_type)
            escaped_table = escape_column_name(table_name, db_type)
            query = text(f"""
                SELECT {', '.join(escaped_columns)}
                FROM {escaped_table}
                ORDER BY RAND()
                LIMIT {self.sample_size}
            """)
        elif db_type == 'postgresql':
            # PostgreSQL supports TABLESAMPLE
            sample_percent = min(100, (self.sample_size / total_rows) * 100)
            if sample_percent < 0.01:  # If percentage is too small, use LIMIT with ORDER BY RANDOM()
                # RANDOM() is a function, not a column, so we build the query manually
                from utils.sql_utils import escape_column_list, escape_column_name
                escaped_columns = escape_column_list(filtered_columns, db_type)
                escaped_table = escape_column_name(table_name, db_type)
                query = text(f"""
                    SELECT {', '.join(escaped_columns)}
                    FROM {escaped_table}
                    ORDER BY RANDOM()
                    LIMIT {self.sample_size}
                """)
            else:
                # For TABLESAMPLE, we need to build manually since it's not a standard ORDER BY
                from utils.sql_utils import escape_column_list, escape_column_name
                escaped_columns = escape_column_list(filtered_columns, db_type)
                escaped_table = escape_column_name(table_name, db_type)
                query = text(f"""
                    SELECT {', '.join(escaped_columns)}
                    FROM {escaped_table} TABLESAMPLE BERNOULLI({sample_percent})
                    LIMIT {self.sample_size}
                """)
        else:
            # Fallback: use LIMIT with chunked scanning
            logger.warning(f"Database type {db_type} may not support efficient sampling, using chunked approach")
            return self._get_chunked_sample_hashes(table_name, engine, identifier_columns, filtered_columns, total_rows, identifier_type)
        
        hashes = {}
        with engine.connect() as conn:
            logger.info(f"DEBUG: Executing sampling query: {query}")
            rows = conn.execute(query).fetchall()
            
            for row in rows:
                # Get identifier values
                identifier_values = []
                for id_col in identifier_columns:
                    if id_col in filtered_columns:
                        id_index = filtered_columns.index(id_col)
                        identifier_values.append(row[id_index])
                
                # Create row signature based on identifier type
                row_signature = create_row_signature(identifier_values, identifier_columns, identifier_type)
                row_hash = self._generate_row_hash(row, filtered_columns)
                hashes[row_signature] = row_hash
        
        logger.info(f"Successfully sampled {len(hashes):,} rows from {table_name}")
        return hashes

    def _get_chunked_sample_hashes(self, table_name: str, engine, identifier_columns: List[str], filtered_columns: List[str], total_rows: int, identifier_type: str) -> Dict[str, str]:
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
                
            # For tables without unique identifiers, ordering might be problematic
            order_by_cols = identifier_columns if identifier_type != 'all_columns' else None
            
            query_str = build_select_query(
                columns=filtered_columns,
                table_name=table_name,
                db_type=get_database_type(engine),
                order_by=order_by_cols,
                limit=min(chunk_sample_size, self.sample_size - sampled_count),
                offset=random_offset
            )
            query = text(query_str)
            
            with engine.connect() as conn:
                rows = conn.execute(query).fetchall()
                
                for row in rows:
                    identifier_values = []
                    for id_col in identifier_columns:
                        if id_col in filtered_columns:
                            id_index = filtered_columns.index(id_col)
                            identifier_values.append(row[id_index])
                    
                    # Create row signature based on identifier type
                    row_signature = create_row_signature(identifier_values, identifier_columns, identifier_type)
                    row_hash = self._generate_row_hash(row, filtered_columns)
                    hashes[row_signature] = row_hash
                    sampled_count += 1
                    
                    if sampled_count >= self.sample_size:
                        break
            
            offset += chunk_interval
        
        logger.info(f"Chunked sampling collected {len(hashes):,} rows from {table_name}")
        return hashes

    def _get_all_table_hashes(self, table_name: str, engine, identifier_columns: List[str], filtered_columns: List[str], identifier_type: str) -> Dict[str, str]:
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
            # For tables without unique identifiers, ordering might be problematic
            order_by_cols = identifier_columns if identifier_type != 'all_columns' else None
            
            query_str = build_select_query(
                columns=filtered_columns,
                table_name=table_name,
                db_type=get_database_type(engine),
                order_by=order_by_cols,
                limit=self.chunk_size,
                offset=offset
            )
            query = text(query_str)
            
            with engine.connect() as conn:
                rows = conn.execute(query).fetchall()
                
            if not rows:
                break
                
            for row in rows:
                identifier_values = []
                for id_col in identifier_columns:
                    if id_col in filtered_columns:
                        id_index = filtered_columns.index(id_col)
                        identifier_values.append(row[id_index])
                
                # Create row signature based on identifier type
                row_signature = create_row_signature(identifier_values, identifier_columns, identifier_type)
                row_hash = self._generate_row_hash(row, filtered_columns)
                hashes[row_signature] = row_hash
                
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
            db_type = get_database_type(engine)
            where_clause = build_where_clause_for_pk(pk_columns, pk_values, db_type)
            
            query_str = build_select_query(
                columns=final_columns,
                table_name=table_name,
                db_type=db_type,
                where_clause=where_clause
            )
            query = text(query_str)
            
            with engine.connect() as conn:
                row = conn.execute(query).fetchone()
                if row:
                    return {col: row[i] for i, col in enumerate(final_columns)}
                return {}
        except Exception as e:
            logger.error(f"Error fetching row data for logging: {str(e)}")
            return {}

    def _log_detailed_mismatch(self, table_name: str, row_identifier: str, identifier_columns: List[str], final_columns: List[str], mismatch_count: int, identifier_type: str, source_hash: str = None, target_hash: str = None):
        """
        Log detailed information about hash mismatches.
        
        Args:
            table_name (str): Name of the table
            row_identifier (str): Row identifier value of mismatched row
            identifier_columns (List[str]): Row identifier column names
            final_columns (List[str]): Columns used for hashing
            mismatch_count (int): Current mismatch count for progress
            identifier_type (str): Type of identifier (primary_key, unique_constraint, all_columns)
            source_hash (str): Hash from source database
            target_hash (str): Hash from target database
        """
        logger.info(f"=== HASH MISMATCH #{mismatch_count} IN TABLE '{table_name}' ===")
        logger.info(f"Row identifier ({identifier_type}): {row_identifier}")
        
        if source_hash and target_hash:
            logger.info(f"Source hash: {source_hash}")
            logger.info(f"Target hash: {target_hash}")
        
        # For detailed row comparison, we need the actual values, not the signature
        # This is complex for 'all_columns' type, so we'll only do it for primary_key and unique_constraint
        if identifier_type in ['primary_key', 'unique_constraint']:
            # Parse the row identifier back to individual values
            pk_values = tuple(row_identifier.split('|'))
            
            # Get detailed row data from both databases
            source_row = self._get_row_data_for_logging(table_name, self.source_engine, identifier_columns, final_columns, pk_values)
            target_row = self._get_row_data_for_logging(table_name, self.target_engine, identifier_columns, final_columns, pk_values)
        else:
            logger.warning(f"Detailed row comparison not available for tables without unique identifiers")
            source_row = {}
            target_row = {}
        
        if source_row and target_row:
            logger.info("SOURCE ROW DATA:")
            source_row_for_hash = []
            for col in final_columns:
                source_val = source_row.get(col, 'NULL')
                source_row_for_hash.append(source_val)
                logger.info(f"  {col}: {repr(source_val)} (type: {type(source_val).__name__})")
            
            logger.info("TARGET ROW DATA:")
            target_row_for_hash = []
            for col in final_columns:
                target_val = target_row.get(col, 'NULL')
                target_row_for_hash.append(target_val)
                logger.info(f"  {col}: {repr(target_val)} (type: {type(target_val).__name__})")
            
            # Generate hashes for both rows and compare
            try:
                computed_source_hash = self._generate_row_hash(tuple(source_row_for_hash), final_columns)
                computed_target_hash = self._generate_row_hash(tuple(target_row_for_hash), final_columns)
                logger.info(f"Computed source hash: {computed_source_hash}")
                logger.info(f"Computed target hash: {computed_target_hash}")
                
                if computed_source_hash != computed_target_hash:
                    logger.info("✓ Hash computation confirms mismatch")
                else:
                    logger.warning("⚠️ Hash computation shows match - possible sampling issue!")
            except Exception as e:
                logger.error(f"Error computing verification hashes: {str(e)}")
            
            # Show differences with detailed type information
            differences = []
            type_differences = []
            for col in final_columns:
                source_val = source_row.get(col)
                target_val = target_row.get(col)
                source_type = type(source_val).__name__
                target_type = type(target_val).__name__
                
                if source_val != target_val:
                    differences.append(f"{col}: {repr(source_val)} != {repr(target_val)}")
                    
                if source_type != target_type:
                    type_differences.append(f"{col}: {source_type} vs {target_type}")
            
            if differences:
                logger.info("VALUE DIFFERENCES FOUND:")
                for diff in differences:
                    logger.info(f"  - {diff}")
            
            if type_differences:
                logger.info("TYPE DIFFERENCES FOUND:")
                for diff in type_differences:
                    logger.info(f"  - {diff}")
                    
            if not differences and not type_differences:
                logger.warning("No obvious differences found - possible encoding, precision, or sampling issue")
                logger.info("Try checking for:")
                logger.info("  - Decimal precision differences (e.g., 150.00 vs 150.0)")
                logger.info("  - Timestamp microsecond differences")
                logger.info("  - Character encoding differences")
                logger.info("  - Whitespace differences")
        else:
            logger.error("Could not retrieve row data for detailed comparison")
        
        logger.info("=" * 60)

    def _generate_fix_query(self, table_name: str, row_identifier: str, identifier_columns: List[str], 
                           final_columns: List[str], identifier_type: str, source_row: Dict[str, Any], 
                           target_row: Dict[str, Any]) -> str:
        """
        Generate a MySQL UPDATE query to fix differences between source and target data.
        
        Args:
            table_name: Name of the table
            row_identifier: Row identifier value
            identifier_columns: List of identifier column names
            final_columns: List of columns used for comparison
            identifier_type: Type of identifier
            source_row: Source row data
            target_row: Target row data
            
        Returns:
            str: MySQL UPDATE query or None if no differences
        """
        if identifier_type not in ['primary_key', 'unique_constraint']:
            logger.warning(f"Cannot generate fix query for table {table_name} - no unique identifier available")
            return None
        
        try:
            # Parse the row identifier back to individual values
            pk_values = tuple(row_identifier.split('|'))
            
            # Get database type for proper escaping
            db_type = get_database_type(self.target_engine)
            
            # Generate the UPDATE query
            update_query = generate_update_query(
                table_name=table_name,
                identifier_columns=identifier_columns,
                identifier_values=pk_values,
                source_data=source_row,
                target_data=target_row,
                db_type=db_type,
                ignored_columns=self.ignored_columns
            )
            
            return update_query
            
        except Exception as e:
            logger.error(f"Error generating fix query for table {table_name}: {str(e)}")
            return None

    def _generate_insert_query(self, table_name: str, row_identifier: str, identifier_columns: List[str], 
                             final_columns: List[str], identifier_type: str, source_row: Dict[str, Any]) -> str:
        """
        Generate a MySQL INSERT query to add missing rows from source data.
        
        Args:
            table_name: Name of the table
            row_identifier: Row identifier value
            identifier_columns: List of identifier column names
            final_columns: List of columns used for comparison
            identifier_type: Type of identifier
            source_row: Source row data (the valid reference)
            
        Returns:
            str: MySQL INSERT query or None if no unique identifier available
        """
        if identifier_type not in ['primary_key', 'unique_constraint']:
            logger.warning(f"Cannot generate insert query for table {table_name} - no unique identifier available")
            return None
        
        try:
            # Get database type for proper escaping
            db_type = get_database_type(self.target_engine)
            
            # Generate the INSERT query using source data as the valid reference
            insert_query = generate_insert_query(
                table_name=table_name,
                source_data=source_row,
                db_type=db_type,
                ignored_columns=self.ignored_columns
            )
            
            return insert_query
            
        except Exception as e:
            logger.error(f"Error generating insert query for table {table_name}: {str(e)}")
            return None

    def _save_fix_queries(self, fix_queries: List[str]) -> None:
        """
        Save fix queries to a SQL file.
        
        Args:
            fix_queries: List of SQL UPDATE and INSERT queries
        """
        if not fix_queries:
            return
            
        try:
            # Ensure logs directory exists
            import os
            os.makedirs(os.path.dirname(self.fix_queries_file), exist_ok=True)
            
            with open(self.fix_queries_file, 'w') as f:
                f.write("-- Auto-generated fix queries for data differences\n")
                f.write("-- Generated by db-checker\n")
                f.write("-- Execute these queries on the TARGET database to fix differences\n")
                f.write("-- Source data is used as the valid reference for all queries\n\n")
                
                for i, query in enumerate(fix_queries, 1):
                    f.write(f"-- Fix query #{i}\n")
                    f.write(f"{query}\n\n")
            
            logger.info(f"Generated {len(fix_queries)} fix queries saved to: {self.fix_queries_file}")
            
        except Exception as e:
            logger.error(f"Error saving fix queries: {str(e)}")

    def debug_hash_for_row(self, table_name: str, row_identifier: str, identifier_columns: List[str], final_columns: List[str], identifier_type: str):
        """
        Debug hash generation for a specific row to identify issues.
        
        Args:
            table_name (str): Name of the table
            row_identifier (str): Row identifier value
            identifier_columns (List[str]): Row identifier column names
            final_columns (List[str]): Columns used for hashing
            identifier_type (str): Type of identifier
        """
        logger.info(f"=== DEBUG HASH GENERATION FOR ROW {row_identifier} ===")
        
        if identifier_type in ['primary_key', 'unique_constraint']:
            pk_values = tuple(row_identifier.split('|'))
            
            # Get raw data from both databases
            source_row = self._get_row_data_for_logging(table_name, self.source_engine, identifier_columns, final_columns, pk_values)
            target_row = self._get_row_data_for_logging(table_name, self.target_engine, identifier_columns, final_columns, pk_values)
            
            if source_row and target_row:
                # Show raw data
                logger.info("SOURCE RAW DATA:")
                source_tuple = []
                for col in final_columns:
                    val = source_row.get(col)
                    source_tuple.append(val)
                    logger.info(f"  {col}: {repr(val)} (type: {type(val).__name__})")
                
                logger.info("TARGET RAW DATA:")
                target_tuple = []
                for col in final_columns:
                    val = target_row.get(col)
                    target_tuple.append(val)
                    logger.info(f"  {col}: {repr(val)} (type: {type(val).__name__})")
                
                # Generate hashes step by step
                logger.info("HASH GENERATION DEBUG:")
                try:
                    source_hash = self._generate_row_hash(tuple(source_tuple), final_columns)
                    target_hash = self._generate_row_hash(tuple(target_tuple), final_columns)
                    
                    logger.info(f"Source hash: {source_hash}")
                    logger.info(f"Target hash: {target_hash}")
                    logger.info(f"Hashes match: {source_hash == target_hash}")
                    
                    # Show hash input string
                    source_dict = {}
                    target_dict = {}
                    for col, val in zip(final_columns, source_tuple):
                        if val is None:
                            source_dict[col] = None
                        elif isinstance(val, (int, float, str, bool)):
                            source_dict[col] = val
                        else:
                            source_dict[col] = str(val)
                    
                    for col, val in zip(final_columns, target_tuple):
                        if val is None:
                            target_dict[col] = None
                        elif isinstance(val, (int, float, str, bool)):
                            target_dict[col] = val
                        else:
                            target_dict[col] = str(val)
                    
                    source_sorted = sorted(source_dict.items())
                    target_sorted = sorted(target_dict.items())
                    source_hash_string = '|'.join([f"{k}:{repr(v)}" for k, v in source_sorted])
                    target_hash_string = '|'.join([f"{k}:{repr(v)}" for k, v in target_sorted])
                    
                    logger.info(f"Source hash string: {source_hash_string}")
                    logger.info(f"Target hash string: {target_hash_string}")
                    logger.info(f"Hash strings match: {source_hash_string == target_hash_string}")
                    
                except Exception as e:
                    logger.error(f"Error generating debug hashes: {str(e)}")
        
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
            
            # Get row identifier columns
            source_identifier, source_id_type = self._get_row_identifier(table_name, self.source_engine, source_filtered)
            target_identifier, target_id_type = self._get_row_identifier(table_name, self.target_engine, target_filtered)
            
            # Check if identifier types match
            if source_id_type != target_id_type:
                return {
                    "status": "error",
                    "error": f"Row identifier type mismatch: source={source_id_type}, target={target_id_type}"
                }
            
            # Check if identifier columns match
            if source_identifier != target_identifier:
                return {
                    "status": "error",
                    "error": f"Row identifier columns mismatch: source={source_identifier}, target={target_identifier}"
                }
            
            # Ensure identifier columns are included in filtered columns
            final_columns = source_filtered.copy()
            for id_col in source_identifier:
                if id_col not in final_columns:
                    final_columns.append(id_col)
            
            # Log information about identifier type
            if source_id_type == 'primary_key':
                logger.info(f"Using primary key {source_identifier} for table {table_name}")
            elif source_id_type == 'unique_constraint':
                logger.warning(f"Table {table_name} has no primary key, using unique constraint {source_identifier}")
            else:  # all_columns
                logger.warning(f"Table {table_name} has no unique identifiers, using all columns for comparison (performance impact expected)")
            
            # Get hashes for both tables using filtered columns
            logger.info(f"Generating hashes for table {table_name} using columns: {final_columns}")
            source_row_count = self._get_table_row_count(table_name, self.source_engine)
            target_row_count = self._get_table_row_count(table_name, self.target_engine)
            
            source_hashes = self._get_table_hashes_with_sampling(table_name, self.source_engine, source_identifier, final_columns, source_row_count, source_id_type)
            target_hashes = self._get_table_hashes_with_sampling(table_name, self.target_engine, target_identifier, final_columns, target_row_count, target_id_type)
            
            # Determine if sampling was used
            sampling_used = (self.sampling_enabled and 
                           (source_row_count > self.max_rows_for_full_scan or 
                            target_row_count > self.max_rows_for_full_scan))
            
            # Compare hashes - only for rows that exist in both samples
            mismatches = []
            mismatch_count = 0
            fix_queries = []  # Collect fix queries for differences
            
            # Get intersection of sampled rows to avoid false positives from inconsistent sampling
            common_row_ids = set(source_hashes.keys()) & set(target_hashes.keys())
            source_only_rows = set(source_hashes.keys()) - set(target_hashes.keys())
            target_only_rows = set(target_hashes.keys()) - set(source_hashes.keys())
            
            if sampling_used:
                # For sampling mode, we only compare rows that exist in both samples
                # This prevents false "missing" warnings due to random sampling differences
                logger.info(f"Comparing {len(common_row_ids):,} rows present in both samples")
                if source_only_rows or target_only_rows:
                    logger.info(f"Sampling note: {len(source_only_rows):,} rows only in source sample, {len(target_only_rows):,} rows only in target sample (this is normal with random sampling)")
                
                # Only compare hash mismatches for common rows
                detailed_logged_count = 0
                for row_id in common_row_ids:
                    source_hash = source_hashes[row_id]
                    target_hash = target_hashes[row_id]
                    if source_hash != target_hash:
                        mismatch_count += 1
                        mismatches.append({
                            "row_identifier": row_id,
                            "status": "hash_mismatch",
                            "source_hash": source_hash,
                            "target_hash": target_hash
                        })
                        
                        # Generate fix query if enabled and we have unique identifiers
                        if self.generate_fix_queries and source_id_type in ['primary_key', 'unique_constraint']:
                            # Check if we've reached the maximum number of fix queries
                            if self.max_fix_queries is not None and len(fix_queries) >= self.max_fix_queries:
                                if len(fix_queries) == self.max_fix_queries:
                                    logger.info(f"🔧 Fix query limit reached ({self.max_fix_queries}). Additional fix queries will not be generated.")
                                continue
                            
                            try:
                                # Get detailed row data for fix query generation
                                pk_values = tuple(row_id.split('|'))
                                source_row = self._get_row_data_for_logging(table_name, self.source_engine, source_identifier, final_columns, pk_values)
                                target_row = self._get_row_data_for_logging(table_name, self.target_engine, source_identifier, final_columns, pk_values)
                                
                                if source_row and target_row:
                                    fix_query = self._generate_fix_query(table_name, row_id, source_identifier, final_columns, source_id_type, source_row, target_row)
                                    if fix_query:
                                        fix_queries.append(fix_query)
                            except Exception as e:
                                logger.error(f"Error generating fix query for row {row_id}: {str(e)}")
                        
                        # Log detailed mismatch information only if under the limit
                        if detailed_logged_count < self.max_detailed_mismatches:
                            detailed_logged_count += 1
                            self._log_detailed_mismatch(table_name, row_id, source_identifier, final_columns, detailed_logged_count, source_id_type, source_hash, target_hash)
                        elif detailed_logged_count == self.max_detailed_mismatches:
                            # Log summary when limit is reached
                            logger.info(f"📋 Detailed logging limit reached ({self.max_detailed_mismatches} mismatches). Additional mismatches will be counted but not logged in detail.")
                            detailed_logged_count += 1  # Increment to avoid logging this message again
            else:
                # For full scan mode, missing rows are actual data issues
                detailed_logged_count = 0
                for row_id, source_hash in source_hashes.items():
                    if row_id not in target_hashes:
                        mismatches.append({
                            "row_identifier": row_id,
                            "status": "missing_in_target",
                            "source_hash": source_hash
                        })
                        logger.warning(f"Row missing in target - Identifier: {row_id}")
                        
                        # Generate INSERT query if enabled and we have unique identifiers
                        if self.generate_fix_queries and source_id_type in ['primary_key', 'unique_constraint']:
                            # Check if we've reached the maximum number of fix queries
                            if self.max_fix_queries is not None and len(fix_queries) >= self.max_fix_queries:
                                if len(fix_queries) == self.max_fix_queries:
                                    logger.info(f"🔧 Fix query limit reached ({self.max_fix_queries}). Additional fix queries will not be generated.")
                                continue
                            
                            try:
                                # Get detailed row data from source for INSERT query generation
                                pk_values = tuple(row_id.split('|'))
                                source_row = self._get_row_data_for_logging(table_name, self.source_engine, source_identifier, final_columns, pk_values)
                                
                                if source_row:
                                    insert_query = self._generate_insert_query(table_name, row_id, source_identifier, final_columns, source_id_type, source_row)
                                    if insert_query:
                                        fix_queries.append(insert_query)
                            except Exception as e:
                                logger.error(f"Error generating insert query for row {row_id}: {str(e)}")
                    elif target_hashes[row_id] != source_hash:
                        mismatch_count += 1
                        mismatches.append({
                            "row_identifier": row_id,
                            "status": "hash_mismatch",
                            "source_hash": source_hash,
                            "target_hash": target_hashes[row_id]
                        })
                        
                        # Generate fix query if enabled and we have unique identifiers
                        if self.generate_fix_queries and source_id_type in ['primary_key', 'unique_constraint']:
                            # Check if we've reached the maximum number of fix queries
                            if self.max_fix_queries is not None and len(fix_queries) >= self.max_fix_queries:
                                if len(fix_queries) == self.max_fix_queries:
                                    logger.info(f"🔧 Fix query limit reached ({self.max_fix_queries}). Additional fix queries will not be generated.")
                                continue
                            
                            try:
                                # Get detailed row data for fix query generation
                                pk_values = tuple(row_id.split('|'))
                                source_row = self._get_row_data_for_logging(table_name, self.source_engine, source_identifier, final_columns, pk_values)
                                target_row = self._get_row_data_for_logging(table_name, self.target_engine, source_identifier, final_columns, pk_values)
                                
                                if source_row and target_row:
                                    fix_query = self._generate_fix_query(table_name, row_id, source_identifier, final_columns, source_id_type, source_row, target_row)
                                    if fix_query:
                                        fix_queries.append(fix_query)
                            except Exception as e:
                                logger.error(f"Error generating fix query for row {row_id}: {str(e)}")
                        
                        # Log detailed mismatch information only if under the limit
                        if detailed_logged_count < self.max_detailed_mismatches:
                            detailed_logged_count += 1
                            self._log_detailed_mismatch(table_name, row_id, source_identifier, final_columns, detailed_logged_count, source_id_type, source_hash, target_hashes[row_id])
                        elif detailed_logged_count == self.max_detailed_mismatches:
                            # Log summary when limit is reached
                            logger.info(f"📋 Detailed logging limit reached ({self.max_detailed_mismatches} mismatches). Additional mismatches will be counted but not logged in detail.")
                            detailed_logged_count += 1  # Increment to avoid logging this message again
                
                # Check for rows in target but not in source (only for full scan)
                for row_id in target_hashes:
                    if row_id not in source_hashes:
                        mismatches.append({
                            "row_identifier": row_id,
                            "status": "missing_in_source",
                            "target_hash": target_hashes[row_id]
                        })
                        logger.warning(f"Row missing in source - Identifier: {row_id}")
            
            # Save fix queries if any were generated
            if fix_queries and self.generate_fix_queries:
                self._save_fix_queries(fix_queries)
            
            result = {
                "status": "success" if not mismatches else "mismatch",
                "total_rows_source": source_row_count,
                "total_rows_target": target_row_count,
                "sampled_rows_source": len(source_hashes),
                "sampled_rows_target": len(target_hashes),
                "compared_rows": len(common_row_ids) if sampling_used else len(source_hashes),
                "sampling_used": sampling_used,
                "sample_size": self.sample_size if sampling_used else None,
                "mismatches": mismatches,
                "fix_queries_generated": len(fix_queries) if self.generate_fix_queries else 0,
                "columns_used": final_columns,
                "ignored_columns": [col for col in target_columns if col in self.ignored_columns]
            }
            
            if mismatches:
                if sampling_used:
                    # For sampling mode, we only report hash mismatches (no missing row false positives)
                    hash_mismatches = [m for m in mismatches if m.get("status") == "hash_mismatch"]
                    
                    if hash_mismatches:
                        logger.warning(
                            f"Hash validation found {len(hash_mismatches)} hash mismatches in table {table_name} "
                            f"(compared {len(common_row_ids):,} common rows from samples; "
                            f"total rows: source={source_row_count:,}, target={target_row_count:,})"
                        )
                else:
                    # For full scan mode, report all types of mismatches
                    hash_mismatches = [m for m in mismatches if m.get("status") == "hash_mismatch"]
                    missing_in_target = [m for m in mismatches if m.get("status") == "missing_in_target"]
                    missing_in_source = [m for m in mismatches if m.get("status") == "missing_in_source"]
                    
                    mismatch_details = []
                    if hash_mismatches:
                        mismatch_details.append(f"{len(hash_mismatches)} hash mismatches")
                    if missing_in_target:
                        mismatch_details.append(f"{len(missing_in_target)} missing in target")
                    if missing_in_source:
                        mismatch_details.append(f"{len(missing_in_source)} missing in source")
                    
                    logger.warning(
                        f"Hash validation found issues in table {table_name}: {', '.join(mismatch_details)}"
                    )
                # Add instruction for detailed logs and fix queries
                if mismatch_count > 0:  # Only show instruction if there were hash mismatches (not just missing rows)
                    logger.info(f"📋 For detailed row-by-row comparison of mismatched data, check: logs/data_checker.log")
                if fix_queries and self.generate_fix_queries:
                    logger.info(f"🔧 Generated {len(fix_queries)} fix queries (UPDATE and INSERT) saved to: {self.fix_queries_file}")
            else:
                if sampling_used:
                    logger.info(f"Hash validation successful for table {table_name} - {len(common_row_ids):,} compared rows matched")
                else:
                    logger.info(f"Hash validation successful for table {table_name}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in hash validation for table {table_name}: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            } 