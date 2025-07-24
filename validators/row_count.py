from sqlalchemy import text, MetaData, Table
from typing import Dict, Any, List, Tuple
from loguru import logger
from validators.base import BaseValidator
from utils.sql_utils import (
    get_database_type, escape_column_name, build_select_query, 
    get_suitable_row_identifier, create_row_signature
)

class RowCountValidator(BaseValidator):
    def __init__(self, source_engine, target_engine, config):
        super().__init__(source_engine, target_engine, config)
        
        # Missing row detection configuration
        self.missing_detection_config = config['validation'].get('row_count_missing_detection', {})
        self.missing_detection_enabled = self.missing_detection_config.get('enabled', True)
        self.max_missing_rows_to_log = self.missing_detection_config.get('max_missing_rows_to_log', 50)
        self.max_table_size_for_detection = self.missing_detection_config.get('max_table_size_for_detection', 1000000)
    
    def _get_row_identifiers(self, table_name: str, engine, limit: int = None) -> Tuple[List[str], List[str], str]:
        """
        Get row identifiers for a table to help identify missing rows.
        
        Args:
            table_name (str): Name of the table
            engine: SQLAlchemy engine
            limit (int): Optional limit for number of rows to fetch
            
        Returns:
            Tuple[List[str], List[str], str]: (identifier_columns, row_identifiers, identifier_type)
        """
        try:
            # Get table metadata to find available columns
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=engine)
            available_columns = [c.name for c in table.columns]
            
            # Get suitable row identifier columns
            identifier_columns, identifier_type = get_suitable_row_identifier(
                table_name, engine, available_columns
            )
            
            if not identifier_columns:
                logger.warning(f"No suitable identifier found for table {table_name}")
                return [], [], "none"
            
            # Build query to get row identifiers
            db_type = get_database_type(engine)
            query = build_select_query(
                columns=identifier_columns,
                table_name=table_name,
                db_type=db_type,
                order_by=identifier_columns,
                limit=limit
            )
            
            # Execute query and get identifiers
            row_identifiers = []
            with engine.connect() as conn:
                result = conn.execute(text(query))
                for row in result:
                    if len(identifier_columns) == 1:
                        row_identifiers.append(str(row[0]))
                    else:
                        # Create composite identifier
                        identifier_values = [str(row[i]) for i in range(len(identifier_columns))]
                        row_identifiers.append(create_row_signature(identifier_values, identifier_columns, identifier_type))
            
            return identifier_columns, row_identifiers, identifier_type
            
        except Exception as e:
            logger.error(f"Error getting row identifiers for table {table_name}: {str(e)}")
            return [], [], "error"
    
    def _log_missing_rows(self, table_name: str, missing_in_target: List[str], missing_in_source: List[str], 
                         identifier_columns: List[str], identifier_type: str):
        """
        Log missing row identifiers.
        
        Args:
            table_name (str): Name of the table
            missing_in_target (List[str]): Row identifiers missing in target
            missing_in_source (List[str]): Row identifiers missing in source  
            identifier_columns (List[str]): Column names used for identification
            identifier_type (str): Type of identifier used
        """
        if missing_in_target:
            logger.info(f"=== ROWS MISSING IN TARGET - TABLE '{table_name}' ===")
            logger.info(f"Identifier type: {identifier_type}")
            logger.info(f"Identifier columns: {identifier_columns}")
            logger.info(f"Total missing rows: {len(missing_in_target)}")
            
            logged_count = 0
            for row_id in missing_in_target:
                if logged_count < self.max_missing_rows_to_log:
                    logged_count += 1
                    logger.info(f"  Missing row #{logged_count}: {row_id}")
                elif logged_count == self.max_missing_rows_to_log:
                    logger.info(f"  ... and {len(missing_in_target) - logged_count} more missing rows (logging limit reached)")
                    break
            logger.info("=" * 60)
        
        if missing_in_source:
            logger.info(f"=== ROWS MISSING IN SOURCE - TABLE '{table_name}' ===")
            logger.info(f"Identifier type: {identifier_type}")
            logger.info(f"Identifier columns: {identifier_columns}")
            logger.info(f"Total missing rows: {len(missing_in_source)}")
            
            logged_count = 0
            for row_id in missing_in_source:
                if logged_count < self.max_missing_rows_to_log:
                    logged_count += 1
                    logger.info(f"  Missing row #{logged_count}: {row_id}")
                elif logged_count == self.max_missing_rows_to_log:
                    logger.info(f"  ... and {len(missing_in_source) - logged_count} more missing rows (logging limit reached)")
                    break
            logger.info("=" * 60)

    def validate_table(self, table_name: str) -> Dict[str, Any]:
        """
        Validate row counts between source and target tables.
        
        Args:
            table_name (str): Name of the table to validate
            
        Returns:
            Dict[str, Any]: Validation results including row counts and status
        """
        try:
            # Get database types and escape table name
            source_db_type = get_database_type(self.source_engine)
            target_db_type = get_database_type(self.target_engine)
            
            source_table_escaped = escape_column_name(table_name, source_db_type)
            target_table_escaped = escape_column_name(table_name, target_db_type)
            
            # Get source row count
            with self.source_engine.connect() as conn:
                source_count = conn.execute(text(f"SELECT COUNT(*) FROM {source_table_escaped}")).scalar()
            
            # Get target row count
            with self.target_engine.connect() as conn:
                target_count = conn.execute(text(f"SELECT COUNT(*) FROM {target_table_escaped}")).scalar()
            
            # Calculate difference
            diff = source_count - target_count
            diff_percentage = (diff / source_count * 100) if source_count > 0 else 0
            
            result = {
                "status": "success" if diff == 0 else "mismatch",
                "source_count": source_count,
                "target_count": target_count,
                "difference": diff,
                "difference_percentage": round(diff_percentage, 2),
                "missing_detection_performed": False,
                "missing_in_target": [],
                "missing_in_source": []
            }
            
            if diff != 0:
                logger.warning(
                    f"Row count mismatch in table {table_name}: "
                    f"source={source_count}, target={target_count}, "
                    f"diff={diff} ({diff_percentage:.2f}%)"
                )
                
                # Perform missing row detection if enabled and table size is reasonable
                if (self.missing_detection_enabled and 
                    max(source_count, target_count) <= self.max_table_size_for_detection):
                    
                    logger.info(f"🔍 Detecting missing rows for table {table_name}...")
                    
                    # Get row identifiers from both tables
                    source_id_cols, source_identifiers, source_id_type = self._get_row_identifiers(table_name, self.source_engine)
                    target_id_cols, target_identifiers, target_id_type = self._get_row_identifiers(table_name, self.target_engine)
                    
                    if (source_identifiers and target_identifiers and 
                        source_id_cols == target_id_cols and source_id_type == target_id_type):
                        
                        # Find missing rows
                        source_set = set(source_identifiers)
                        target_set = set(target_identifiers)
                        
                        missing_in_target = list(source_set - target_set)
                        missing_in_source = list(target_set - source_set)
                        
                        result.update({
                            "missing_detection_performed": True,
                            "missing_in_target": missing_in_target,
                            "missing_in_source": missing_in_source,
                            "identifier_columns": source_id_cols,
                            "identifier_type": source_id_type
                        })
                        
                        # Log missing rows
                        self._log_missing_rows(table_name, missing_in_target, missing_in_source, 
                                             source_id_cols, source_id_type)
                        
                        # Summary logging
                        if missing_in_target or missing_in_source:
                            summary_parts = []
                            if missing_in_target:
                                summary_parts.append(f"{len(missing_in_target)} missing in target")
                            if missing_in_source:
                                summary_parts.append(f"{len(missing_in_source)} missing in source")
                            
                            logger.info(f"📋 Missing row analysis for {table_name}: {', '.join(summary_parts)}")
                            logger.info(f"📋 For detailed missing row identifiers, check: logs/data_checker.log")
                        else:
                            logger.warning(f"⚠️ Row count mismatch detected but no missing rows found - possible data consistency issue")
                    else:
                        logger.warning(f"Cannot detect missing rows for table {table_name}: identifier mismatch or unavailable")
                        result["missing_detection_performed"] = False
                elif not self.missing_detection_enabled:
                    logger.info("Missing row detection is disabled")
                else:
                    logger.info(f"Skipping missing row detection for large table {table_name} ({max(source_count, target_count):,} rows)")
                    
            else:
                logger.info(f"Row count match in table {table_name}: {source_count} rows")
            
            return result
            
        except Exception as e:
            logger.error(f"Error validating row count for table {table_name}: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            } 