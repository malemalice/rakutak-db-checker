from typing import List, Optional, Tuple
from sqlalchemy.engine import Engine
from sqlalchemy import MetaData, Table, text
from loguru import logger

# MySQL/MariaDB reserved keywords that need escaping
MYSQL_RESERVED_KEYWORDS = {
    'order', 'type', 'status', 'key', 'value', 'group', 'user', 'role', 'index', 'unique',
    'primary', 'foreign', 'constraint', 'table', 'database', 'schema', 'view', 'trigger',
    'procedure', 'function', 'cursor', 'declare', 'begin', 'end', 'if', 'then', 'else',
    'when', 'case', 'while', 'loop', 'repeat', 'leave', 'iterate', 'return', 'call',
    'select', 'insert', 'update', 'delete', 'create', 'drop', 'alter', 'grant', 'revoke',
    'commit', 'rollback', 'savepoint', 'lock', 'unlock', 'desc', 'asc', 'limit', 'offset',
    'union', 'intersect', 'except', 'exists', 'in', 'between', 'like', 'regexp', 'match',
    'and', 'or', 'not', 'xor', 'is', 'null', 'true', 'false', 'distinct', 'all', 'any',
    'some', 'as', 'on', 'using', 'join', 'inner', 'outer', 'left', 'right', 'full',
    'cross', 'natural', 'where', 'having', 'order', 'group', 'by', 'into', 'values',
    'set', 'from', 'with', 'recursive', 'window', 'partition', 'over', 'rows', 'range',
    'unbounded', 'preceding', 'following', 'current', 'row'
}

# PostgreSQL reserved keywords that need escaping  
POSTGRESQL_RESERVED_KEYWORDS = {
    'order', 'type', 'user', 'group', 'key', 'value', 'role', 'index', 'unique', 'primary',
    'foreign', 'constraint', 'table', 'database', 'schema', 'view', 'trigger', 'procedure',
    'function', 'cursor', 'declare', 'begin', 'end', 'if', 'then', 'else', 'when', 'case',
    'while', 'loop', 'return', 'call', 'select', 'insert', 'update', 'delete', 'create',
    'drop', 'alter', 'grant', 'revoke', 'commit', 'rollback', 'savepoint', 'lock', 'desc',
    'asc', 'limit', 'offset', 'union', 'intersect', 'except', 'exists', 'in', 'between',
    'like', 'ilike', 'similar', 'and', 'or', 'not', 'is', 'null', 'true', 'false',
    'distinct', 'all', 'any', 'some', 'as', 'on', 'using', 'join', 'inner', 'outer',
    'left', 'right', 'full', 'cross', 'natural', 'where', 'having', 'order', 'group',
    'by', 'into', 'values', 'set', 'from', 'with', 'recursive', 'window', 'partition',
    'over', 'rows', 'range', 'unbounded', 'preceding', 'following', 'current', 'row'
}


def get_database_type(engine: Engine) -> str:
    """
    Get the database type from SQLAlchemy engine.
    
    Args:
        engine: SQLAlchemy engine instance
        
    Returns:
        str: Database type ('mysql', 'postgresql', etc.)
    """
    return engine.name.lower()


def escape_column_name(column_name: str, db_type: str) -> str:
    """
    Escape column name if it's a reserved keyword for the given database type.
    
    Args:
        column_name: The column name to potentially escape
        db_type: Database type ('mysql', 'postgresql', etc.)
        
    Returns:
        str: Escaped column name if needed, otherwise original name
    """
    column_lower = column_name.lower()
    
    if db_type in ['mysql', 'mariadb']:
        if column_lower in MYSQL_RESERVED_KEYWORDS:
            return f"`{column_name}`"
    elif db_type == 'postgresql':
        if column_lower in POSTGRESQL_RESERVED_KEYWORDS:
            return f'"{column_name}"'
    
    return column_name


def escape_column_list(columns: List[str], db_type: str) -> List[str]:
    """
    Escape a list of column names for reserved keywords.
    
    Args:
        columns: List of column names
        db_type: Database type ('mysql', 'postgresql', etc.)
        
    Returns:
        List[str]: List of escaped column names
    """
    return [escape_column_name(col, db_type) for col in columns]


def get_table_columns(table_name: str, engine: Engine) -> List[str]:
    """
    Get all column names for a table.
    
    Args:
        table_name: Name of the table
        engine: SQLAlchemy engine
        
    Returns:
        List[str]: List of column names
    """
    try:
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)
        return [col.name for col in table.columns]
    except Exception as e:
        logger.error(f"Error getting columns for table {table_name}: {str(e)}")
        return []


def get_primary_key_columns(table_name: str, engine: Engine) -> List[str]:
    """
    Get primary key columns for a table.
    
    Args:
        table_name: Name of the table
        engine: SQLAlchemy engine
        
    Returns:
        List[str]: List of primary key column names (empty if no primary key)
    """
    try:
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)
        pk_columns = [c.name for c in table.primary_key.columns]
        
        if pk_columns:
            logger.debug(f"Table {table_name} has primary key: {pk_columns}")
        else:
            logger.warning(f"Table {table_name} has no primary key defined")
            
        return pk_columns
    except Exception as e:
        logger.error(f"Error getting primary key for table {table_name}: {str(e)}")
        return []


def get_unique_columns(table_name: str, engine: Engine) -> List[List[str]]:
    """
    Get unique constraint columns for a table.
    
    Args:
        table_name: Name of the table
        engine: SQLAlchemy engine
        
    Returns:
        List[List[str]]: List of unique constraints, each containing column names
    """
    try:
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)
        
        unique_constraints = []
        for constraint in table.constraints:
            if hasattr(constraint, 'columns') and len(constraint.columns) > 0:
                # Check if it's a unique constraint (not primary key)
                if constraint.__class__.__name__ == 'UniqueConstraint':
                    constraint_cols = [col.name for col in constraint.columns]
                    unique_constraints.append(constraint_cols)
        
        return unique_constraints
    except Exception as e:
        logger.error(f"Error getting unique constraints for table {table_name}: {str(e)}")
        return []


def get_suitable_row_identifier(table_name: str, engine: Engine, available_columns: List[str] = None) -> Tuple[List[str], str]:
    """
    Get the best available row identifier for a table.
    
    Priority order:
    1. Primary key columns
    2. Single unique constraint columns
    3. All columns (for tables without any unique identifier)
    
    Args:
        table_name: Name of the table
        engine: SQLAlchemy engine
        available_columns: List of available columns to choose from (optional)
        
    Returns:
        Tuple[List[str], str]: (identifier_columns, identifier_type)
        identifier_type can be: 'primary_key', 'unique_constraint', 'all_columns'
    """
    # Get primary key columns
    pk_columns = get_primary_key_columns(table_name, engine)
    
    if pk_columns:
        # Filter primary key columns by available columns if specified
        if available_columns:
            pk_columns = [col for col in pk_columns if col in available_columns]
            if pk_columns:
                return pk_columns, 'primary_key'
        else:
            return pk_columns, 'primary_key'
    
    logger.warning(f"Table {table_name} has no primary key, checking for unique constraints...")
    
    # Try to find unique constraints
    unique_constraints = get_unique_columns(table_name, engine)
    
    for unique_cols in unique_constraints:
        # Filter by available columns if specified
        if available_columns:
            filtered_unique = [col for col in unique_cols if col in available_columns]
            if len(filtered_unique) == len(unique_cols):  # All unique columns are available
                logger.info(f"Using unique constraint {filtered_unique} as row identifier for table {table_name}")
                return filtered_unique, 'unique_constraint'
        else:
            logger.info(f"Using unique constraint {unique_cols} as row identifier for table {table_name}")
            return unique_cols, 'unique_constraint'
    
    # No unique identifier found, use all columns
    if available_columns:
        all_cols = available_columns
    else:
        all_cols = get_table_columns(table_name, engine)
    
    logger.warning(
        f"Table {table_name} has no primary key or unique constraints. "
        f"Using all columns as row identifier. This may impact performance and accuracy."
    )
    
    return all_cols, 'all_columns'


def build_select_query(
    columns: List[str], 
    table_name: str, 
    db_type: str,
    where_clause: str = None,
    order_by: List[str] = None,
    limit: int = None,
    offset: int = None
) -> str:
    """
    Build a SELECT query with properly escaped column names.
    
    Args:
        columns: List of column names to select
        table_name: Name of the table
        db_type: Database type
        where_clause: Optional WHERE clause
        order_by: Optional list of columns to order by
        limit: Optional LIMIT value
        offset: Optional OFFSET value
        
    Returns:
        str: Complete SELECT query
    """
    escaped_columns = escape_column_list(columns, db_type)
    escaped_table = escape_column_name(table_name, db_type)
    
    query = f"SELECT {', '.join(escaped_columns)} FROM {escaped_table}"
    
    if where_clause:
        query += f" WHERE {where_clause}"
    
    if order_by:
        escaped_order_cols = escape_column_list(order_by, db_type)
        query += f" ORDER BY {', '.join(escaped_order_cols)}"
    
    if limit is not None:
        query += f" LIMIT {limit}"
    
    if offset is not None:
        query += f" OFFSET {offset}"
    
    return query


def build_where_clause_for_pk(pk_columns: List[str], pk_values: tuple, db_type: str) -> str:
    """
    Build a WHERE clause for primary key matching with proper escaping.
    
    Args:
        pk_columns: List of primary key column names
        pk_values: Tuple of primary key values
        db_type: Database type
        
    Returns:
        str: WHERE clause string
    """
    escaped_columns = escape_column_list(pk_columns, db_type)
    
    if len(pk_columns) == 1:
        return f"{escaped_columns[0]} = '{pk_values[0]}'"
    else:
        conditions = []
        for i, col in enumerate(escaped_columns):
            conditions.append(f"{col} = '{pk_values[i]}'")
        return " AND ".join(conditions)


def create_row_signature(row_data: List, identifier_columns: List[str], identifier_type: str) -> str:
    """
    Create a unique signature for a row based on available identifier columns.
    
    Args:
        row_data: List of row values
        identifier_columns: List of column names used as identifier
        identifier_type: Type of identifier ('primary_key', 'unique_constraint', 'all_columns')
        
    Returns:
        str: Unique row signature
    """
    import hashlib
    import json
    
    if identifier_type == 'all_columns':
        # For tables without unique identifiers, create a hash of all column values
        # This is less reliable but the best we can do
        row_dict = {col: str(val) for col, val in zip(identifier_columns, row_data)}
        row_json = json.dumps(row_dict, sort_keys=True)
        return hashlib.md5(row_json.encode()).hexdigest()
    else:
        # For primary keys and unique constraints, create a more readable signature
        # Preserve the order of identifier columns (important for composite keys)
        ordered_values = [str(val) for val in row_data]
        return '|'.join(ordered_values) 