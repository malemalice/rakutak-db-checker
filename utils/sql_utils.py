from typing import List
from sqlalchemy.engine import Engine

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