from fastapi import HTTPException
from typing import Optional, List, Dict, Any
import pymysql
from pymysql.cursors import DictCursor
from src.config import (
    DB_HOST,
    DB_PORT,
    DB_USER,
    DB_PASSWORD,
    DB_NAME
)


def get_db_connection():
    """Create and return a MySQL database connection."""
    if not DB_PASSWORD:
        raise HTTPException(
            status_code=500,
            detail="Database password not configured. Please set DB_PASSWORD in your .env file."
        )
    
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            cursorclass=DictCursor,
            connect_timeout=10,
            read_timeout=30,
            charset='utf8mb4'
        )
        return connection
    except pymysql.OperationalError as e:
        error_code = e.args[0] if e.args else 'Unknown'
        error_msg = e.args[1] if len(e.args) > 1 else str(e)
        raise HTTPException(
            status_code=500,
            detail=f"Database connection failed (Error {error_code}): {error_msg}"
        )
    except pymysql.Error as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )


def get_tables() -> List[Dict[str, Any]]:
    """Get list of all tables in the database with row counts."""
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            # Get all tables
            cursor.execute("SHOW TABLES")
            tables_result = cursor.fetchall()
            
            tables = []
            for row in tables_result:
                table_name = list(row.values())[0]
                
                # Get row count for each table
                cursor.execute(f"SELECT COUNT(*) as count FROM `{table_name}`")
                count_result = cursor.fetchone()
                row_count = count_result['count'] if count_result else 0
                
                tables.append({
                    'name': table_name,
                    'row_count': row_count
                })
            
            return tables
    except pymysql.Error as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching tables: {str(e)}"
        )
    finally:
        connection.close()


def get_table_schema(table_name: str) -> List[Dict[str, Any]]:
    """Get column information for a table."""
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"DESCRIBE `{table_name}`")
            columns = cursor.fetchall()
            return columns
    except pymysql.Error as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching table schema: {str(e)}"
        )
    finally:
        connection.close()


def get_table_data(
    table_name: str,
    page: int = 1,
    page_size: int = 50,
    search_query: Optional[str] = None,
    order_by: Optional[str] = None,
    order_dir: str = 'DESC'
) -> Dict[str, Any]:
    """
    Fetch data from a table with pagination and optional search.
    
    Args:
        table_name: Name of the table to query
        page: Current page number (1-indexed)
        page_size: Number of rows per page
        search_query: Optional search string
        order_by: Column to order by
        order_dir: Order direction (ASC or DESC)
    
    Returns:
        Dictionary containing rows, pagination info, and column names
    """
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            # Get column names
            cursor.execute(f"DESCRIBE `{table_name}`")
            columns_info = cursor.fetchall()
            columns = [col['Field'] for col in columns_info]
            
            # Build base query
            base_query = f"SELECT * FROM `{table_name}`"
            count_query = f"SELECT COUNT(*) as total FROM `{table_name}`"
            
            # Add search filter if provided
            where_clause = ""
            if search_query:
                search_term = f"%{search_query}%"
                # Search across all string-compatible columns
                conditions = []
                for col in columns:
                    conditions.append(f"`{col}` LIKE %s")
                where_clause = " WHERE " + " OR ".join(conditions)
            
            # Get total count
            if where_clause:
                cursor.execute(count_query + where_clause, [f"%{search_query}%" for _ in columns])
            else:
                cursor.execute(count_query)
            total_result = cursor.fetchone()
            total = total_result['total'] if total_result else 0
            
            # Add ordering
            if order_by and order_by in columns:
                order_clause = f" ORDER BY `{order_by}` {order_dir}"
            else:
                # Default to first column or id if exists
                primary_col = 'id' if 'id' in columns else columns[0] if columns else None
                order_clause = f" ORDER BY `{primary_col}` DESC" if primary_col else ""
            
            # Add pagination
            offset = (page - 1) * page_size
            limit_clause = f" LIMIT {page_size} OFFSET {offset}"
            
            # Execute final query
            final_query = base_query + where_clause + order_clause + limit_clause
            if where_clause:
                cursor.execute(final_query, [f"%{search_query}%" for _ in columns])
            else:
                cursor.execute(final_query)
            
            rows = cursor.fetchall()
            
            # Convert any non-serializable types
            serializable_rows = []
            for row in rows:
                serializable_row = {}
                for key, value in row.items():
                    if isinstance(value, bytes):
                        serializable_row[key] = value.decode('utf-8', errors='replace')
                    elif hasattr(value, 'isoformat'):
                        serializable_row[key] = value.isoformat()
                    else:
                        serializable_row[key] = value
                serializable_rows.append(serializable_row)
            
            total_pages = (total + page_size - 1) // page_size if total > 0 else 1
            
            return {
                'rows': serializable_rows,
                'columns': columns,
                'total': total,
                'page': page,
                'page_size': page_size,
                'has_more': (page * page_size) < total,
                'total_pages': total_pages,
                'table_name': table_name
            }
            
    except pymysql.Error as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching table data: {str(e)}"
        )
    finally:
        connection.close()
