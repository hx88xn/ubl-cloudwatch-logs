import boto3
from fastapi import HTTPException
from typing import Optional, List, Dict, Any
from src.config import (
    AWS_REGION,
    DB_NAME,
    RDS_RESOURCE_ARN,
    RDS_SECRET_ARN
)


def get_rds_data_client():
    """Create and return an RDS Data API client."""
    return boto3.client('rds-data', region_name=AWS_REGION)


def execute_sql(sql: str, parameters: list = None) -> Dict[str, Any]:
    """
    Execute SQL statement using RDS Data API.
    
    Args:
        sql: SQL statement to execute
        parameters: Optional list of parameters for parameterized queries
        
    Returns:
        Response from the RDS Data API
    """
    if not RDS_RESOURCE_ARN or not RDS_SECRET_ARN:
        raise HTTPException(
            status_code=500,
            detail="RDS Data API not configured. Please set RDS_RESOURCE_ARN and RDS_SECRET_ARN in your .env file."
        )
    
    client = get_rds_data_client()
    
    params = {
        'resourceArn': RDS_RESOURCE_ARN,
        'secretArn': RDS_SECRET_ARN,
        'database': DB_NAME,
        'sql': sql,
        'includeResultMetadata': True
    }
    
    if parameters:
        params['parameters'] = parameters
    
    try:
        response = client.execute_statement(**params)
        return response
    except client.exceptions.BadRequestException as e:
        raise HTTPException(
            status_code=400,
            detail=f"Bad SQL request: {str(e)}"
        )
    except client.exceptions.ForbiddenException as e:
        raise HTTPException(
            status_code=403,
            detail=f"Access denied to RDS Data API: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )


def parse_data_api_response(response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse RDS Data API response into a standard format.
    
    Args:
        response: Raw response from RDS Data API
        
    Returns:
        Dictionary with 'columns' and 'rows' keys
    """
    columns = [col['name'] for col in response.get('columnMetadata', [])]
    rows = []
    
    for record in response.get('records', []):
        row = {}
        for i, field in enumerate(record):
            # Extract value from typed field (stringValue, longValue, booleanValue, etc.)
            if 'isNull' in field and field['isNull']:
                value = None
            elif 'stringValue' in field:
                value = field['stringValue']
            elif 'longValue' in field:
                value = field['longValue']
            elif 'doubleValue' in field:
                value = field['doubleValue']
            elif 'booleanValue' in field:
                value = field['booleanValue']
            elif 'blobValue' in field:
                value = field['blobValue'].decode('utf-8', errors='replace')
            elif 'arrayValue' in field:
                value = field['arrayValue']
            else:
                # Fallback: get first available value
                value = list(field.values())[0] if field else None
            
            row[columns[i]] = value
        rows.append(row)
    
    return {'columns': columns, 'rows': rows}


def get_tables() -> List[Dict[str, Any]]:
    """Get list of all tables in the database with row counts."""
    try:
        # Get all tables
        response = execute_sql("SHOW TABLES")
        tables_result = parse_data_api_response(response)
        
        tables = []
        for row in tables_result['rows']:
            table_name = list(row.values())[0]
            
            # Get row count for each table
            count_response = execute_sql(f"SELECT COUNT(*) as count FROM `{table_name}`")
            count_result = parse_data_api_response(count_response)
            row_count = list(count_result['rows'][0].values())[0] if count_result['rows'] else 0
            
            tables.append({
                'name': table_name,
                'row_count': row_count
            })
        
        return tables
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching tables: {str(e)}"
        )


def get_table_schema(table_name: str) -> List[Dict[str, Any]]:
    """Get column information for a table."""
    try:
        response = execute_sql(f"DESCRIBE `{table_name}`")
        result = parse_data_api_response(response)
        return result['rows']
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching table schema: {str(e)}"
        )


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
    try:
        # Get column names
        schema_response = execute_sql(f"DESCRIBE `{table_name}`")
        columns_info = parse_data_api_response(schema_response)
        columns = [col['Field'] for col in columns_info['rows']]
        
        # Build base query
        base_query = f"SELECT * FROM `{table_name}`"
        count_query = f"SELECT COUNT(*) as total FROM `{table_name}`"
        
        # Add search filter if provided
        where_clause = ""
        if search_query:
            # Search across all columns using LIKE
            conditions = []
            for col in columns:
                conditions.append(f"CAST(`{col}` AS CHAR) LIKE '%{search_query}%'")
            where_clause = " WHERE " + " OR ".join(conditions)
        
        # Get total count
        count_response = execute_sql(count_query + where_clause)
        count_result = parse_data_api_response(count_response)
        total = list(count_result['rows'][0].values())[0] if count_result['rows'] else 0
        
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
        data_response = execute_sql(final_query)
        
        result = parse_data_api_response(data_response)
        rows = result['rows']
        
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
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching table data: {str(e)}"
        )
