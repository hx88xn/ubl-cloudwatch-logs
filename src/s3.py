from fastapi import HTTPException
from datetime import datetime
from typing import Optional, List
import boto3
from botocore.exceptions import ClientError
from src.config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    S3_BUCKET_NAME
)


def get_s3_client():
    """Create and return an S3 client using configured credentials."""
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )


def format_file_size(size_bytes: int) -> str:
    """Convert bytes to human-readable format."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def list_audio_files(
    prefix: str = "",
    page: int = 1,
    page_size: int = 50,
    search_query: Optional[str] = None
):
    """
    List audio files from S3 bucket with pagination and search.
    
    Args:
        prefix: Optional prefix to filter by folder/path
        page: Current page number (1-indexed)
        page_size: Number of items per page
        search_query: Optional search string to filter filenames
    
    Returns:
        Dictionary containing files list, pagination info
    """
    client = get_s3_client()
    
    try:
        all_files = []
        continuation_token = None
        
        # Fetch all objects from bucket
        while True:
            params = {
                'Bucket': S3_BUCKET_NAME,
                'MaxKeys': 1000
            }
            
            if prefix:
                params['Prefix'] = prefix
            
            if continuation_token:
                params['ContinuationToken'] = continuation_token
            
            response = client.list_objects_v2(**params)
            
            for obj in response.get('Contents', []):
                key = obj['Key']
                # Filter for audio files only
                if key.lower().endswith(('.mp3', '.wav', '.ogg', '.m4a', '.flac', '.aac', '.webm')):
                    all_files.append({
                        'key': key,
                        'name': key.split('/')[-1],
                        'size': obj['Size'],
                        'size_formatted': format_file_size(obj['Size']),
                        'last_modified': obj['LastModified'].isoformat(),
                        'last_modified_formatted': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                    })
            
            if response.get('IsTruncated'):
                continuation_token = response.get('NextContinuationToken')
            else:
                break
        
        # Sort by last modified (newest first)
        all_files.sort(key=lambda x: x['last_modified'], reverse=True)
        
        # Apply search filter
        if search_query:
            search_term = search_query.lower().strip()
            all_files = [
                f for f in all_files
                if search_term in f['name'].lower()
            ]
        
        # Pagination
        total_files = len(all_files)
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_files = all_files[start_idx:end_idx]
        
        return {
            'files': paginated_files,
            'total': total_files,
            'page': page,
            'page_size': page_size,
            'has_more': end_idx < total_files,
            'total_pages': (total_files + page_size - 1) // page_size if total_files > 0 else 1,
            'bucket': S3_BUCKET_NAME
        }
        
    except ClientError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error listing S3 files: {e.response['Error']['Message']}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing S3 files: {str(e)}")


def get_presigned_url(file_key: str, expiration: int = 3600, download: bool = False) -> dict:
    """
    Generate a presigned URL for secure file access.
    
    Args:
        file_key: The S3 object key
        expiration: URL expiration time in seconds (default 1 hour)
        download: If True, forces browser to download instead of playing the file
    
    Returns:
        Dictionary containing the presigned URL
    """
    client = get_s3_client()
    
    try:
        params = {
            'Bucket': S3_BUCKET_NAME,
            'Key': file_key
        }
        
        # Add Content-Disposition header to force download
        if download:
            filename = file_key.split('/')[-1]
            params['ResponseContentDisposition'] = f'attachment; filename="{filename}"'
        
        url = client.generate_presigned_url(
            'get_object',
            Params=params,
            ExpiresIn=expiration
        )
        
        return {
            'url': url,
            'key': file_key,
            'expires_in': expiration
        }
        
    except ClientError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error generating presigned URL: {e.response['Error']['Message']}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating presigned URL: {str(e)}")
