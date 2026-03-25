from fastapi import HTTPException
from datetime import datetime, timezone, timedelta
from typing import Optional, List
import time
import boto3

# Pakistan Standard Time (UTC+5)
PKT = timezone(timedelta(hours=5))
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from src.config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    S3_BUCKET_NAME,
    S3_AUDIO_LISTING_TIMEOUT_SECONDS,
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


AUDIO_EXTENSIONS = ('.mp3', '.wav', '.ogg', '.m4a', '.flac', '.aac', '.webm')


def list_audio_files_chunk(
    prefix: str = "",
    s3_continuation_token: Optional[str] = None,
):
    client = get_s3_client()

    try:
        chunk_files: List[dict] = []
        continuation = s3_continuation_token
        s3_objects_scanned = 0
        deadline = time.monotonic() + S3_AUDIO_LISTING_TIMEOUT_SECONDS
        listing_complete = False
        next_token_out: Optional[str] = None

        while time.monotonic() < deadline:
            params = {
                'Bucket': S3_BUCKET_NAME,
                'MaxKeys': 1000,
            }
            if prefix:
                params['Prefix'] = prefix
            if continuation:
                params['ContinuationToken'] = continuation

            response = client.list_objects_v2(**params)
            contents = response.get('Contents') or []
            s3_objects_scanned += len(contents)

            for obj in contents:
                key = obj['Key']
                if key.lower().endswith(AUDIO_EXTENSIONS):
                    last_modified_pkt = obj['LastModified'].astimezone(PKT)
                    chunk_files.append({
                        'key': key,
                        'name': key.split('/')[-1],
                        'size': obj['Size'],
                        'size_formatted': format_file_size(obj['Size']),
                        'last_modified': last_modified_pkt.isoformat(),
                        'last_modified_formatted': last_modified_pkt.strftime('%Y-%m-%d %H:%M:%S')
                    })

            if not response.get('IsTruncated'):
                listing_complete = True
                next_token_out = None
                break

            continuation = response.get('NextContinuationToken')
            next_token_out = continuation

        if not listing_complete and next_token_out is None:
            listing_complete = True

        return {
            'files': chunk_files,
            'listing_complete': listing_complete,
            'next_s3_continuation_token': None if listing_complete else next_token_out,
            's3_objects_scanned': s3_objects_scanned,
            'bucket': S3_BUCKET_NAME,
        }

    except ClientError as e:
        err = e.response.get('Error', {}) if e.response else {}
        raise HTTPException(
            status_code=500,
            detail=f"Error listing S3 files: {err.get('Message', str(e))}"
        )
    except NoCredentialsError:
        raise HTTPException(
            status_code=500,
            detail="AWS credentials not configured. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (or use an instance/task role)."
        )
    except BotoCoreError as e:
        raise HTTPException(status_code=500, detail=f"AWS error listing S3: {str(e)}")
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
        err = e.response.get('Error', {}) if e.response else {}
        raise HTTPException(
            status_code=500,
            detail=f"Error generating presigned URL: {err.get('Message', str(e))}"
        )
    except NoCredentialsError:
        raise HTTPException(
            status_code=500,
            detail="AWS credentials not configured. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (or use an instance/task role)."
        )
    except BotoCoreError as e:
        raise HTTPException(status_code=500, detail=f"AWS error generating URL: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating presigned URL: {str(e)}")
