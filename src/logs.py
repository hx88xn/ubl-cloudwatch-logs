from fastapi import HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import Optional, List
import boto3
from src.config import (
    AWS_ACCESS_KEY_ID, 
    AWS_SECRET_ACCESS_KEY, 
    AWS_REGION, 
    LOG_GROUP_NAME,
    CACHE_TTL_SECONDS
)

# Filter patterns - logs containing these strings will be excluded
FILTER_PATTERNS = [
    'Data inserted into MySQL database',
    '✅ Background task scheduled successfully',
    '✅ Background task completed: Successfully uploaded',
    'S3 Storage: Uploading audio file to S3...',
    'Using Low Cost Pipeline',
    'Processing command validation with GPT...',
    'Preprocessed text: ',
    '🔄 Transcribing audio (supports Urdu + English)...',
    'GET /health',
    'GET /docs'
]

def should_filter_log(message: str) -> bool:
    """
    Check if a log message should be filtered out.
    Returns True if the message contains any of the filter patterns.
    """
    for pattern in FILTER_PATTERNS:
        if pattern in message:
            return True
    return False

class LogEntry(BaseModel):
    timestamp: int
    message: str
    formatted_time: str

class LogsResponse(BaseModel):
    logs: List[LogEntry]
    total: int
    page: int
    page_size: int
    has_more: bool

def get_cloudwatch_client():
    return boto3.client(
        'logs',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

_logs_cache = {
    'data': None,
    'timestamp': None,
    'hours': None,
    'search': None
}

def fetch_logs(
    hours: int = 1,
    limit: int = 500,
    search_query: Optional[str] = None,
    page: int = 1,
    page_size: int = 50
):
    global _logs_cache
    client = get_cloudwatch_client()
    
    end_time = datetime.now()
    
    if hours >= 24:
        days_back = hours // 24
        start_time = end_time.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_back)
    else:
        start_time = end_time - timedelta(hours=hours)
    
    start_time_ms = int(start_time.timestamp() * 1000)
    end_time_ms = int(end_time.timestamp() * 1000)
    
    if hours <= 1:
        smart_limit = min(limit, 5000)
        cache_ttl = CACHE_TTL_SECONDS
    elif hours <= 6:
        smart_limit = min(limit, 3000)
        cache_ttl = CACHE_TTL_SECONDS * 2
    elif hours <= 24:
        smart_limit = min(limit, 2000)
        cache_ttl = CACHE_TTL_SECONDS * 3
    else:
        smart_limit = min(limit, 1000)
        cache_ttl = CACHE_TTL_SECONDS * 5
    
    cache_key = f"{hours}:{search_query}"
    now = datetime.now()
    if (_logs_cache['data'] is not None and 
        _logs_cache['timestamp'] is not None and
        _logs_cache['hours'] == hours and
        _logs_cache['search'] == search_query and
        (now - _logs_cache['timestamp']).total_seconds() < cache_ttl):
        all_events = _logs_cache['data']
    else:
        try:
            all_events = []
            
            paginator = client.get_paginator('filter_log_events')
            
            pagination_config = {
                'logGroupName': LOG_GROUP_NAME,
                'startTime': start_time_ms,
                'endTime': end_time_ms,
                'PaginationConfig': {
                    'MaxItems': smart_limit,
                    'PageSize': 100
                }
            }
            
            if search_query:
                pagination_config['filterPattern'] = search_query
            
            page_iterator = paginator.paginate(**pagination_config)
            
            for page_response in page_iterator:
                events = page_response.get('events', [])
                all_events.extend(events)
                if len(all_events) >= smart_limit:
                    break
            
            all_events.sort(key=lambda x: x['timestamp'], reverse=True)
            
            all_events = all_events[:smart_limit]
            
            # Filter out unwanted log messages BEFORE caching
            all_events = [
                event for event in all_events 
                if not should_filter_log(event.get('message', ''))
            ]
            
            _logs_cache = {
                'data': all_events,
                'timestamp': now,
                'hours': hours,
                'search': search_query
            }
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error fetching logs: {str(e)}")
    
    total_events = len(all_events)
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    
    paginated_events = all_events[start_idx:end_idx]
    
    formatted_events = []
    for event in paginated_events:
        formatted_events.append({
            'timestamp': event['timestamp'],
            'message': event['message'].strip(),
            'formatted_time': datetime.fromtimestamp(event['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        })
    
    return {
        'logs': formatted_events,
        'total': total_events,
        'page': page,
        'page_size': page_size,
        'has_more': end_idx < total_events,
        'total_pages': (total_events + page_size - 1) // page_size
    }

def get_log_streams():
    client = get_cloudwatch_client()
    
    try:
        response = client.describe_log_streams(
            logGroupName=LOG_GROUP_NAME,
            orderBy='LastEventTime',
            descending=True,
            limit=20
        )
        
        streams = []
        for stream in response.get('logStreams', []):
            streams.append({
                'name': stream['logStreamName'],
                'lastEventTime': stream.get('lastEventTime', 0),
                'lastIngestionTime': stream.get('lastIngestionTime', 0),
            })
        
        return streams
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching log streams: {str(e)}")
