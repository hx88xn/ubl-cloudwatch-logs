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
    'GET /docs',
    # Filter HTTP access logs (security scans, bots, probes)
    'HTTP/1.1" 404',
    'HTTP/1.1" 405',
    'HTTP/1.1" 200 OK',
    '"GET / HTTP',
    '"POST / HTTP',
    '/.git/',
    '/.env',
    '/api-docs/',
    '/service/api-docs',
]

APP_LOGS_PATTERNS = [
    'Beneficiaries: ',
    'Final Response: ',
    'Phone contacts: ',
    'Bill types: '
]

def should_filter_log(message: str) -> bool:
    for pattern in FILTER_PATTERNS:
        if pattern in message:
            return True
    return False

def should_include_app_log(message: str) -> bool:
    for pattern in APP_LOGS_PATTERNS:
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
    
    # start_time = current time (logs start displaying from here - newest)
    # end_time = time range back (logs end here - oldest)
    start_time = datetime.now()
    
    if hours >= 24:
        days_back = hours // 24
        end_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_back)
    else:
        end_time = start_time - timedelta(hours=hours)
    
    # CloudWatch API requires: startTime < endTime
    # So we pass end_time (older) as API startTime, start_time (newer) as API endTime
    api_start_time_ms = int(end_time.timestamp() * 1000)
    api_end_time_ms = int(start_time.timestamp() * 1000)
    
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
            next_token = None
            
            # Fetch ALL logs in the time range first (CloudWatch returns oldest first)
            while True:
                params = {
                    'logGroupName': LOG_GROUP_NAME,
                    'startTime': api_start_time_ms,
                    'endTime': api_end_time_ms,
                    'limit': 10000
                }
                
                if search_query:
                    params['filterPattern'] = search_query
                
                if next_token:
                    params['nextToken'] = next_token
                
                response = client.filter_log_events(**params)
                events = response.get('events', [])
                all_events.extend(events)
                
                next_token = response.get('nextToken')
                if not next_token:
                    break
            
            # NOW sort by timestamp descending (newest first)
            all_events.sort(key=lambda x: x['timestamp'], reverse=True)
            
            # Apply limit AFTER sorting to keep the newest logs
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

def fetch_app_logs(
    hours: int = 1,
    limit: int = 500,
    page: int = 1,
    page_size: int = 50
):
    global _logs_cache
    client = get_cloudwatch_client()
    
    # start_time = current time (logs start displaying from here - newest)
    # end_time = time range back (logs end here - oldest)
    start_time = datetime.now()
    
    if hours >= 24:
        days_back = hours // 24
        end_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_back)
    else:
        end_time = start_time - timedelta(hours=hours)
    
    # CloudWatch API requires: startTime < endTime
    api_start_time_ms = int(end_time.timestamp() * 1000)
    api_end_time_ms = int(start_time.timestamp() * 1000)
    
    if hours <= 1:
        smart_limit = min(limit, 5000)
    elif hours <= 6:
        smart_limit = min(limit, 3000)
    elif hours <= 24:
        smart_limit = min(limit, 2000)
    else:
        smart_limit = min(limit, 1000)
    
    try:
        all_events = []
        next_token = None
        
        # Fetch ALL logs in the time range first (CloudWatch returns oldest first)
        while True:
            params = {
                'logGroupName': LOG_GROUP_NAME,
                'startTime': api_start_time_ms,
                'endTime': api_end_time_ms,
                'limit': 10000
            }
            
            if next_token:
                params['nextToken'] = next_token
            
            response = client.filter_log_events(**params)
            events = response.get('events', [])
            all_events.extend(events)
            
            next_token = response.get('nextToken')
            if not next_token:
                break
        
        # NOW sort by timestamp descending (newest first)
        all_events.sort(key=lambda x: x['timestamp'], reverse=True)
        
        # Apply limit AFTER sorting to keep the newest logs
        all_events = all_events[:smart_limit]
        
        # Filter to include ONLY app logs
        app_events = [
            event for event in all_events 
            if should_include_app_log(event.get('message', ''))
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching app logs: {str(e)}")
    
    total_events = len(app_events)
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    
    paginated_events = app_events[start_idx:end_idx]
    
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
