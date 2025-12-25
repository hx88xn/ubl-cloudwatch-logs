from fastapi import HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import Optional, List
import boto3
from src.config import (
    AWS_ACCESS_KEY_ID, 
    AWS_SECRET_ACCESS_KEY, 
    AWS_REGION, 
    LOG_GROUP_NAME
)
from src.cache import (
    get_cached_logs,
    set_cached_logs,
    generate_cache_key
)

FILTER_PATTERNS = [
    'Data inserted into MySQL database',
    '✅ Background task scheduled successfully',
    '✅ Background task completed: Successfully uploaded',
    '✅ Uploaded audio_files',
    'Time taken to transcribe audio:',
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

# CloudWatch filter pattern for app logs (server-side filtering)
APP_LOGS_FILTER_PATTERN = '?"Beneficiaries:" ?"Final Response:" ?"Phone contacts:" ?"Bill types:"'

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

def case_insensitive_search(message: str, search_query: str) -> bool:
    if not search_query:
        return True
    return search_query.lower() in message.lower()

def _fetch_from_cloudwatch(hours: int, filter_pattern: Optional[str] = None) -> List[dict]:
    client = get_cloudwatch_client()
    
    start_time = datetime.now()
    
    if hours >= 24:
        days_back = hours // 24
        end_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_back)
    else:
        end_time = start_time - timedelta(hours=hours)
    
    api_start_time_ms = int(end_time.timestamp() * 1000)
    api_end_time_ms = int(start_time.timestamp() * 1000)
    
    all_events = []
    next_token = None
    
    while True:
        params = {
            'logGroupName': LOG_GROUP_NAME,
            'startTime': api_start_time_ms,
            'endTime': api_end_time_ms,
            'limit': 10000
        }
        
        # Use server-side filter if provided
        if filter_pattern:
            params['filterPattern'] = filter_pattern
        
        if next_token:
            params['nextToken'] = next_token
        
        response = client.filter_log_events(**params)
        events = response.get('events', [])
        all_events.extend(events)
        
        next_token = response.get('nextToken')
        if not next_token:
            break
    
    # Sort by timestamp descending (newest first)
    all_events.sort(key=lambda x: x['timestamp'], reverse=True)
    
    return all_events

def _format_events(events: List[dict]) -> List[dict]:
    formatted = []
    for event in events:
        formatted.append({
            'timestamp': event['timestamp'],
            'message': event['message'].strip(),
            'formatted_time': datetime.fromtimestamp(event['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        })
    return formatted

def fetch_logs(
    hours: int = 1,
    limit: int = 500,
    search_query: Optional[str] = None,
    page: int = 1,
    page_size: int = 50
):
    # Generate cache key
    cache_key = generate_cache_key("dashboard", hours, search_query)
    
    # Try to get from cache first (only if no search query or search is cached)
    if not search_query:
        cached_events = get_cached_logs(cache_key)
        if cached_events is not None:
            print(f"✅ Cache HIT: {cache_key} ({len(cached_events)} logs)")
            all_events = cached_events
        else:
            print(f"⚡ Cache MISS: {cache_key} - fetching from CloudWatch...")
            try:
                all_events = _fetch_from_cloudwatch(hours)
                
                # Filter out unwanted log messages
                all_events = [
                    event for event in all_events 
                    if not should_filter_log(event.get('message', ''))
                ]
                
                # Cache the filtered data
                set_cached_logs(cache_key, all_events, hours)
                
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error fetching logs: {str(e)}")
    else:
        # For search queries, first try cache without search
        base_cache_key = generate_cache_key("dashboard", hours, None)
        cached_events = get_cached_logs(base_cache_key)
        
        if cached_events is not None:
            print(f"✅ Cache HIT (base): {base_cache_key}")
            all_events = cached_events
        else:
            print(f"⚡ Cache MISS: fetching from CloudWatch for search...")
            try:
                all_events = _fetch_from_cloudwatch(hours)
                
                # Filter out unwanted log messages
                all_events = [
                    event for event in all_events 
                    if not should_filter_log(event.get('message', ''))
                ]
                
                # Cache the base data
                set_cached_logs(base_cache_key, all_events, hours)
                
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error fetching logs: {str(e)}")
        
        # Apply search filter on cached data
        search_term = search_query.strip()
        all_events = [
            event for event in all_events
            if case_insensitive_search(event.get('message', ''), search_term)
        ]
    
    # Paginate
    total_events = len(all_events)
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    
    paginated_events = all_events[start_idx:end_idx]
    formatted_events = _format_events(paginated_events)
    
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
    # Generate cache key
    cache_key = generate_cache_key("app", hours, None)
    
    # Try to get from cache first
    cached_events = get_cached_logs(cache_key)
    if cached_events is not None:
        print(f"✅ Cache HIT: {cache_key} ({len(cached_events)} logs)")
        app_events = cached_events
    else:
        print(f"⚡ Cache MISS: {cache_key} - fetching from CloudWatch with server-side filter...")
        try:
            # Use server-side filtering for app logs - much more efficient!
            app_events = _fetch_from_cloudwatch(hours, filter_pattern=APP_LOGS_FILTER_PATTERN)
            
            # Cache the app logs data
            set_cached_logs(cache_key, app_events, hours)
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error fetching app logs: {str(e)}")
    
    # Paginate
    total_events = len(app_events)
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    
    paginated_events = app_events[start_idx:end_idx]
    formatted_events = _format_events(paginated_events)
    
    return {
        'logs': formatted_events,
        'total': total_events,
        'page': page,
        'page_size': page_size,
        'has_more': end_idx < total_events,
        'total_pages': (total_events + page_size - 1) // page_size
    }
