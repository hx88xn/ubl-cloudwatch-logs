from fastapi import HTTPException
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from collections import defaultdict
import re
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
from src.logs import _fetch_from_grafana

# Regex patterns to extract target fields
UUID_PATTERN = re.compile(r'\[([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\]')
DETECTED_INTENT_PATTERN = re.compile(r'Detected Intent:\s*(\w+)', re.IGNORECASE)
ORIGINAL_TRANSCRIPTION_PATTERN = re.compile(r'Original transcription:\s*(.+)', re.IGNORECASE)
FINAL_RESPONSE_PATTERN = re.compile(r'Final Response:\s*(.+)', re.IGNORECASE)

# CloudWatch filter pattern for summary logs (server-side filtering)
SUMMARY_LOGS_FILTER = '?"Detected Intent" ?"Original transcription" ?"Final Response"'

def get_cloudwatch_client():
    return boto3.client(
        'logs',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

def _fetch_summary_from_cloudwatch(hours: int) -> List[dict]:
    """Fetch summary logs from CloudWatch with chunked querying for large time ranges."""
    client = get_cloudwatch_client()
    
    now = datetime.now()
    if hours >= 24:
        days_back = hours // 24
        range_start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_back)
    else:
        range_start = now - timedelta(hours=hours)
    
    range_end = now
    
    # For large time ranges (> 48 hours), use chunked fetching
    if hours > 48:
        return _fetch_summary_chunked(client, range_start, range_end, hours)
    
    api_start_time_ms = int(range_start.timestamp() * 1000)
    api_end_time_ms = int(range_end.timestamp() * 1000)
    
    all_events = []
    next_token = None
    max_iterations = 100
    iteration = 0
    
    while iteration < max_iterations:
        iteration += 1
        params = {
            'logGroupName': LOG_GROUP_NAME,
            'startTime': api_start_time_ms,
            'endTime': api_end_time_ms,
            'filterPattern': SUMMARY_LOGS_FILTER,
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
    
    # Sort by timestamp descending
    all_events.sort(key=lambda x: x['timestamp'], reverse=True)
    return all_events


def _fetch_summary_chunked(client, range_start: datetime, range_end: datetime, hours: int) -> List[dict]:
    """Fetch summary logs in daily chunks - for large time ranges (> 48 hours)."""
    all_events = []
    
    chunk_hours = 24  # Fetch in 24-hour chunks
    current_end = range_end
    chunk_count = 0
    max_chunks = 31  # Safety limit: max 31 days for 1 month
    max_iterations_per_chunk = 20  # Reduced to speed up - most days won't have this many pages
    
    print(f"📦 Fetching {hours} hours of summary logs in {chunk_hours}-hour chunks...")
    
    while current_end > range_start and chunk_count < max_chunks:
        chunk_count += 1
        chunk_start = max(current_end - timedelta(hours=chunk_hours), range_start)
        
        api_start_time_ms = int(chunk_start.timestamp() * 1000)
        api_end_time_ms = int(current_end.timestamp() * 1000)
        
        next_token = None
        chunk_events = []
        iteration = 0
        
        while iteration < max_iterations_per_chunk:
            iteration += 1
            params = {
                'logGroupName': LOG_GROUP_NAME,
                'startTime': api_start_time_ms,
                'endTime': api_end_time_ms,
                'filterPattern': SUMMARY_LOGS_FILTER,
                'limit': 10000
            }
            
            if next_token:
                params['nextToken'] = next_token
            
            response = client.filter_log_events(**params)
            events = response.get('events', [])
            chunk_events.extend(events)
            
            next_token = response.get('nextToken')
            if not next_token:
                break
        
        print(f"  📄 Chunk {chunk_count}: {chunk_start.strftime('%Y-%m-%d %H:%M')} to {current_end.strftime('%Y-%m-%d %H:%M')} - {len(chunk_events)} events")
        all_events.extend(chunk_events)
        
        # Move to next chunk
        current_end = chunk_start
    
    print(f"✅ Total fetched: {len(all_events)} summary events from {chunk_count} chunks")
    
    # Sort by timestamp descending
    all_events.sort(key=lambda x: x['timestamp'], reverse=True)
    return all_events


def parse_log_message(message: str) -> Dict:
    """Parse a log message and extract Detected Intent, Original transcription, and Final Response."""
    result = {
        'uuid': None,
        'detected_intent': None,
        'original_transcription': None,
        'final_response': None
    }
    
    # Extract UUID
    uuid_match = UUID_PATTERN.search(message)
    if uuid_match:
        result['uuid'] = uuid_match.group(1)
    
    # Extract Detected Intent
    intent_match = DETECTED_INTENT_PATTERN.search(message)
    if intent_match:
        result['detected_intent'] = intent_match.group(1)
    
    # Extract Original transcription
    transcription_match = ORIGINAL_TRANSCRIPTION_PATTERN.search(message)
    if transcription_match:
        result['original_transcription'] = transcription_match.group(1).strip()
    
    # Extract Final Response
    final_response_match = FINAL_RESPONSE_PATTERN.search(message)
    if final_response_match:
        result['final_response'] = final_response_match.group(1).strip()
    
    return result

def _process_events_to_summaries(all_events: List[dict]) -> Dict:
    """Process raw events into grouped summaries by UUID."""
    uuid_groups = defaultdict(lambda: {
        'logs': [],
        'detected_intent': None,
        'original_transcription': None,
        'final_response': None,
        'timestamp': None,
        'formatted_time': None
    })
    
    no_uuid_logs = []
    
    for event in all_events:
        message = event.get('message', '').strip()
        timestamp = event.get('timestamp', 0)
        formatted_time = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
        
        parsed = parse_log_message(message)
        
        if parsed['uuid']:
            uuid = parsed['uuid']
            group = uuid_groups[uuid]
            
            # Store first (most recent) timestamp
            if group['timestamp'] is None:
                group['timestamp'] = timestamp
                group['formatted_time'] = formatted_time
            
            # Update fields if found
            if parsed['detected_intent'] and group['detected_intent'] is None:
                group['detected_intent'] = parsed['detected_intent']
            if parsed['original_transcription'] and group['original_transcription'] is None:
                group['original_transcription'] = parsed['original_transcription']
            if parsed['final_response'] and group['final_response'] is None:
                group['final_response'] = parsed['final_response']
            
            group['logs'].append({
                'timestamp': timestamp,
                'formatted_time': formatted_time,
                'message': message
            })
        else:
            no_uuid_logs.append({
                'timestamp': timestamp,
                'formatted_time': formatted_time,
                'message': message,
                'parsed': parsed
            })
    
    # Convert to list and sort by timestamp
    summaries = []
    for uuid, data in uuid_groups.items():
        summaries.append({
            'uuid': uuid,
            'detected_intent': data['detected_intent'],
            'original_transcription': data['original_transcription'],
            'final_response': data['final_response'],
            'timestamp': data['timestamp'],
            'formatted_time': data['formatted_time'],
            'log_count': len(data['logs']),
            'logs': data['logs']
        })
    
    # Sort by timestamp descending (newest first)
    summaries.sort(key=lambda x: x['timestamp'] or 0, reverse=True)
    
    return {
        'summaries': summaries,
        'total': len(summaries),
        'no_uuid_count': len(no_uuid_logs)
    }


def fetch_summary_logs(hours: int = 1, allow_slow_fetch: bool = True, source: str = 'cloudwatch') -> Dict:
    """Fetch and parse summary logs, grouping by UUID."""
    
    # For short time ranges (<=6h), always fetch fresh (no caching)
    if hours <= 6:
        print(f"⚡ Fetching fresh {hours}h summary logs from {source} (no cache)...")
        try:
            if source == 'grafana':
                all_events = _fetch_from_grafana(hours, filter_pattern=SUMMARY_LOGS_FILTER)
            else:
                all_events = _fetch_summary_from_cloudwatch(hours)
            return _process_events_to_summaries(all_events)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error fetching summary logs: {str(e)}")
    
    # For longer time ranges (>6 hours), cache the PROCESSED summaries (not raw events)
    cache_key = generate_cache_key(f"summary_processed_{source}", hours, None)
    cached_result = get_cached_logs(cache_key)
    if cached_result is not None:
        print(f"✅ Cache HIT: {cache_key} ({cached_result.get('total', 0)} summaries)")
        return cached_result
    
    # For very large time ranges (7+ days), if cache is not ready, return a message
    # This prevents 504 timeouts from load balancers
    if hours >= 168 and not allow_slow_fetch:
        return {
            'summaries': [],
            'total': 0,
            'no_uuid_count': 0,
            'cache_warming': True,
            'message': f'Cache is warming up for {hours // 24} day data. Please retry in 2-3 minutes.'
        }
    
    print(f"⚡ Cache MISS: {cache_key} - fetching from {source}...")
    try:
        if source == 'grafana':
            all_events = _fetch_from_grafana(hours, filter_pattern=SUMMARY_LOGS_FILTER)
        else:
            all_events = _fetch_summary_from_cloudwatch(hours)
        result = _process_events_to_summaries(all_events)
        
        # Cache the processed result (much smaller than raw events)
        # Remove detailed logs from cache to reduce size for large time ranges
        if hours >= 168:  # 7+ days
            cache_result = {
                'summaries': [
                    {k: v for k, v in s.items() if k != 'logs'} 
                    for s in result['summaries']
                ],
                'total': result['total'],
                'no_uuid_count': result['no_uuid_count']
            }
        else:
            cache_result = result
        
        set_cached_logs(cache_key, cache_result, hours)
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching summary logs: {str(e)}")
