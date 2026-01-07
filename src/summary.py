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

# Regex patterns to extract target fields
UUID_PATTERN = re.compile(r'\[([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\]')
TIME_TAKEN_PATTERN = re.compile(r'Time taken[^:]*:\s*([\d.]+)\s*(?:s|seconds?)?', re.IGNORECASE)
DETECTED_INTENT_PATTERN = re.compile(r'Detected Intent:\s*(\w+)', re.IGNORECASE)
ORIGINAL_TRANSCRIPTION_PATTERN = re.compile(r'Original transcription:\s*(.+)', re.IGNORECASE)

# CloudWatch filter pattern for summary logs (server-side filtering)
SUMMARY_LOGS_FILTER = '?"Time taken" ?"Detected Intent" ?"Original transcription"'

def get_cloudwatch_client():
    return boto3.client(
        'logs',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

def _fetch_summary_from_cloudwatch(hours: int) -> List[dict]:
    """Fetch summary logs from CloudWatch."""
    client = get_cloudwatch_client()
    
    now = datetime.now()
    if hours >= 24:
        days_back = hours // 24
        range_start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_back)
    else:
        range_start = now - timedelta(hours=hours)
    
    range_end = now
    
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

def parse_log_message(message: str) -> Dict:
    """Parse a log message and extract Time taken, Detected Intent, and Original transcription."""
    result = {
        'uuid': None,
        'time_taken': None,
        'detected_intent': None,
        'original_transcription': None
    }
    
    # Extract UUID
    uuid_match = UUID_PATTERN.search(message)
    if uuid_match:
        result['uuid'] = uuid_match.group(1)
    
    # Extract Time taken
    time_match = TIME_TAKEN_PATTERN.search(message)
    if time_match:
        result['time_taken'] = time_match.group(1)
    
    # Extract Detected Intent
    intent_match = DETECTED_INTENT_PATTERN.search(message)
    if intent_match:
        result['detected_intent'] = intent_match.group(1)
    
    # Extract Original transcription
    transcription_match = ORIGINAL_TRANSCRIPTION_PATTERN.search(message)
    if transcription_match:
        result['original_transcription'] = transcription_match.group(1).strip()
    
    return result

def fetch_summary_logs(hours: int = 1) -> Dict:
    """Fetch and parse summary logs, grouping by UUID."""
    
    # For short time ranges (<=6h), always fetch fresh (no caching)
    if hours <= 6:
        print(f"⚡ Fetching fresh {hours}h summary logs from CloudWatch (no cache)...")
        try:
            all_events = _fetch_summary_from_cloudwatch(hours)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error fetching summary logs: {str(e)}")
    else:
        # For longer time ranges (>6 hours), use caching
        cache_key = generate_cache_key("summary", hours, None)
        cached_events = get_cached_logs(cache_key)
        if cached_events is not None:
            print(f"✅ Cache HIT: {cache_key} ({len(cached_events)} logs)")
            all_events = cached_events
        else:
            print(f"⚡ Cache MISS: {cache_key} - fetching from CloudWatch...")
            try:
                all_events = _fetch_summary_from_cloudwatch(hours)
                set_cached_logs(cache_key, all_events, hours)
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error fetching summary logs: {str(e)}")
    
    # Group logs by UUID
    uuid_groups = defaultdict(lambda: {
        'logs': [],
        'time_taken': None,
        'detected_intent': None,
        'original_transcription': None,
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
            if parsed['time_taken'] and group['time_taken'] is None:
                group['time_taken'] = parsed['time_taken']
            if parsed['detected_intent'] and group['detected_intent'] is None:
                group['detected_intent'] = parsed['detected_intent']
            if parsed['original_transcription'] and group['original_transcription'] is None:
                group['original_transcription'] = parsed['original_transcription']
            
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
            'time_taken': data['time_taken'],
            'detected_intent': data['detected_intent'],
            'original_transcription': data['original_transcription'],
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
