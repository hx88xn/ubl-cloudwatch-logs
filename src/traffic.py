from fastapi import HTTPException
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List
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

# Pakistan Standard Time (UTC+5)
PKT = timezone(timedelta(hours=5))

# Intent types to track
INTENT_TYPES = ['send_money', 'pay_bill', 'mobile_topup', 'download_statement', 'unknown']

# Regex to extract detected intent from log messages
DETECTED_INTENT_PATTERN = re.compile(r'Detected Intent:\s*(\w+)', re.IGNORECASE)

# CloudWatch filter pattern for detected intent logs (server-side filtering)
DETECTED_INTENT_FILTER = '"Detected Intent:"'

def get_cloudwatch_client():
    return boto3.client(
        'logs',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

def parse_detected_intent(message: str) -> Optional[str]:
    match = DETECTED_INTENT_PATTERN.search(message)
    if match:
        intent = match.group(1).lower()
        if intent in INTENT_TYPES:
            return intent
        # Map any unrecognized intent to 'unknown'
        return 'unknown'
    return None

def _fetch_intent_logs_from_cloudwatch(hours: int) -> List[dict]:
    """
    Fetch intent logs from CloudWatch with chunked querying for large time ranges.
    For ranges > 48 hours, fetches in daily chunks to avoid timeouts.
    """
    client = get_cloudwatch_client()
    
    now_utc = datetime.now(timezone.utc)
    range_start = now_utc - timedelta(hours=hours)
    range_end = now_utc
    
    # For large time ranges (> 48 hours), use chunked fetching
    if hours > 48:
        return _fetch_intent_logs_chunked(client, range_start, range_end)
    
    # For smaller ranges, use single query
    api_start_time_ms = int(range_start.timestamp() * 1000)
    api_end_time_ms = int(range_end.timestamp() * 1000)
    
    all_events = []
    next_token = None
    max_iterations = 100  # Safety limit to prevent infinite loops/timeouts
    iteration = 0
    
    while iteration < max_iterations:
        iteration += 1
        params = {
            'logGroupName': LOG_GROUP_NAME,
            'startTime': api_start_time_ms,
            'endTime': api_end_time_ms,
            'filterPattern': DETECTED_INTENT_FILTER,
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
    
    if iteration >= max_iterations:
        print(f"⚠️ Warning: Reached max iterations ({max_iterations}) for traffic fetch, results may be incomplete")
    
    return all_events


def _fetch_intent_logs_chunked(client, range_start: datetime, range_end: datetime) -> List[dict]:
    """
    Fetch intent logs in daily chunks - for large time ranges (>48 hours).
    """
    all_events = []
    
    chunk_hours = 24  # Fetch in 24-hour chunks
    current_end = range_end
    chunk_count = 0
    max_chunks = 14  # Safety limit: max 14 days
    
    total_hours = (range_end - range_start).total_seconds() / 3600
    print(f"📦 Fetching {total_hours:.1f} hours of intent logs in {chunk_hours}-hour chunks...")
    
    max_iterations_per_chunk = 30  # Limit iterations per chunk to prevent timeouts
    
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
                'filterPattern': DETECTED_INTENT_FILTER,
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
    
    print(f"✅ Total fetched: {len(all_events)} intent events from {chunk_count} chunks")
    
    return all_events

def get_intent_traffic_data(hours: int = 1) -> Dict:
    cached_events = None  # Initialize to track cache status
    
    # For short time ranges (<=6h), always fetch fresh (no caching)
    if hours <= 6:
        print(f"⚡ Fetching fresh {hours}h traffic data from CloudWatch (no cache)...")
        try:
            all_events = _fetch_intent_logs_from_cloudwatch(hours)
        except Exception as e:
            print(f"❌ Traffic fetch error: {type(e).__name__}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error fetching traffic data: {type(e).__name__}: {str(e)}")
    else:
        # For longer time ranges (>6 hours), use caching
        cache_key = generate_cache_key("traffic", hours, None)
        cached_events = get_cached_logs(cache_key)
        if cached_events is not None:
            print(f"✅ Cache HIT: {cache_key} ({len(cached_events)} logs)")
            all_events = cached_events
        else:
            print(f"⚡ Cache MISS: {cache_key} - fetching from CloudWatch...")
            try:
                all_events = _fetch_intent_logs_from_cloudwatch(hours)
                set_cached_logs(cache_key, all_events, hours)
            except Exception as e:
                print(f"❌ Traffic fetch error: {type(e).__name__}: {str(e)}")
                raise HTTPException(status_code=500, detail=f"Error fetching traffic data: {type(e).__name__}: {str(e)}")
    
    # Determine bucket size based on time range
    if hours <= 1:
        bucket_minutes = 5
        num_buckets = 12
    elif hours <= 6:
        bucket_minutes = 30
        num_buckets = 12
    elif hours <= 24:
        bucket_minutes = 60
        num_buckets = 24
    elif hours <= 48:
        bucket_minutes = 120
        num_buckets = 24
    elif hours <= 168:  # up to 7 days
        bucket_minutes = 120  # 2 hour buckets
        num_buckets = hours * 60 // bucket_minutes  # dynamic buckets to cover full range
    elif hours <= 336:  # up to 14 days
        bucket_minutes = 360  # 6 hour buckets
        num_buckets = hours * 60 // bucket_minutes
    else:  # 1 month (720 hours = 30 days)
        bucket_minutes = 720  # 12 hour buckets
        num_buckets = hours * 60 // bucket_minutes
    
    now_utc = datetime.now(timezone.utc)
    
    # Initialize data structures
    time_buckets = []
    bucket_data = defaultdict(lambda: defaultdict(int))
    totals = defaultdict(int)
    
    # Generate time bucket labels (from oldest to newest) in PKT
    now_pkt = now_utc.astimezone(PKT)
    for i in range(num_buckets - 1, -1, -1):
        bucket_start = now_pkt - timedelta(minutes=(i + 1) * bucket_minutes)
        if bucket_minutes < 60:
            label = bucket_start.strftime('%H:%M')
        else:
            label = bucket_start.strftime('%b %d %H:%M')
        time_buckets.append(label)
    
    # Process each log event
    for event in all_events:
        message = event.get('message', '')
        timestamp = event.get('timestamp', 0)
        
        intent = parse_detected_intent(message)
        if intent:
            # Calculate bucket index using UTC
            event_time = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
            delta = now_utc - event_time
            total_minutes = int(delta.total_seconds() / 60)
            bucket_index = min(total_minutes // bucket_minutes, num_buckets - 1)
            # Convert to forward index (0 = oldest)
            forward_index = num_buckets - 1 - bucket_index
            
            bucket_data[forward_index][intent] += 1
            totals[intent] += 1
    
    # Build datasets for each intent type
    datasets = {}
    for intent in INTENT_TYPES:
        datasets[intent] = [bucket_data[i][intent] for i in range(num_buckets)]
    
    # Convert totals to regular dict
    totals_dict = {intent: totals[intent] for intent in INTENT_TYPES}
    
    # Time range display
    if hours <= 1:
        time_range_display = '1h'
    elif hours <= 6:
        time_range_display = '6h'
    elif hours <= 24:
        time_range_display = '24h'
    elif hours <= 48:
        time_range_display = '2d'
    elif hours <= 168:
        time_range_display = '7d'
    elif hours <= 336:
        time_range_display = '14d'
    else:
        time_range_display = '1m'
    
    return {
        'labels': time_buckets,
        'datasets': datasets,
        'totals': totals_dict,
        'time_range': time_range_display,
        'total_events': len(all_events),
        'bucket_minutes': bucket_minutes,
        'cached': cached_events is not None
    }
