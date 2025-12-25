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
    client = get_cloudwatch_client()
    
    now_utc = datetime.now(timezone.utc)
    end_time = now_utc - timedelta(hours=hours)
    
    api_start_time_ms = int(end_time.timestamp() * 1000)
    api_end_time_ms = int(now_utc.timestamp() * 1000)
    
    all_events = []
    next_token = None
    
    while True:
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
    
    return all_events

def get_intent_traffic_data(hours: int = 1) -> Dict:
    # Generate cache key
    cache_key = generate_cache_key("traffic", hours, None)
    
    # Try cache first
    cached_events = get_cached_logs(cache_key)
    if cached_events is not None:
        print(f"✅ Cache HIT: {cache_key} ({len(cached_events)} logs)")
        all_events = cached_events
    else:
        print(f"⚡ Cache MISS: {cache_key} - fetching from CloudWatch...")
        try:
            all_events = _fetch_intent_logs_from_cloudwatch(hours)
            
            # Cache the intent logs
            set_cached_logs(cache_key, all_events, hours)
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error fetching traffic data: {str(e)}")
    
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
    else:  # 48 hours
        bucket_minutes = 120
        num_buckets = 24
    
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
    else:
        time_range_display = '2d'
    
    return {
        'labels': time_buckets,
        'datasets': datasets,
        'totals': totals_dict,
        'time_range': time_range_display,
        'total_events': len(all_events),
        'bucket_minutes': bucket_minutes,
        'cached': cached_events is not None
    }
