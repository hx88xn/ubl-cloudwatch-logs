import re
import json
import time
import boto3
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from botocore.config import Config as BotoConfig
from src.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, LOG_GROUP_NAME
from src.cache import get_redis_client
from src.logs import fetch_from_grafana_range

PKT = timezone(timedelta(hours=5))

FINAL_RESPONSE_FILTER = '"Final Response:"'
UUID_PATTERN = re.compile(r'\[([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\]')

COMPLETED_PERIOD_TTL = 365 * 24 * 3600
RETRY_TTL = 3600

_cw_retry_config = BotoConfig(
    retries={'max_attempts': 8, 'mode': 'adaptive'}
)

def _get_cloudwatch_client():
    return boto3.client(
        'logs',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        config=_cw_retry_config,
    )


def get_log_retention_days() -> Optional[int]:
    try:
        client = _get_cloudwatch_client()
        resp = client.describe_log_groups(logGroupNamePrefix=LOG_GROUP_NAME, limit=1)
        groups = resp.get('logGroups', [])
        for g in groups:
            if g['logGroupName'] == LOG_GROUP_NAME:
                return g.get('retentionInDays')
        return None
    except Exception:
        return None


def _fetch_events_for_range(start_dt: datetime, end_dt: datetime, source: str = 'cloudwatch') -> List[dict]:
    if source == 'grafana':
        return fetch_from_grafana_range(start_dt, end_dt, filter_pattern=FINAL_RESPONSE_FILTER)
        
    client = _get_cloudwatch_client()
    total_hours = (end_dt - start_dt).total_seconds() / 3600

    if total_hours > 48:
        return _fetch_events_chunked(client, start_dt, end_dt)

    all_events = []
    next_token = None
    api_start_ms = int(start_dt.timestamp() * 1000)
    api_end_ms = int(end_dt.timestamp() * 1000)

    for iteration in range(100):
        params = {
            'logGroupName': LOG_GROUP_NAME,
            'startTime': api_start_ms,
            'endTime': api_end_ms,
            'filterPattern': FINAL_RESPONSE_FILTER,
            'limit': 10000,
        }
        if next_token:
            params['nextToken'] = next_token

        response = client.filter_log_events(**params)
        all_events.extend(response.get('events', []))

        next_token = response.get('nextToken')
        if not next_token:
            break
        if iteration % 3 == 2:
            time.sleep(0.25)

    return all_events


def _fetch_events_chunked(client, range_start: datetime, range_end: datetime) -> List[dict]:
    all_events = []
    current_end = range_end
    chunk_count = 0
    max_chunks = 32

    total_hours = (range_end - range_start).total_seconds() / 3600
    print(f"📦 Requests: fetching {total_hours:.1f}h in 24h chunks...")

    while current_end > range_start and chunk_count < max_chunks:
        chunk_count += 1
        chunk_start = max(current_end - timedelta(hours=24), range_start)

        api_start_ms = int(chunk_start.timestamp() * 1000)
        api_end_ms = int(current_end.timestamp() * 1000)

        next_token = None
        for iteration in range(30):
            params = {
                'logGroupName': LOG_GROUP_NAME,
                'startTime': api_start_ms,
                'endTime': api_end_ms,
                'filterPattern': FINAL_RESPONSE_FILTER,
                'limit': 10000,
            }
            if next_token:
                params['nextToken'] = next_token

            response = client.filter_log_events(**params)
            all_events.extend(response.get('events', []))

            next_token = response.get('nextToken')
            if not next_token:
                break
            if iteration % 3 == 2:
                time.sleep(0.25)

        current_end = chunk_start
        if chunk_count % 5 == 0:
            time.sleep(0.5)

    print(f"✅ Requests: fetched {len(all_events)} events from {chunk_count} chunks")
    return all_events


def _count_unique_requests(events: List[dict]) -> int:
    uuids = set()
    for event in events:
        match = UUID_PATTERN.search(event.get('message', ''))
        if match:
            uuids.add(match.group(1))
    return len(uuids)


def _get_cached_count(cache_key: str) -> Optional[int]:
    client = get_redis_client()
    if client is None:
        return None
    try:
        data = client.get(cache_key)
        if data is not None:
            return json.loads(data)
        return None
    except Exception:
        return None


def clear_stale_request_caches():
    client = get_redis_client()
    if client is None:
        return
    try:
        keys = client.keys("requests:*")
        if not keys:
            return
        stale = []
        for key in keys:
            key_str = key.decode() if isinstance(key, bytes) else key
            val = client.get(key)
            if val is not None:
                count = json.loads(val)
                if count == 0:
                    ttl_remaining = client.ttl(key)
                    if ttl_remaining > RETRY_TTL:
                        stale.append(key)
        if stale:
            client.delete(*stale)
            print(f"🗑️ Cleared {len(stale)} stale request cache entries with 0 count")
    except Exception as e:
        print(f"⚠️ Error clearing stale caches: {e}")


def _set_cached_count(cache_key: str, count: int, ttl: int):
    client = get_redis_client()
    if client is None:
        return
    try:
        client.setex(cache_key, ttl, json.dumps(count))
        print(f"✅ Cached requests count={count} key='{cache_key}' TTL={ttl}s")
    except Exception as e:
        print(f"⚠️ Cache write error for {cache_key}: {e}")


def _build_monthly_specs(now_pkt: datetime, num_periods: int, source: str = 'cloudwatch') -> List[dict]:
    specs = []
    for i in range(num_periods - 1, -1, -1):
        total_months = now_pkt.year * 12 + now_pkt.month - 1 - i
        year = total_months // 12
        month = total_months % 12 + 1

        start = datetime(year, month, 1, tzinfo=PKT)
        end = datetime(year + 1, 1, 1, tzinfo=PKT) if month == 12 else datetime(year, month + 1, 1, tzinfo=PKT)
        is_current = (year == now_pkt.year and month == now_pkt.month)
        if is_current:
            end = now_pkt

        specs.append({
            'label': datetime(year, month, 1).strftime('%b %Y'),
            'cache_key': f"requests_{source}:monthly:{year}-{month:02d}",
            'start': start,
            'end': end,
            'is_complete': not is_current,
        })
    return specs


def _build_weekly_specs(now_pkt: datetime, num_periods: int, source: str = 'cloudwatch') -> List[dict]:
    specs = []
    week_start = (now_pkt - timedelta(days=now_pkt.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    for i in range(num_periods - 1, -1, -1):
        start = week_start - timedelta(weeks=i)
        end = start + timedelta(weeks=1)
        is_current = (i == 0)
        if is_current:
            end = now_pkt

        iso_year, iso_week, _ = start.isocalendar()
        specs.append({
            'label': f"W{iso_week:02d} {iso_year}",
            'cache_key': f"requests_{source}:weekly:{iso_year}-W{iso_week:02d}",
            'start': start,
            'end': end,
            'is_complete': not is_current,
        })
    return specs


def _build_daily_specs(now_pkt: datetime, num_periods: int) -> List[dict]:
    specs = []
    today_start = now_pkt.replace(hour=0, minute=0, second=0, microsecond=0)

    for i in range(num_periods - 1, -1, -1):
        start = today_start - timedelta(days=i)
        end = start + timedelta(days=1)
        is_current = (i == 0)
        if is_current:
            end = now_pkt

        specs.append({
            'label': start.strftime('%d %b'),
            'cache_key': f"requests:daily:{start.strftime('%Y-%m-%d')}",
            'start': start,
            'end': end,
            'is_complete': not is_current,
        })
    return specs


def get_requests_data(period: str = 'monthly', num_periods: int = 6, source: str = 'cloudwatch') -> Dict:
    now_pkt = datetime.now(PKT)

    retention_days = get_log_retention_days() if source != 'grafana' else 30 # Grafana typically 30 days
    if retention_days:
        max_lookback = now_pkt - timedelta(days=retention_days)
        print(f"📋 {source.title()} retention: {retention_days} days (logs available from {max_lookback.strftime('%Y-%m-%d')})")
    else:
        max_lookback = None
        print(f"📋 {source.title()} retention: unlimited (or unable to determine)")

    if period == 'monthly':
        specs = _build_monthly_specs(now_pkt, num_periods, source)
    elif period == 'weekly':
        specs = _build_weekly_specs(now_pkt, num_periods, source)
    else:
        raise ValueError(f"Invalid period '{period}'. Must be monthly or weekly.")

    # Trim specs to only include periods within retention window
    if max_lookback is not None:
        valid_specs = []
        for spec in specs:
            if spec['end'] > max_lookback:
                if spec['start'] < max_lookback:
                    spec['start'] = max_lookback
                valid_specs.append(spec)
            else:
                print(f"  ⏭️ Skipping {spec['label']} — outside CloudWatch retention ({retention_days}d)")
        specs = valid_specs

    if not specs:
        return {
            'period': period, 'labels': [], 'counts': [], 'total': 0,
            'peak': 0, 'average': 0, 'cached_flags': [],
            'retention_days': retention_days,
            'details': [],
        }

    results: List[Optional[dict]] = [None] * len(specs)
    uncached_indices = []

    for i, spec in enumerate(specs):
        if spec['is_complete']:
            cached_val = _get_cached_count(spec['cache_key'])
            if cached_val is not None:
                results[i] = {'label': spec['label'], 'count': cached_val, 'cached': True,
                              'events_found': None, 'error': None}
            else:
                uncached_indices.append(i)
        else:
            uncached_indices.append(i)

    if uncached_indices:
        print(f"⚡ Requests ({period}): fetching {len(uncached_indices)} uncached period(s) from {source}...")
        for fetch_num, idx in enumerate(uncached_indices):
            spec = specs[idx]
            error_msg = None
            events_found = 0
            try:
                events = _fetch_events_for_range(spec['start'], spec['end'], source=source)
                events_found = len(events)
                count_val = _count_unique_requests(events)
                print(f"  📊 {spec['label']}: {count_val} unique requests from {events_found} events "
                      f"({spec['start'].strftime('%Y-%m-%d %H:%M')} → {spec['end'].strftime('%Y-%m-%d %H:%M')} PKT)")
            except Exception as e:
                error_msg = f"{type(e).__name__}: {e}"
                print(f"⚠️ Requests: failed to fetch {spec['label']}: {error_msg}")
                count_val = 0

            if spec['is_complete'] and count_val > 0:
                ttl = COMPLETED_PERIOD_TTL
                _set_cached_count(spec['cache_key'], count_val, ttl)
            elif spec['is_complete']:
                ttl = RETRY_TTL
                _set_cached_count(spec['cache_key'], count_val, ttl)
            else:
                pass
            results[idx] = {'label': spec['label'], 'count': count_val, 'cached': False,
                            'events_found': events_found, 'error': error_msg}

            if fetch_num < len(uncached_indices) - 1:
                time.sleep(1)

    counts = [r['count'] for r in results]
    return {
        'period': period,
        'labels': [r['label'] for r in results],
        'counts': counts,
        'total': sum(counts),
        'peak': max(counts) if counts else 0,
        'average': round(sum(counts) / len(counts), 1) if counts else 0,
        'cached_flags': [r['cached'] for r in results],
        'retention_days': retention_days,
        'details': [
            {
                'label': r['label'],
                'count': r['count'],
                'cached': r['cached'],
                'events_found': r.get('events_found'),
                'error': r.get('error'),
            }
            for r in results
        ],
    }
