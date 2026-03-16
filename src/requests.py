import re
import json
import boto3
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from src.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, LOG_GROUP_NAME
from src.cache import get_redis_client

PKT = timezone(timedelta(hours=5))

FINAL_RESPONSE_FILTER = '"Final Response:"'
UUID_PATTERN = re.compile(r'\[([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\]')

# Completed periods never change — cache for 1 year
COMPLETED_PERIOD_TTL = 365 * 24 * 3600
# Current (incomplete) period refreshes every hour
CURRENT_PERIOD_TTL = 3600


def _get_cloudwatch_client():
    return boto3.client(
        'logs',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )


def _fetch_events_for_range(start_dt: datetime, end_dt: datetime) -> List[dict]:
    client = _get_cloudwatch_client()
    total_hours = (end_dt - start_dt).total_seconds() / 3600

    if total_hours > 48:
        return _fetch_events_chunked(client, start_dt, end_dt)

    all_events = []
    next_token = None
    api_start_ms = int(start_dt.timestamp() * 1000)
    api_end_ms = int(end_dt.timestamp() * 1000)

    for _ in range(100):
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
        for _ in range(30):
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

        current_end = chunk_start

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


def _set_cached_count(cache_key: str, count: int, ttl: int):
    client = get_redis_client()
    if client is None:
        return
    try:
        client.setex(cache_key, ttl, json.dumps(count))
        print(f"✅ Cached requests count={count} key='{cache_key}' TTL={ttl}s")
    except Exception as e:
        print(f"⚠️ Cache write error for {cache_key}: {e}")


def _build_monthly_specs(now_pkt: datetime, num_periods: int) -> List[dict]:
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
            'cache_key': f"requests:monthly:{year}-{month:02d}",
            'start': start,
            'end': end,
            'is_complete': not is_current,
        })
    return specs


def _build_weekly_specs(now_pkt: datetime, num_periods: int) -> List[dict]:
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
            'cache_key': f"requests:weekly:{iso_year}-W{iso_week:02d}",
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


def get_requests_data(period: str = 'monthly', num_periods: int = 6) -> Dict:
    now_pkt = datetime.now(PKT)

    if period == 'monthly':
        specs = _build_monthly_specs(now_pkt, num_periods)
    elif period == 'weekly':
        specs = _build_weekly_specs(now_pkt, num_periods)
    elif period == 'daily':
        specs = _build_daily_specs(now_pkt, num_periods)
    else:
        raise ValueError(f"Invalid period '{period}'. Must be monthly, weekly, or daily.")

    results: List[Optional[dict]] = [None] * len(specs)
    uncached_indices = []

    for i, spec in enumerate(specs):
        cached_val = _get_cached_count(spec['cache_key'])
        if cached_val is not None:
            results[i] = {'label': spec['label'], 'count': cached_val, 'cached': True}
        else:
            uncached_indices.append(i)

    if uncached_indices:
        print(f"⚡ Requests ({period}): fetching {len(uncached_indices)} uncached period(s) from CloudWatch...")
        for idx in uncached_indices:
            spec = specs[idx]
            try:
                events = _fetch_events_for_range(spec['start'], spec['end'])
                count_val = _count_unique_requests(events)
            except Exception as e:
                print(f"⚠️ Requests: failed to fetch {spec['label']}: {e}")
                count_val = 0
            ttl = COMPLETED_PERIOD_TTL if spec['is_complete'] else CURRENT_PERIOD_TTL
            _set_cached_count(spec['cache_key'], count_val, ttl)
            results[idx] = {'label': spec['label'], 'count': count_val, 'cached': False}

    counts = [r['count'] for r in results]
    return {
        'period': period,
        'labels': [r['label'] for r in results],
        'counts': counts,
        'total': sum(counts),
        'peak': max(counts) if counts else 0,
        'average': round(sum(counts) / len(counts), 1) if counts else 0,
        'cached_flags': [r['cached'] for r in results],
    }
