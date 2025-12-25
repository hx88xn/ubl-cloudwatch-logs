import json
import hashlib
from typing import Optional, Any, List, Dict
import redis
from src.config import (
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    CACHE_TTL_1H,
    CACHE_TTL_6H,
    CACHE_TTL_24H,
    CACHE_TTL_48H
)

# Global Redis connection pool
_redis_pool = None
_redis_client = None

def get_redis_client() -> Optional[redis.Redis]:
    global _redis_pool, _redis_client
    
    if _redis_client is not None:
        try:
            _redis_client.ping()
            return _redis_client
        except (redis.ConnectionError, redis.TimeoutError):
            _redis_client = None
            _redis_pool = None
    
    try:
        if _redis_pool is None:
            _redis_pool = redis.ConnectionPool(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                max_connections=10,
                socket_timeout=2,
                socket_connect_timeout=2,
                retry_on_timeout=True
            )
        
        _redis_client = redis.Redis(connection_pool=_redis_pool)
        _redis_client.ping()
        return _redis_client
    except (redis.ConnectionError, redis.TimeoutError) as e:
        print(f"⚠️ Redis unavailable: {e}. Falling back to direct CloudWatch fetch.")
        return None

def get_cache_ttl(hours: int) -> int:
    if hours <= 1:
        return CACHE_TTL_1H
    elif hours <= 6:
        return CACHE_TTL_6H
    elif hours <= 24:
        return CACHE_TTL_24H
    else:
        return CACHE_TTL_48H

def generate_cache_key(prefix: str, hours: int, search_query: Optional[str] = None) -> str:
    if search_query:
        search_hash = hashlib.md5(search_query.encode()).hexdigest()[:8]
    else:
        search_hash = "all"
    
    return f"logs:{prefix}:{hours}:{search_hash}"

def get_cached_logs(cache_key: str) -> Optional[List[Dict]]:
    client = get_redis_client()
    if client is None:
        return None
    
    try:
        cached_data = client.get(cache_key)
        if cached_data:
            return json.loads(cached_data)
        return None
    except (redis.RedisError, json.JSONDecodeError) as e:
        print(f"⚠️ Cache read error: {e}")
        return None

def set_cached_logs(cache_key: str, logs: List[Dict], hours: int) -> bool:
    client = get_redis_client()
    if client is None:
        return False
    
    try:
        ttl = get_cache_ttl(hours)
        serialized = json.dumps(logs)
        client.setex(cache_key, ttl, serialized)
        print(f"✅ Cached {len(logs)} logs with key '{cache_key}' (TTL: {ttl}s)")
        return True
    except (redis.RedisError, TypeError) as e:
        print(f"⚠️ Cache write error: {e}")
        return False

def invalidate_cache(pattern: str = "logs:*") -> int:
    client = get_redis_client()
    if client is None:
        return 0
    
    try:
        keys = client.keys(pattern)
        if keys:
            deleted = client.delete(*keys)
            print(f"🗑️ Invalidated {deleted} cache keys matching '{pattern}'")
            return deleted
        return 0
    except redis.RedisError as e:
        print(f"⚠️ Cache invalidation error: {e}")
        return 0

def get_cache_stats() -> Dict[str, Any]:
    client = get_redis_client()
    if client is None:
        return {"status": "unavailable"}
    
    try:
        info = client.info("memory")
        keys = client.keys("logs:*")
        return {
            "status": "connected",
            "cached_keys": len(keys),
            "used_memory": info.get("used_memory_human", "unknown"),
            "keys": [k.decode() if isinstance(k, bytes) else k for k in keys[:10]]
        }
    except redis.RedisError as e:
        return {"status": "error", "error": str(e)}
