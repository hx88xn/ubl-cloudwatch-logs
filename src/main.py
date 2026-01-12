from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from datetime import timedelta
from typing import Optional
from contextlib import asynccontextmanager
import asyncio
import threading

from src.config import AWS_REGION, LOG_GROUP_NAME, ACCESS_TOKEN_EXPIRE_MINUTES, S3_BUCKET_NAME, DB_NAME
from src.auth import (
    Token, User, get_current_user, 
    authenticate_user, create_access_token
)
from src.logs import fetch_logs, get_log_streams, fetch_app_logs
from src.s3 import list_audio_files, get_presigned_url
from src.database import get_tables, get_table_data
from src.utils.helper import filter_uuid, extract_uuids_from_logs
from src.traffic import get_intent_traffic_data
from src.summary import fetch_summary_logs


def acquire_cache_lock(lock_name: str, timeout: int = 300) -> bool:
    """Try to acquire a Redis lock. Returns True if lock acquired."""
    try:
        from src.cache import get_redis_client
        client = get_redis_client()
        if client is None:
            return True  # No Redis, proceed anyway
        
        # Try to set lock with NX (only if not exists) and EX (expire time)
        result = client.set(f"lock:{lock_name}", "1", nx=True, ex=timeout)
        return result is True
    except Exception as e:
        print(f"⚠️ Lock error: {e}")
        return True  # On error, proceed anyway


def release_cache_lock(lock_name: str):
    """Release a Redis lock."""
    try:
        from src.cache import get_redis_client
        client = get_redis_client()
        if client:
            client.delete(f"lock:{lock_name}")
    except Exception:
        pass


def warmup_cache_sync():
    """Synchronously warm up cache for common time ranges (with distributed lock)."""
    # Only one worker should do warmup at a time
    if not acquire_cache_lock("cache_warmup", timeout=900):
        print("🔒 Another worker is already warming cache, skipping...")
        return
    
    try:
        time_ranges = [6, 24, 168]  # 6 hours, 24 hours, 7 days (1h always fetches fresh)
        summary_ranges = [168, 336]  # 7 days, 14 days for summary (most requested)
        
        print("🔥 Starting cache warmup (this worker acquired the lock)...")
        
        for hours in time_ranges:
            try:
                print(f"  📦 Warming cache for {hours}h dashboard logs...")
                fetch_logs(hours=hours, limit=10000, page=1, page_size=10000)
                
                print(f"  📦 Warming cache for {hours}h app logs...")
                fetch_app_logs(hours=hours, limit=10000, page=1, page_size=10000)
                
            except Exception as e:
                print(f"  ⚠️ Failed to warm cache for {hours}h: {e}")
        
        # Warm up summary cache for large time ranges
        for hours in summary_ranges:
            try:
                print(f"  📦 Warming cache for {hours}h summary logs...")
                fetch_summary_logs(hours=hours)
            except Exception as e:
                print(f"  ⚠️ Failed to warm summary cache for {hours}h: {e}")
        
        print("✅ Cache warmup complete!")
    finally:
        release_cache_lock("cache_warmup")


def periodic_cache_refresh():
    """
    Smart cache refresh - refreshes each time range based on its own TTL.
    Uses distributed lock to prevent multiple workers from refreshing simultaneously.
    """
    import time
    from src.config import CACHE_TTL_6H, CACHE_TTL_24H, CACHE_TTL_48H, CACHE_TTL_1MONTH
    
    # Time ranges with their TTLs (refresh at 80% of TTL)
    # Note: 1h always fetches fresh, so not included here
    time_range_config = [
        (6, CACHE_TTL_6H * 0.8),      # 6h logs: refresh at 80% of 3h = 2.4h
        (24, CACHE_TTL_24H * 0.8),    # 24h logs: refresh at 80% of 12h = 9.6h
        (168, CACHE_TTL_48H * 0.8),   # 7d logs: refresh at 80% of 24h = 19.2h
        (720, CACHE_TTL_1MONTH * 0.8), # 1 month logs: refresh at 80% of 24h = 19.2h
    ]
    
    # Summary-specific refresh (7d and 14d are commonly used)
    summary_range_config = [
        (168, CACHE_TTL_48H * 0.8),   # 7d summary
        (336, CACHE_TTL_48H * 0.8),   # 14d summary
    ]
    
    # Track last refresh time for each range
    last_refresh = {hours: 0 for hours, _ in time_range_config}
    last_summary_refresh = {hours: 0 for hours, _ in summary_range_config}
    
    # Wait for initial warmup to complete + stagger workers
    import random
    time.sleep(30 + random.randint(0, 10))  # Stagger to prevent race conditions
    print("🔄 Smart cache refresh started - each range refreshes based on its TTL")
    
    while True:
        current_time = time.time()
        
        for hours, refresh_interval in time_range_config:
            time_since_refresh = current_time - last_refresh[hours]
            
            if time_since_refresh >= refresh_interval:
                # Try to acquire lock for this specific refresh
                lock_name = f"refresh_{hours}h"
                if acquire_cache_lock(lock_name, timeout=300):
                    try:
                        print(f"  🔄 Refreshing {hours}h cache (TTL-based, every {int(refresh_interval)}s)...")
                        fetch_logs(hours=hours, limit=10000, page=1, page_size=10000)
                        fetch_app_logs(hours=hours, limit=10000, page=1, page_size=10000)
                        last_refresh[hours] = current_time
                    except Exception as e:
                        print(f"  ⚠️ Failed to refresh {hours}h cache: {e}")
                    finally:
                        release_cache_lock(lock_name)
                else:
                    print(f"  🔒 Another worker is refreshing {hours}h cache, skipping...")
        
        # Refresh summary caches
        for hours, refresh_interval in summary_range_config:
            time_since_refresh = current_time - last_summary_refresh[hours]
            
            if time_since_refresh >= refresh_interval:
                lock_name = f"refresh_summary_{hours}h"
                if acquire_cache_lock(lock_name, timeout=600):
                    try:
                        print(f"  🔄 Refreshing {hours}h summary cache...")
                        fetch_summary_logs(hours=hours)
                        last_summary_refresh[hours] = current_time
                    except Exception as e:
                        print(f"  ⚠️ Failed to refresh {hours}h summary cache: {e}")
                    finally:
                        release_cache_lock(lock_name)
        
        # Check every 30 seconds if any cache needs refresh
        time.sleep(30)


# Flag to stop background threads on shutdown
_shutdown_flag = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle events."""
    global _shutdown_flag
    _shutdown_flag = False
    
    # Startup: warm cache in background thread
    warmup_thread = threading.Thread(target=warmup_cache_sync, daemon=True)
    warmup_thread.start()
    
    # Start periodic refresh thread
    refresh_thread = threading.Thread(target=periodic_cache_refresh, daemon=True)
    refresh_thread.start()
    
    yield  # Server runs here
    
    # Shutdown
    _shutdown_flag = True
    print("👋 Shutting down...")


app = FastAPI(title="CloudWatch Logs Viewer", version="1.0.0", lifespan=lifespan)

# Add gzip compression middleware for faster response delivery
from starlette.middleware.gzip import GZipMiddleware
app.add_middleware(GZipMiddleware, minimum_size=500)  # Compress responses > 500 bytes

templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def root():
    return RedirectResponse(url="/login")

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    # Just serve the HTML page - authentication is handled by JavaScript
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "log_group": LOG_GROUP_NAME,
        "region": AWS_REGION
    })

@app.get("/app-logs", response_class=HTMLResponse)
async def app_logs_page(request: Request):
    # Just serve the HTML page - authentication is handled by JavaScript
    return templates.TemplateResponse("app_logs.html", {
        "request": request,
        "log_group": LOG_GROUP_NAME,
        "region": AWS_REGION
    })

@app.get("/audio-files", response_class=HTMLResponse)
async def audio_files_page(request: Request):
    # Just serve the HTML page - authentication is handled by JavaScript
    return templates.TemplateResponse("audio_files.html", {
        "request": request,
        "bucket": S3_BUCKET_NAME,
        "region": AWS_REGION
    })

@app.get("/database", response_class=HTMLResponse)
async def database_page(request: Request):
    # Just serve the HTML page - authentication is handled by JavaScript
    return templates.TemplateResponse("database.html", {
        "request": request,
        "db_name": DB_NAME,
        "region": AWS_REGION
    })

@app.get("/traffic", response_class=HTMLResponse)
async def traffic_page(request: Request):
    # Traffic analytics page - admin-ubl only (enforced by JavaScript)
    return templates.TemplateResponse("traffic.html", {
        "request": request,
        "log_group": LOG_GROUP_NAME,
        "region": AWS_REGION
    })

@app.get("/summary", response_class=HTMLResponse)
async def summary_page(request: Request):
    # Summary page - shows Time taken, Detected Intent, Original transcription grouped by UUID
    return templates.TemplateResponse("summary.html", {
        "request": request,
        "log_group": LOG_GROUP_NAME,
        "region": AWS_REGION
    })

@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {
        "access_token": access_token, 
        "token_type": "bearer",
        "role": user.role  # Include user role in response
    }

@app.get("/api/logs")
async def get_logs(
    hours: int = 1,
    limit: int = 10000,
    page: int = 1,
    page_size: int = 10000,
    search: Optional[str] = None,
    uuid: Optional[str] = None,
    current_user: User = Depends(get_current_user)
):
    hours = max(1, min(hours, 720))  # Allow up to 1 month (30 days)
    limit = max(100, min(limit, 500000))  # Increased for 1 month data
    page = max(1, page)
    page_size = max(10, min(page_size, 500000))  # Increased for 1 month data
    
    result = fetch_logs(
        hours=hours, 
        limit=limit, 
        search_query=search,
        page=page,
        page_size=page_size
    )
    
    if uuid:
        uuid_map = filter_uuid(result['logs'])
        filtered_logs = uuid_map.get(uuid, [])
        
        total_filtered = len(filtered_logs)
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_logs = filtered_logs[start_idx:end_idx]
        
        result = {
            'logs': paginated_logs,
            'total': total_filtered,
            'page': page,
            'page_size': page_size,
            'has_more': end_idx < total_filtered,
            'total_pages': (total_filtered + page_size - 1) // page_size if total_filtered > 0 else 1
        }
    
    return result

@app.get("/api/logs/streams")
async def get_streams(current_user: User = Depends(get_current_user)):
    streams = get_log_streams()
    return {"streams": streams}

@app.get("/api/logs/uuids")
async def get_uuids(
    hours: int = 1,
    limit: int = 10000,
    current_user: User = Depends(get_current_user)
):
    hours = max(1, min(hours, 720))  # Allow up to 1 month (30 days)
    limit = max(100, min(limit, 500000))  # Increased for 1 month data
    
    result = fetch_logs(
        hours=hours, 
        limit=limit, 
        search_query=None,
        page=1,
        page_size=limit
    )
    
    uuids = extract_uuids_from_logs(result['logs'])
    return {"uuids": uuids, "count": len(uuids)}

@app.get("/api/app-logs")
async def get_app_logs(
    hours: int = 1,
    limit: int = 10000,
    page: int = 1,
    page_size: int = 10000,
    current_user: User = Depends(get_current_user)
):
    hours = max(1, min(hours, 720))  # Allow up to 1 month (30 days)
    limit = max(100, min(limit, 500000))  # Increased for 1 month data
    page = max(1, page)
    page_size = max(10, min(page_size, 500000))  # Increased for 1 month data
    
    result = fetch_app_logs(
        hours=hours,
        limit=limit,
        page=page,
        page_size=page_size
    )
    
    return result

@app.get("/api/traffic")
async def get_traffic_data(
    hours: int = 1,
    current_user: User = Depends(get_current_user)
):
    # Restrict to admin-ubl only
    if current_user.role != 'admin-ubl':
        raise HTTPException(
            status_code=403,
            detail="Access denied. Admin access required."
        )
    
    hours = max(1, min(hours, 336))  # Allow up to 14 days
    result = get_intent_traffic_data(hours=hours)
    return result

@app.get("/api/summary")
async def get_summary_data(
    hours: int = 1,
    current_user: User = Depends(get_current_user)
):
    hours = max(1, min(hours, 720))  # Allow up to 1 month (30 days)
    # For large time ranges (7+ days), use quick-fail mode to avoid 504 timeouts
    # The background cache warming will populate the cache
    allow_slow = hours < 168  # Only allow slow fetch for < 7 days
    result = fetch_summary_logs(hours=hours, allow_slow_fetch=allow_slow)
    return result

@app.get("/api/user/me", response_model=User)
async def read_users_me(current_user: User = Depends(get_current_user)):
    return current_user

@app.get("/api/s3/audio-files")
async def get_audio_files(
    page: int = 1,
    page_size: int = 50,
    search: Optional[str] = None,
    current_user: User = Depends(get_current_user)
):
    page = max(1, page)
    page_size = max(10, min(page_size, 100))
    
    result = list_audio_files(
        page=page,
        page_size=page_size,
        search_query=search
    )
    
    return result

@app.get("/api/s3/audio-url")
async def get_audio_url(
    key: str,
    download: bool = False,
    current_user: User = Depends(get_current_user)
):
    result = get_presigned_url(file_key=key, download=download)
    return result

@app.get("/api/db/tables")
async def get_db_tables(
    current_user: User = Depends(get_current_user)
):
    tables = get_tables()
    return {"tables": tables, "database": DB_NAME}

@app.get("/api/db/table/{table_name}")
async def get_db_table_data(
    table_name: str,
    page: int = 1,
    page_size: int = 50,
    search: Optional[str] = None,
    order_by: Optional[str] = None,
    order_dir: str = 'DESC',
    current_user: User = Depends(get_current_user)
):
    page = max(1, page)
    page_size = max(10, min(page_size, 100))
    order_dir = 'ASC' if order_dir.upper() == 'ASC' else 'DESC'
    
    result = get_table_data(
        table_name=table_name,
        page=page,
        page_size=page_size,
        search_query=search,
        order_by=order_by,
        order_dir=order_dir
    )
    
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
