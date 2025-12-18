from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from datetime import timedelta
from typing import Optional

from src.config import AWS_REGION, LOG_GROUP_NAME, ACCESS_TOKEN_EXPIRE_MINUTES
from src.auth import (
    Token, User, get_current_user, 
    authenticate_user, create_access_token
)
from src.logs import fetch_logs, get_log_streams, fetch_app_logs
from src.utils.helper import filter_uuid, extract_uuids_from_logs

app = FastAPI(title="CloudWatch Logs Viewer", version="1.0.0")
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
    page_size: int = 50,
    search: Optional[str] = None,
    uuid: Optional[str] = None,
    current_user: User = Depends(get_current_user)
):
    hours = max(1, min(hours, 168))
    limit = max(100, min(limit, 50000))
    page = max(1, page)
    page_size = max(10, min(page_size, 500))
    
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
    hours = max(1, min(hours, 168))
    limit = max(100, min(limit, 50000))
    
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
    page_size: int = 50,
    current_user: User = Depends(get_current_user)
):
    hours = max(1, min(hours, 168))
    limit = max(100, min(limit, 50000))
    page = max(1, page)
    page_size = max(10, min(page_size, 500))
    
    result = fetch_app_logs(
        hours=hours,
        limit=limit,
        page=page,
        page_size=page_size
    )
    
    return result

@app.get("/api/user/me", response_model=User)
async def read_users_me(current_user: User = Depends(get_current_user)):
    return current_user

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
