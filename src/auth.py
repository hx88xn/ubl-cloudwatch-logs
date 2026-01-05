from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import Optional
import hashlib
from src.config import JWT_SECRET_KEY, JWT_ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

class User(BaseModel):
    username: str
    role: Optional[str] = "admin-ubl"  # "admin-ubl" or "ubl-test-team"
    disabled: Optional[bool] = None

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

USERS_DB = {
    "admin-ubl": {
        "username": "admin-ubl",
        "hashed_password": hash_password("admin-ubl123"),
        "role": "admin-ubl",
        "disabled": False,
    },
    "ubl-test-team": {
        "username": "ubl-test-team",
        "hashed_password": hash_password("ubl-test-team123"),
        "role": "ubl-test-team",
        "disabled": False,
    },
    "innovation@ubl": {
        "username": "innovation@ubl",
        "hashed_password": hash_password("admin123"),
        "role": "admin-ubl",
        "disabled": False,
    },
    "ibtisam@ubl": {
        "username": "ibtisam@ubl",
        "hashed_password": hash_password("admin123"),
        "role": "admin-ubl",
        "disabled": False,
    },
    "innovation-ops@ubl": {
        "username": "innovation-ops@ubl",
        "hashed_password": hash_password("admin123"),
        "role": "admin-ubl",
        "disabled": False,
    },
    "rafay@ubl": {
        "username": "rafay@ubl",
        "hashed_password": hash_password("admin123"),
        "role": "admin-ubl",
        "disabled": False,
    },
    "innovation1@ubl": {
        "username": "innovation1@ubl",
        "hashed_password": hash_password("admin123"),
        "role": "admin-ubl",
        "disabled": False,
    },
}

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return hash_password(plain_password) == hashed_password

def get_user(username: str):
    if username in USERS_DB:
        user_dict = USERS_DB[username]
        return User(**user_dict)

def authenticate_user(username: str, password: str):
    user = get_user(username)
    if not user:
        return False
    if not verify_password(password, USERS_DB[username]["hashed_password"]):
        return False
    return user

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return encoded_jwt

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = get_user(username=token_data.username)
    if user is None:
        raise credentials_exception
    if user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return user
