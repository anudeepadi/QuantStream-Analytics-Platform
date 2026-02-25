"""
Authentication Endpoints

Handles user login, registration, token refresh, and logout.
"""

from fastapi import APIRouter, HTTPException, Depends, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field, EmailStr
from typing import Optional

router = APIRouter()
security = HTTPBearer()

# ── Request / Response Models ─────────────────────────────────

class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=50)
    password: str = Field(..., min_length=1)

class RegisterRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    email: str = Field(..., min_length=5, max_length=100)
    password: str = Field(..., min_length=6)
    full_name: Optional[str] = Field(None, max_length=100)

class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"

class RefreshRequest(BaseModel):
    refresh_token: str

class UserResponse(BaseModel):
    id: int
    username: str
    email: str
    role: str
    full_name: Optional[str] = None

# ── Service accessor (injected from main.py) ─────────────────

_auth_service = None
_db_service = None

def set_services(auth_service, db_service):
    """Called from main.py lifespan to inject services."""
    global _auth_service, _db_service
    _auth_service = auth_service
    _db_service = db_service

def _get_auth():
    if _auth_service is None:
        raise RuntimeError("Auth service not initialized")
    return _auth_service

# ── Endpoints ─────────────────────────────────────────────────

@router.post("/login", response_model=TokenResponse)
async def login(body: LoginRequest):
    """Authenticate user and return JWT tokens."""
    auth = _get_auth()
    user = await auth.authenticate_user(body.username, body.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
        )
    session = await auth.create_user_session(user)
    return TokenResponse(
        access_token=session["access_token"],
        refresh_token=session["refresh_token"],
    )

@router.post("/register", response_model=TokenResponse, status_code=status.HTTP_201_CREATED)
async def register(body: RegisterRequest):
    """Register a new user and return JWT tokens."""
    auth = _get_auth()

    # Check if username already exists
    if _db_service:
        existing = await _db_service.get_user(body.username)
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Username already exists",
            )

    password_hash = auth.hash_password(body.password)

    if _db_service:
        user_id = await _db_service.create_user({
            "username": body.username,
            "email": body.email,
            "password_hash": password_hash,
            "full_name": body.full_name,
            "role": "viewer",
        })
    else:
        # Mock fallback
        user_id = 99

    user_data = {
        "id": user_id,
        "username": body.username,
        "email": body.email,
        "role": "viewer",
        "full_name": body.full_name,
    }

    session = await auth.create_user_session(user_data)
    return TokenResponse(
        access_token=session["access_token"],
        refresh_token=session["refresh_token"],
    )

@router.get("/me", response_model=UserResponse)
async def get_me(credentials: HTTPAuthorizationCredentials = Security(security)):
    """Return profile of the currently authenticated user."""
    auth = _get_auth()
    user = await auth.verify_token(credentials.credentials)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )
    return UserResponse(
        id=user.get("id", 0),
        username=user.get("username", ""),
        email=user.get("email", ""),
        role=user.get("role", "viewer"),
        full_name=user.get("full_name"),
    )

@router.post("/refresh", response_model=dict)
async def refresh_token(body: RefreshRequest):
    """Refresh an expired access token."""
    auth = _get_auth()
    result = await auth.refresh_access_token(body.refresh_token)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token",
        )
    return result

@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(credentials: HTTPAuthorizationCredentials = Security(security)):
    """Logout by blacklisting the current access token."""
    auth = _get_auth()
    await auth.logout_user(credentials.credentials)
