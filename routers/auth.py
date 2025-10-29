from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from services.auth_service import auth_service
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

class LoginRequest(BaseModel):
    email: str
    password: str

class RegisterRequest(BaseModel):
    email: str
    password: str

class LogoutRequest(BaseModel):
    access_token: str

class RefreshTokenRequest(BaseModel):
    refresh_token: str

@router.post("/auth/login")
async def login(request: LoginRequest):
    """Login endpoint with token expiration"""
    result = auth_service.login(request.email, request.password)
    
    if result["status"] == "error":
        raise HTTPException(status_code=401, detail=result["message"])
    
    return result

@router.post("/auth/register")
async def register(request: RegisterRequest):
    """Register endpoint - handles email confirmation flow"""
    result = auth_service.register(request.email, request.password)
    
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    
    return result

@router.post("/auth/logout")
async def logout(request: LogoutRequest):
    """Logout endpoint"""
    result = auth_service.logout(request.access_token)
    
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    
    return result

@router.post("/auth/refresh")
async def refresh_token(request: RefreshTokenRequest):
    """Refresh expired access token"""
    result = auth_service.refresh_token(request.refresh_token)
    
    if result["status"] == "error":
        raise HTTPException(status_code=401, detail=result["message"])
    
    return result