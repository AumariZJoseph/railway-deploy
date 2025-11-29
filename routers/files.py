from services.supabase_client import supabase_client
from services.sanitization_service import sanitization_service
from services.rate_limiter import rate_limiter
from fastapi import APIRouter, HTTPException
import logging
from services.fast_ingest_service import fast_ingest_service

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/files/{user_id}")
async def get_user_files(user_id: str):
    """Get user's files with input sanitization"""
    try:
        sanitized_user_id = sanitization_service.sanitize_user_id(user_id)
        files = await fast_ingest_service.get_user_files(sanitized_user_id)
        return {
            "status": "success",
            "files": files
        }
    except Exception as e:
        logger.error(f"Get files error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting files: {str(e)}"
        )

@router.delete("/files/{user_id}/{filename}")
async def delete_user_file(user_id: str, filename: str):
    """Delete user's file with input sanitization"""
    is_limited, message = rate_limiter.is_rate_limited(user_id, "file_operations")
    if is_limited:
        raise HTTPException(status_code=429, detail=message)
    
    try:
        sanitized_user_id = sanitization_service.sanitize_user_id(user_id)
        sanitized_filename = sanitization_service.sanitize_filename(filename)
        
        result = await fast_ingest_service.delete_file(sanitized_user_id, sanitized_filename)
        return result
    except Exception as e:
        logger.error(f"Delete error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error deleting file: {str(e)}"
        )

@router.get("/usage/{user_id}")
async def get_user_usage(user_id: str):
    """Get user's current trial usage"""
    try:
        print(f"DEBUG: Getting usage for user: {user_id}")  # Add this
        print(f"DEBUG: supabase_client type: {type(supabase_client)}")  # Add this
        
        sanitized_user_id = sanitization_service.sanitize_user_id(user_id)
        usage = supabase_client.get_user_usage(sanitized_user_id)
        
        print(f"DEBUG: Usage result: {usage}")  # Add this
        
        return {
            "status": "success",
            "usage": usage
        }
    except Exception as e:
        print(f"DEBUG: Error in get_user_usage: {str(e)}")  # Add this
        logger.error(f"Get usage error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting usage: {str(e)}")