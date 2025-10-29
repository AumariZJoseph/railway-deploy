import logging
import traceback
from typing import Dict, Any, Optional
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

class AppError(Exception):
    """Base exception class for our application"""
    def __init__(self, message: str, user_message: str = None, status_code: int = 500):
        self.message = message
        self.user_message = user_message or "An unexpected error occurred. Please try again."
        self.status_code = status_code
        super().__init__(self.message)

class ValidationError(AppError):
    """Raised when input validation fails"""
    def __init__(self, message: str, user_message: str = None):
        super().__init__(message, user_message or "Invalid input provided.", 400)

class AuthenticationError(AppError):
    """Raised when authentication fails"""
    def __init__(self, message: str, user_message: str = None):
        super().__init__(message, user_message or "Authentication failed.", 401)

class AuthorizationError(AppError):
    """Raised when user doesn't have permission"""
    def __init__(self, message: str, user_message: str = None):
        super().__init__(message, user_message or "You don't have permission to perform this action.", 403)

class ResourceNotFoundError(AppError):
    """Raised when a requested resource isn't found"""
    def __init__(self, message: str, user_message: str = None):
        super().__init__(message, user_message or "The requested resource was not found.", 404)

class RateLimitError(AppError):
    """Raised when rate limits are exceeded"""
    def __init__(self, message: str, user_message: str = None):
        super().__init__(message, user_message, 429)

class ExternalServiceError(AppError):
    """Raised when external services (like Groq, Supabase) fail"""
    def __init__(self, message: str, user_message: str = None):
        super().__init__(message, user_message or "Service temporarily unavailable. Please try again.", 503)

def log_error(
    error: Exception,
    user_id: str = None,
    endpoint: str = None,
    additional_context: Dict[str, Any] = None
):
    """Comprehensive error logging with context"""
    context = {
        "user_id": user_id,
        "endpoint": endpoint,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "traceback": traceback.format_exc(),
    }
    
    if additional_context:
        context.update(additional_context)
    
    # Log with different levels based on error type
    if isinstance(error, (ValidationError, ResourceNotFoundError)):
        logger.warning(f"Application error: {context}")
    else:
        logger.error(f"Unexpected error: {context}")

async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler for uncaught exceptions"""
    user_id = getattr(request.state, 'user_id', None)
    
    # Log the error with context
    log_error(exc, user_id, request.url.path, {"method": request.method})
    
    # Return user-friendly response
    if isinstance(exc, AppError):
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "success": False,
                "error": exc.user_message,
                "error_code": type(exc).__name__
            }
        )
    elif isinstance(exc, HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "success": False,
                "error": exc.detail,
                "error_code": "HTTPException"
            }
        )
    else:
        # Don't expose internal error details to users
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": "An unexpected error occurred. Please try again.",
                "error_code": "InternalServerError"
            }
        )