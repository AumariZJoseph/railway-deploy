import time
import logging
import asyncio
from functools import wraps
from typing import Callable, Any

logger = logging.getLogger(__name__)

def log_performance(func: Callable) -> Callable:
    """Decorator to log function execution time"""
    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = await func(*args, **kwargs)
            return result
        finally:
            execution_time = time.time() - start_time
            logger.info(f"{func.__name__} executed in {execution_time:.2f} seconds")
    
    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            execution_time = time.time() - start_time
            logger.info(f"{func.__name__} executed in {execution_time:.2f} seconds")
    
    return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper