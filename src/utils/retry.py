"""Retry decorator with exponential backoff."""

import time
import logging
from functools import wraps
from typing import Callable, Any

from ..config.settings import CONFIG

logger = logging.getLogger(__name__)

def retry_with_backoff(max_retries: int = None, base_delay: float = None):
    """Decorator for retrying functions with exponential backoff."""
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            retries = max_retries or CONFIG.MAX_RETRIES
            delay = base_delay or CONFIG.BASE_DELAY
            
            for attempt in range(retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == retries:
                        logger.error(f"Function {func.__name__} failed after {retries} retries: {e}")
                        raise
                    
                    # Check for rate limiting
                    if hasattr(e, 'status_code') and e.status_code == 429:
                        wait_time = delay * (2 ** attempt)
                        logger.warning(f"Rate limited, retrying {func.__name__} in {wait_time}s (attempt {attempt + 1}/{retries + 1})")
                    else:
                        wait_time = delay * (2 ** attempt)
                        logger.warning(f"Retrying {func.__name__} in {wait_time}s (attempt {attempt + 1}/{retries + 1}): {e}")
                    
                    time.sleep(wait_time)
            
        return wrapper
    return decorator