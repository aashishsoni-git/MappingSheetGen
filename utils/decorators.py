# utils/decorators.py
"""
Reusable decorators for error handling, retries, and logging
"""
import functools
import time
import logging
from typing import Callable, Optional, Type, Tuple
from openai import OpenAIError, RateLimitError, APIConnectionError

logger = logging.getLogger(__name__)


def retry_on_error(max_retries: int = 3, 
                   delay: float = 1.0,
                   backoff_factor: float = 2.0,
                   exceptions: Tuple[Type[Exception], ...] = (Exception,)):
    """
    Retry decorator with exponential backoff
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries (seconds)
        backoff_factor: Multiplier for delay after each retry
        exceptions: Tuple of exception types to catch
        
    Usage:
        @retry_on_error(max_retries=3, delay=2)
        def call_openai_api():
            # API call here
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(
                            f"{func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {str(e)}. "
                            f"Retrying in {current_delay:.1f}s..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff_factor
                    else:
                        logger.error(
                            f"{func.__name__} failed after {max_retries + 1} attempts: {str(e)}"
                        )
            
            raise last_exception
        
        return wrapper
    return decorator


def handle_openai_errors(func: Callable) -> Callable:
    """
    Decorator specifically for OpenAI API error handling
    
    Usage:
        @handle_openai_errors
        def predict_mappings(self, ...):
            # OpenAI API call
            pass
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except RateLimitError as e:
            logger.error(f"OpenAI rate limit exceeded in {func.__name__}: {str(e)}")
            logger.info("Consider implementing request throttling or upgrading API tier")
            raise
        except APIConnectionError as e:
            logger.error(f"OpenAI connection error in {func.__name__}: {str(e)}")
            logger.info("Check internet connection and OpenAI status")
            raise
        except OpenAIError as e:
            logger.error(f"OpenAI API error in {func.__name__}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {str(e)}", exc_info=True)
            raise
    
    return wrapper


def log_execution_time(func: Callable) -> Callable:
    """
    Log function execution time
    
    Usage:
        @log_execution_time
        def expensive_operation():
            pass
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"Starting {func.__name__}...")
        
        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time
            logger.info(f"{func.__name__} completed in {elapsed:.2f}s")
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"{func.__name__} failed after {elapsed:.2f}s: {str(e)}")
            raise
    
    return wrapper


def validate_inputs(**type_checks):
    """
    Validate function input types
    
    Usage:
        @validate_inputs(xml_path=str, schema_size=int)
        def process_file(xml_path, schema_size):
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get function signature
            import inspect
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            
            # Validate types
            for param_name, expected_type in type_checks.items():
                if param_name in bound_args.arguments:
                    value = bound_args.arguments[param_name]
                    if not isinstance(value, expected_type):
                        raise TypeError(
                            f"{func.__name__}: Expected {param_name} to be {expected_type.__name__}, "
                            f"got {type(value).__name__}"
                        )
            
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


# Combined decorator for common ETL operations
def etl_operation(max_retries: int = 3):
    """
    Combined decorator for ETL operations with retry, error handling, and logging
    
    Usage:
        @etl_operation(max_retries=3)
        def extract_data():
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        @log_execution_time
        @retry_on_error(
            max_retries=max_retries,
            exceptions=(APIConnectionError, RateLimitError)
        )
        @handle_openai_errors
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator
