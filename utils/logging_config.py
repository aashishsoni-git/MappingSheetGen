# utils/logging_config.py
"""
Centralized logging configuration
"""
import logging
import sys
from pathlib import Path
from typing import Optional  # ⭐ Add this import


def setup_logging(log_level: str = 'INFO', log_file: Optional[str] = None):
    """
    Configure logging for the entire application
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional log file path
    """
    # Create logs directory if needed
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Configure format
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # Create handlers
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        datefmt=date_format,
        handlers=handlers
    )
    
    # Suppress noisy third-party loggers
    logging.getLogger('snowflake.connector').setLevel(logging.WARNING)
    logging.getLogger('openai').setLevel(logging.WARNING)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured at {log_level} level")
