"""
Centralized logging utility for the distressed property intelligence platform.

This module provides a standardized logging configuration across all pipeline modules.
"""

import logging
import logging.config
import os
from pathlib import Path
from typing import Optional

import yaml


def setup_logging(
    default_path: str = "config/logging.yaml",
    default_level: int = logging.INFO,
    env_key: str = "LOG_CFG"
) -> None:
    """
    Setup logging configuration from YAML file.
    
    Args:
        default_path: Path to the logging configuration YAML file
        default_level: Default logging level if configuration file is not found
        env_key: Environment variable name to override the default path
        
    Raises:
        FileNotFoundError: If the logging configuration file is not found and no default is set
    """
    path = default_path
    value = os.getenv(env_key, None)
    
    if value:
        path = value
    
    if os.path.exists(path):
        # Ensure logs directory exists
        logs_dir = Path("logs")
        logs_dir.mkdir(parents=True, exist_ok=True)
        
        with open(path, "rt") as f:
            try:
                config = yaml.safe_load(f.read())
                logging.config.dictConfig(config)
            except Exception as e:
                print(f"Error loading logging configuration from {path}: {e}")
                print("Using default logging configuration")
                logging.basicConfig(level=default_level)
    else:
        logging.basicConfig(level=default_level)
        logging.warning(f"Logging configuration file not found at {path}. Using default configuration.")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for the specified module name.
    
    Args:
        name: Name of the logger (typically __name__ of the calling module)
        
    Returns:
        Logger instance configured according to the logging.yaml settings
        
    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Processing started")
    """
    return logging.getLogger(name)


class LoggerAdapter(logging.LoggerAdapter):
    """
    Custom logger adapter that adds contextual information to log records.
    
    This adapter can be used to add context-specific information (like pipeline name,
    run ID, etc.) to all log messages from a particular context.
    """
    
    def process(self, msg: str, kwargs: dict) -> tuple:
        """
        Process the logging message and keyword arguments.
        
        Args:
            msg: The logging message
            kwargs: Additional keyword arguments
            
        Returns:
            Tuple of processed message and kwargs
        """
        if self.extra:
            context_str = " | ".join(f"{k}={v}" for k, v in self.extra.items())
            return f"[{context_str}] {msg}", kwargs
        return msg, kwargs


def get_contextual_logger(name: str, context: Optional[dict] = None) -> LoggerAdapter:
    """
    Get a contextual logger that includes additional information in log messages.
    
    Args:
        name: Name of the logger
        context: Dictionary of contextual information to include in logs
        
    Returns:
        LoggerAdapter instance with contextual information
        
    Example:
        >>> logger = get_contextual_logger(__name__, {"pipeline": "probate", "run_id": "123"})
        >>> logger.info("Processing file")  # Will include [pipeline=probate | run_id=123] prefix
    """
    base_logger = get_logger(name)
    return LoggerAdapter(base_logger, context or {})
