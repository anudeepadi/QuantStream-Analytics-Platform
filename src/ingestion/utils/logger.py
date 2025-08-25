"""
Logging utilities for the QuantStream Analytics Platform.

This module provides structured logging with correlation IDs, performance
tracking, and integration with monitoring systems.
"""

import logging
import logging.handlers
import sys
import json
import time
from datetime import datetime
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import threading
import uuid
from contextlib import contextmanager
import traceback
import os


class LogLevel(Enum):
    """Log level enumeration."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class LogConfig:
    """Logging configuration."""
    level: LogLevel = LogLevel.INFO
    format_type: str = "structured"  # "structured" or "simple"
    console_output: bool = True
    file_output: bool = True
    file_path: Optional[str] = None
    max_file_size: int = 100 * 1024 * 1024  # 100MB
    backup_count: int = 5
    correlation_tracking: bool = True
    performance_tracking: bool = True
    include_caller_info: bool = True
    include_thread_info: bool = True
    json_format: bool = True
    custom_fields: Dict[str, Any] = field(default_factory=dict)


class StructuredFormatter(logging.Formatter):
    """Structured JSON formatter for log records."""
    
    def __init__(self, config: LogConfig):
        super().__init__()
        self.config = config
        self.hostname = os.getenv('HOSTNAME', 'unknown')
        self.service_name = os.getenv('SERVICE_NAME', 'quantstream-ingestion')
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON."""
        # Base log entry
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": self.service_name,
            "hostname": self.hostname
        }
        
        # Add correlation ID if present
        correlation_id = getattr(record, 'correlation_id', None) or self._get_correlation_id()
        if correlation_id:
            log_entry["correlation_id"] = correlation_id
        
        # Add thread information
        if self.config.include_thread_info:
            log_entry["thread"] = {
                "id": record.thread,
                "name": record.threadName
            }
        
        # Add caller information
        if self.config.include_caller_info:
            log_entry["caller"] = {
                "filename": record.filename,
                "line": record.lineno,
                "function": record.funcName,
                "module": record.module
            }
        
        # Add exception information
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info)
            }
        
        # Add performance metrics if available
        if hasattr(record, 'duration'):
            log_entry["performance"] = {
                "duration_ms": record.duration * 1000,
                "operation": getattr(record, 'operation', 'unknown')
            }
        
        # Add custom fields from config
        log_entry.update(self.config.custom_fields)
        
        # Add extra fields from record
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname',
                          'filename', 'module', 'lineno', 'funcName', 'created',
                          'msecs', 'relativeCreated', 'thread', 'threadName',
                          'processName', 'process', 'exc_info', 'exc_text',
                          'stack_info', 'correlation_id', 'duration', 'operation']:
                log_entry[key] = value
        
        return json.dumps(log_entry, default=str, separators=(',', ':'))
    
    def _get_correlation_id(self) -> Optional[str]:
        """Get correlation ID from thread local storage."""
        return getattr(_thread_local, 'correlation_id', None)


class SimpleFormatter(logging.Formatter):
    """Simple text formatter for log records."""
    
    def __init__(self, config: LogConfig):
        format_string = "%(asctime)s - %(name)s - %(levelname)s"
        
        if config.include_caller_info:
            format_string += " - %(filename)s:%(lineno)d"
        
        if config.correlation_tracking:
            format_string += " - [%(correlation_id)s]"
        
        format_string += " - %(message)s"
        
        super().__init__(format_string)
        self.config = config
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as simple text."""
        # Add correlation ID if not present
        if not hasattr(record, 'correlation_id'):
            record.correlation_id = self._get_correlation_id() or 'no-correlation'
        
        return super().format(record)
    
    def _get_correlation_id(self) -> Optional[str]:
        """Get correlation ID from thread local storage."""
        return getattr(_thread_local, 'correlation_id', None)


class PerformanceFilter(logging.Filter):
    """Filter for adding performance metrics to log records."""
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add performance context if available."""
        perf_context = getattr(_thread_local, 'performance_context', None)
        if perf_context:
            record.duration = time.time() - perf_context['start_time']
            record.operation = perf_context.get('operation', 'unknown')
        return True


class CorrelationFilter(logging.Filter):
    """Filter for adding correlation IDs to log records."""
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add correlation ID if not present."""
        if not hasattr(record, 'correlation_id'):
            correlation_id = getattr(_thread_local, 'correlation_id', None)
            if correlation_id:
                record.correlation_id = correlation_id
        return True


# Thread-local storage for context
_thread_local = threading.local()


class LoggerManager:
    """Centralized logger management."""
    
    def __init__(self, config: LogConfig):
        self.config = config
        self._loggers: Dict[str, logging.Logger] = {}
        self._setup_root_logger()
    
    def _setup_root_logger(self):
        """Setup root logger configuration."""
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, self.config.level.value))
        
        # Clear existing handlers
        root_logger.handlers.clear()
        
        # Create formatter
        if self.config.format_type == "structured" and self.config.json_format:
            formatter = StructuredFormatter(self.config)
        else:
            formatter = SimpleFormatter(self.config)
        
        # Console handler
        if self.config.console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            
            # Add filters
            if self.config.correlation_tracking:
                console_handler.addFilter(CorrelationFilter())
            if self.config.performance_tracking:
                console_handler.addFilter(PerformanceFilter())
            
            root_logger.addHandler(console_handler)
        
        # File handler
        if self.config.file_output:
            file_path = Path(self.config.file_path or "logs/quantstream.log")
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.handlers.RotatingFileHandler(
                file_path,
                maxBytes=self.config.max_file_size,
                backupCount=self.config.backup_count
            )
            file_handler.setFormatter(formatter)
            
            # Add filters
            if self.config.correlation_tracking:
                file_handler.addFilter(CorrelationFilter())
            if self.config.performance_tracking:
                file_handler.addFilter(PerformanceFilter())
            
            root_logger.addHandler(file_handler)
    
    def get_logger(self, name: str, extra_fields: Optional[Dict[str, Any]] = None) -> 'QuantStreamLogger':
        """Get or create logger with given name."""
        if name not in self._loggers:
            logger = logging.getLogger(name)
            self._loggers[name] = QuantStreamLogger(logger, extra_fields or {})
        return self._loggers[name]
    
    def set_level(self, level: LogLevel):
        """Set logging level for all loggers."""
        self.config.level = level
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, level.value))
        
        # Update all existing loggers
        for logger_wrapper in self._loggers.values():
            logger_wrapper.logger.setLevel(getattr(logging, level.value))


class QuantStreamLogger:
    """Enhanced logger wrapper with correlation tracking and performance monitoring."""
    
    def __init__(self, logger: logging.Logger, extra_fields: Optional[Dict[str, Any]] = None):
        self.logger = logger
        self.extra_fields = extra_fields or {}
    
    def _log(self, level: int, message: str, *args, correlation_id: Optional[str] = None, 
             extra: Optional[Dict[str, Any]] = None, **kwargs):
        """Internal logging method with enhanced context."""
        # Combine extra fields
        log_extra = {**self.extra_fields}
        if extra:
            log_extra.update(extra)
        
        # Add correlation ID if provided
        if correlation_id:
            log_extra['correlation_id'] = correlation_id
        
        # Add any additional kwargs as extra fields
        log_extra.update(kwargs)
        
        self.logger.log(level, message, *args, extra=log_extra)
    
    def debug(self, message: str, *args, correlation_id: Optional[str] = None, 
              extra: Optional[Dict[str, Any]] = None, **kwargs):
        """Log debug message."""
        self._log(logging.DEBUG, message, *args, correlation_id=correlation_id, extra=extra, **kwargs)
    
    def info(self, message: str, *args, correlation_id: Optional[str] = None, 
             extra: Optional[Dict[str, Any]] = None, **kwargs):
        """Log info message."""
        self._log(logging.INFO, message, *args, correlation_id=correlation_id, extra=extra, **kwargs)
    
    def warning(self, message: str, *args, correlation_id: Optional[str] = None, 
                extra: Optional[Dict[str, Any]] = None, **kwargs):
        """Log warning message."""
        self._log(logging.WARNING, message, *args, correlation_id=correlation_id, extra=extra, **kwargs)
    
    def error(self, message: str, *args, correlation_id: Optional[str] = None, 
              extra: Optional[Dict[str, Any]] = None, **kwargs):
        """Log error message."""
        self._log(logging.ERROR, message, *args, correlation_id=correlation_id, extra=extra, **kwargs)
    
    def critical(self, message: str, *args, correlation_id: Optional[str] = None, 
                 extra: Optional[Dict[str, Any]] = None, **kwargs):
        """Log critical message."""
        self._log(logging.CRITICAL, message, *args, correlation_id=correlation_id, extra=extra, **kwargs)
    
    def exception(self, message: str, *args, correlation_id: Optional[str] = None, 
                  extra: Optional[Dict[str, Any]] = None, **kwargs):
        """Log exception with traceback."""
        log_extra = {**self.extra_fields}
        if extra:
            log_extra.update(extra)
        if correlation_id:
            log_extra['correlation_id'] = correlation_id
        log_extra.update(kwargs)
        
        self.logger.exception(message, *args, extra=log_extra)
    
    @contextmanager
    def performance_context(self, operation: str, correlation_id: Optional[str] = None):
        """Context manager for performance tracking."""
        start_time = time.time()
        
        # Set up performance context in thread local
        _thread_local.performance_context = {
            'start_time': start_time,
            'operation': operation
        }
        
        if correlation_id:
            _thread_local.correlation_id = correlation_id
        
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.info(
                f"Operation '{operation}' completed",
                duration=duration,
                operation=operation,
                correlation_id=correlation_id
            )
            
            # Clean up thread local
            if hasattr(_thread_local, 'performance_context'):
                delattr(_thread_local, 'performance_context')
            if correlation_id and hasattr(_thread_local, 'correlation_id'):
                delattr(_thread_local, 'correlation_id')
    
    @contextmanager
    def correlation_context(self, correlation_id: Optional[str] = None):
        """Context manager for correlation ID tracking."""
        if correlation_id is None:
            correlation_id = str(uuid.uuid4())
        
        # Store in thread local
        old_correlation_id = getattr(_thread_local, 'correlation_id', None)
        _thread_local.correlation_id = correlation_id
        
        try:
            yield correlation_id
        finally:
            if old_correlation_id:
                _thread_local.correlation_id = old_correlation_id
            elif hasattr(_thread_local, 'correlation_id'):
                delattr(_thread_local, 'correlation_id')
    
    def set_correlation_id(self, correlation_id: str):
        """Set correlation ID for current thread."""
        _thread_local.correlation_id = correlation_id
    
    def get_correlation_id(self) -> Optional[str]:
        """Get current correlation ID."""
        return getattr(_thread_local, 'correlation_id', None)
    
    def clear_correlation_id(self):
        """Clear correlation ID for current thread."""
        if hasattr(_thread_local, 'correlation_id'):
            delattr(_thread_local, 'correlation_id')


# Global logger manager
_logger_manager: Optional[LoggerManager] = None


def setup_logging(config: LogConfig) -> LoggerManager:
    """Setup global logging configuration."""
    global _logger_manager
    _logger_manager = LoggerManager(config)
    return _logger_manager


def get_logger(name: str, extra_fields: Optional[Dict[str, Any]] = None) -> QuantStreamLogger:
    """Get logger instance."""
    if _logger_manager is None:
        # Setup with default config if not initialized
        default_config = LogConfig()
        setup_logging(default_config)
    
    return _logger_manager.get_logger(name, extra_fields)


def set_log_level(level: LogLevel):
    """Set global log level."""
    if _logger_manager:
        _logger_manager.set_level(level)


# Convenience functions
def log_api_call(logger: QuantStreamLogger, method: str, url: str, 
                status_code: Optional[int] = None, duration: Optional[float] = None,
                correlation_id: Optional[str] = None):
    """Log API call with structured information."""
    extra = {
        "api_method": method,
        "api_url": url
    }
    
    if status_code:
        extra["status_code"] = status_code
    if duration:
        extra["duration_ms"] = duration * 1000
    
    message = f"{method} {url}"
    if status_code:
        message += f" -> {status_code}"
    
    logger.info(message, correlation_id=correlation_id, extra=extra)


def log_message_processing(logger: QuantStreamLogger, message_type: str, 
                          symbol: str, processing_time: Optional[float] = None,
                          correlation_id: Optional[str] = None):
    """Log message processing with structured information."""
    extra = {
        "message_type": message_type,
        "symbol": symbol
    }
    
    if processing_time:
        extra["processing_time_ms"] = processing_time * 1000
    
    message = f"Processed {message_type} for {symbol}"
    logger.info(message, correlation_id=correlation_id, extra=extra)


def log_connector_event(logger: QuantStreamLogger, connector_name: str, 
                       event: str, details: Optional[Dict[str, Any]] = None,
                       correlation_id: Optional[str] = None):
    """Log connector events with structured information."""
    extra = {
        "connector_name": connector_name,
        "connector_event": event
    }
    
    if details:
        extra.update(details)
    
    message = f"Connector {connector_name}: {event}"
    logger.info(message, correlation_id=correlation_id, extra=extra)


# Default logging setup for module import
if __name__ != "__main__":
    default_config = LogConfig(
        level=LogLevel.INFO,
        console_output=True,
        file_output=False,  # Disable file output by default for library usage
        json_format=True
    )
    setup_logging(default_config)