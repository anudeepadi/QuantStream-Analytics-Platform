"""
Configuration management system for the QuantStream Analytics Platform.

This module provides configuration loading, validation, and management
with support for multiple environments and hot-reload capabilities.
"""

import os
import yaml
import json
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field, asdict
from enum import Enum
import logging
from datetime import datetime
import threading
import time

from ..models import DataSource, DataType, AssetClass


class Environment(Enum):
    """Environment types."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TEST = "test"


@dataclass
class DatabaseConfig:
    """Database configuration."""
    host: str = "localhost"
    port: int = 5432
    database: str = "quantstream"
    username: str = ""
    password: str = ""
    ssl_enabled: bool = True
    connection_pool_size: int = 10
    connection_timeout: int = 30


@dataclass
class KafkaConfig:
    """Kafka configuration."""
    bootstrap_servers: List[str] = field(default_factory=lambda: ["localhost:9092"])
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    ssl_cafile: Optional[str] = None
    producer_config: Dict[str, Any] = field(default_factory=dict)
    consumer_config: Dict[str, Any] = field(default_factory=dict)
    topic_config: Dict[str, Dict[str, Any]] = field(default_factory=dict)


@dataclass
class RedisConfig:
    """Redis configuration for caching and rate limiting."""
    host: str = "localhost"
    port: int = 6379
    database: int = 0
    password: Optional[str] = None
    ssl_enabled: bool = False
    connection_pool_size: int = 10
    socket_timeout: int = 5


@dataclass
class APIEndpointConfig:
    """API endpoint configuration."""
    name: str
    base_url: str
    api_key: Optional[str] = None
    rate_limit: int = 60  # requests per minute
    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    headers: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, str] = field(default_factory=dict)


@dataclass
class ConnectorConfig:
    """Individual connector configuration."""
    name: str
    type: str  # "rest_api", "websocket", "csv_file"
    data_source: str
    enabled: bool = True
    symbols: List[str] = field(default_factory=list)
    data_types: List[str] = field(default_factory=list)
    batch_size: int = 100
    max_queue_size: int = 10000
    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    health_check_interval: float = 30.0
    metrics_interval: float = 60.0
    custom_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MonitoringConfig:
    """Monitoring and observability configuration."""
    enabled: bool = True
    metrics_port: int = 8080
    metrics_path: str = "/metrics"
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    prometheus_enabled: bool = True
    grafana_dashboard_enabled: bool = False
    alert_manager_url: Optional[str] = None
    alert_rules_path: Optional[str] = None


@dataclass
class PerformanceConfig:
    """Performance tuning configuration."""
    max_workers: int = 4
    message_buffer_size: int = 10000
    batch_processing_size: int = 1000
    compression_enabled: bool = True
    compression_type: str = "gzip"
    memory_limit_mb: int = 2048
    cpu_limit_percent: int = 80
    gc_threshold: int = 1000000  # messages


@dataclass
class QuantStreamConfig:
    """Main application configuration."""
    environment: Environment = Environment.DEVELOPMENT
    app_name: str = "quantstream-ingestion"
    version: str = "1.0.0"
    debug: bool = False
    
    # Component configurations
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    performance: PerformanceConfig = field(default_factory=PerformanceConfig)
    
    # Connector configurations
    connectors: Dict[str, ConnectorConfig] = field(default_factory=dict)
    api_endpoints: Dict[str, APIEndpointConfig] = field(default_factory=dict)
    
    # File paths
    data_directory: str = "./data"
    log_directory: str = "./logs"
    config_directory: str = "./config"
    temp_directory: str = "./temp"
    
    # Feature flags
    features: Dict[str, bool] = field(default_factory=lambda: {
        "real_time_processing": True,
        "batch_processing": True,
        "data_validation": True,
        "schema_registry": True,
        "dead_letter_queue": True,
        "metrics_collection": True,
        "health_checks": True,
        "hot_reload": True
    })


class ConfigurationError(Exception):
    """Configuration related errors."""
    pass


class ConfigLoader:
    """Configuration loader with validation and hot-reload support."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = Path(config_path) if config_path else Path("config/config.yaml")
        self.config: Optional[QuantStreamConfig] = None
        self.last_modified: Optional[float] = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self._lock = threading.RLock()
        self._hot_reload_enabled = False
        self._hot_reload_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
    
    def load_config(self, reload: bool = False) -> QuantStreamConfig:
        """Load configuration from file."""
        with self._lock:
            if self.config is not None and not reload:
                return self.config
            
            if not self.config_path.exists():
                self.logger.warning(f"Config file not found: {self.config_path}. Using defaults.")
                self.config = QuantStreamConfig()
                return self.config
            
            try:
                # Read configuration file
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    if self.config_path.suffix.lower() == '.json':
                        config_data = json.load(f)
                    else:
                        config_data = yaml.safe_load(f)
                
                # Parse configuration
                self.config = self._parse_config(config_data)
                self.last_modified = self.config_path.stat().st_mtime
                
                # Apply environment overrides
                self._apply_environment_overrides()
                
                # Validate configuration
                self._validate_config()
                
                self.logger.info(f"Configuration loaded successfully from {self.config_path}")
                return self.config
                
            except Exception as e:
                raise ConfigurationError(f"Failed to load configuration: {e}")
    
    def save_config(self, config: QuantStreamConfig, path: Optional[str] = None) -> None:
        """Save configuration to file."""
        save_path = Path(path) if path else self.config_path
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            config_data = asdict(config)
            
            with open(save_path, 'w', encoding='utf-8') as f:
                if save_path.suffix.lower() == '.json':
                    json.dump(config_data, f, indent=2, default=str)
                else:
                    yaml.dump(config_data, f, default_flow_style=False, sort_keys=True)
            
            self.logger.info(f"Configuration saved to {save_path}")
            
        except Exception as e:
            raise ConfigurationError(f"Failed to save configuration: {e}")
    
    def start_hot_reload(self, check_interval: float = 5.0) -> None:
        """Start hot-reload monitoring in a background thread."""
        if self._hot_reload_enabled:
            return
        
        self._hot_reload_enabled = True
        self._stop_event.clear()
        self._hot_reload_thread = threading.Thread(
            target=self._hot_reload_worker,
            args=(check_interval,),
            daemon=True,
            name="config-hot-reload"
        )
        self._hot_reload_thread.start()
        self.logger.info("Hot-reload monitoring started")
    
    def stop_hot_reload(self) -> None:
        """Stop hot-reload monitoring."""
        if not self._hot_reload_enabled:
            return
        
        self._hot_reload_enabled = False
        self._stop_event.set()
        if self._hot_reload_thread and self._hot_reload_thread.is_alive():
            self._hot_reload_thread.join(timeout=10)
        self.logger.info("Hot-reload monitoring stopped")
    
    def _hot_reload_worker(self, check_interval: float) -> None:
        """Background worker for hot-reload monitoring."""
        while not self._stop_event.is_set():
            try:
                if self.config_path.exists():
                    current_mtime = self.config_path.stat().st_mtime
                    if self.last_modified and current_mtime > self.last_modified:
                        self.logger.info("Configuration file changed, reloading...")
                        self.load_config(reload=True)
                
                self._stop_event.wait(check_interval)
                
            except Exception as e:
                self.logger.error(f"Hot-reload error: {e}")
                self._stop_event.wait(check_interval)
    
    def _parse_config(self, config_data: Dict[str, Any]) -> QuantStreamConfig:
        """Parse configuration data into QuantStreamConfig object."""
        # Convert environment string to enum
        if 'environment' in config_data:
            env_str = config_data['environment'].lower()
            for env in Environment:
                if env.value == env_str:
                    config_data['environment'] = env
                    break
        
        # Parse nested configurations
        if 'database' in config_data:
            config_data['database'] = DatabaseConfig(**config_data['database'])
        
        if 'kafka' in config_data:
            config_data['kafka'] = KafkaConfig(**config_data['kafka'])
        
        if 'redis' in config_data:
            config_data['redis'] = RedisConfig(**config_data['redis'])
        
        if 'monitoring' in config_data:
            config_data['monitoring'] = MonitoringConfig(**config_data['monitoring'])
        
        if 'performance' in config_data:
            config_data['performance'] = PerformanceConfig(**config_data['performance'])
        
        # Parse connector configurations
        if 'connectors' in config_data:
            connectors = {}
            for name, conn_config in config_data['connectors'].items():
                connectors[name] = ConnectorConfig(name=name, **conn_config)
            config_data['connectors'] = connectors
        
        # Parse API endpoint configurations
        if 'api_endpoints' in config_data:
            endpoints = {}
            for name, endpoint_config in config_data['api_endpoints'].items():
                endpoints[name] = APIEndpointConfig(name=name, **endpoint_config)
            config_data['api_endpoints'] = endpoints
        
        return QuantStreamConfig(**config_data)
    
    def _apply_environment_overrides(self) -> None:
        """Apply environment variable overrides."""
        if not self.config:
            return
        
        # Database overrides
        if os.getenv('DATABASE_HOST'):
            self.config.database.host = os.getenv('DATABASE_HOST')
        if os.getenv('DATABASE_PORT'):
            self.config.database.port = int(os.getenv('DATABASE_PORT'))
        if os.getenv('DATABASE_USERNAME'):
            self.config.database.username = os.getenv('DATABASE_USERNAME')
        if os.getenv('DATABASE_PASSWORD'):
            self.config.database.password = os.getenv('DATABASE_PASSWORD')
        
        # Kafka overrides
        if os.getenv('KAFKA_BOOTSTRAP_SERVERS'):
            servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS').split(',')
            self.config.kafka.bootstrap_servers = [s.strip() for s in servers]
        
        # Redis overrides
        if os.getenv('REDIS_HOST'):
            self.config.redis.host = os.getenv('REDIS_HOST')
        if os.getenv('REDIS_PORT'):
            self.config.redis.port = int(os.getenv('REDIS_PORT'))
        if os.getenv('REDIS_PASSWORD'):
            self.config.redis.password = os.getenv('REDIS_PASSWORD')
        
        # API key overrides
        for endpoint_name, endpoint_config in self.config.api_endpoints.items():
            env_key = f"{endpoint_name.upper()}_API_KEY"
            if os.getenv(env_key):
                endpoint_config.api_key = os.getenv(env_key)
    
    def _validate_config(self) -> None:
        """Validate configuration values."""
        if not self.config:
            raise ConfigurationError("Configuration is not loaded")
        
        # Validate required fields
        if not self.config.app_name:
            raise ConfigurationError("app_name is required")
        
        # Validate database configuration
        if self.config.database.port <= 0 or self.config.database.port > 65535:
            raise ConfigurationError("Invalid database port")
        
        # Validate Kafka configuration
        if not self.config.kafka.bootstrap_servers:
            raise ConfigurationError("Kafka bootstrap_servers is required")
        
        # Validate connector configurations
        for name, connector_config in self.config.connectors.items():
            if not connector_config.data_source:
                raise ConfigurationError(f"Connector {name} missing data_source")
            
            # Validate data source
            valid_sources = [ds.value for ds in DataSource]
            if connector_config.data_source not in valid_sources:
                raise ConfigurationError(f"Invalid data source: {connector_config.data_source}")
        
        # Validate API endpoint configurations
        for name, endpoint_config in self.config.api_endpoints.items():
            if not endpoint_config.base_url:
                raise ConfigurationError(f"API endpoint {name} missing base_url")
        
        # Validate directories
        for directory in [self.config.data_directory, self.config.log_directory]:
            if directory and not Path(directory).parent.exists():
                self.logger.warning(f"Parent directory does not exist: {directory}")


class ConfigManager:
    """Singleton configuration manager."""
    
    _instance: Optional['ConfigManager'] = None
    _lock = threading.Lock()
    
    def __new__(cls, config_path: Optional[str] = None) -> 'ConfigManager':
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config_path: Optional[str] = None):
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        self.loader = ConfigLoader(config_path)
        self.config: Optional[QuantStreamConfig] = None
        self._initialized = True
    
    def load_config(self) -> QuantStreamConfig:
        """Load and return configuration."""
        self.config = self.loader.load_config()
        return self.config
    
    def get_config(self) -> QuantStreamConfig:
        """Get current configuration, loading if necessary."""
        if self.config is None:
            return self.load_config()
        return self.config
    
    def reload_config(self) -> QuantStreamConfig:
        """Force reload configuration."""
        self.config = self.loader.load_config(reload=True)
        return self.config
    
    def start_hot_reload(self, check_interval: float = 5.0) -> None:
        """Start hot-reload monitoring."""
        self.loader.start_hot_reload(check_interval)
    
    def stop_hot_reload(self) -> None:
        """Stop hot-reload monitoring."""
        self.loader.stop_hot_reload()


# Utility functions
def get_config() -> QuantStreamConfig:
    """Get the global configuration instance."""
    return ConfigManager().get_config()


def create_default_config(path: str) -> None:
    """Create a default configuration file."""
    config = QuantStreamConfig()
    
    # Set some example values
    config.connectors = {
        "alpha_vantage": ConnectorConfig(
            name="alpha_vantage",
            type="rest_api",
            data_source="alpha_vantage",
            symbols=["AAPL", "GOOGL", "MSFT"],
            data_types=["quote", "bar"],
            custom_config={
                "function": "TIME_SERIES_INTRADAY",
                "interval": "1min"
            }
        ),
        "finnhub_websocket": ConnectorConfig(
            name="finnhub_websocket",
            type="websocket",
            data_source="finnhub",
            symbols=["AAPL", "GOOGL", "MSFT"],
            data_types=["trade", "quote"]
        )
    }
    
    config.api_endpoints = {
        "alpha_vantage": APIEndpointConfig(
            name="alpha_vantage",
            base_url="https://www.alphavantage.co/query",
            rate_limit=5  # 5 requests per minute for free tier
        ),
        "yahoo_finance": APIEndpointConfig(
            name="yahoo_finance",
            base_url="https://query1.finance.yahoo.com/v8/finance/chart",
            rate_limit=60
        ),
        "iex_cloud": APIEndpointConfig(
            name="iex_cloud",
            base_url="https://cloud.iexapis.com/stable",
            rate_limit=500
        )
    }
    
    # Save configuration
    loader = ConfigLoader()
    loader.save_config(config, path)
    print(f"Default configuration created at: {path}")


if __name__ == "__main__":
    # Create example configuration
    create_default_config("config/config.yaml")