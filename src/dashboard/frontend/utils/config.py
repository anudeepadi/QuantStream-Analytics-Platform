"""
Configuration Utilities

Load and manage dashboard configuration settings.
"""

import yaml
import os
from typing import Dict, Any, Optional
import streamlit as st

def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load configuration from YAML file"""
    
    if config_path is None:
        # Default config path
        config_path = os.getenv('CONFIG_PATH', 'config/dashboard/config.yaml')
    
    try:
        # Try to load from the specified path
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        # Fallback to default configuration
        return get_default_config()
    except Exception as e:
        st.error(f"Error loading configuration: {str(e)}")
        return get_default_config()

def get_default_config() -> Dict[str, Any]:
    """Return default configuration"""
    
    return {
        'app': {
            'name': 'QuantStream Analytics Dashboard',
            'version': '1.0.0',
            'environment': 'development',
            'debug': True,
            'log_level': 'INFO'
        },
        'server': {
            'host': '0.0.0.0',
            'port': 8501,
            'max_connections': 1000,
            'timeout': 300
        },
        'database': {
            'url': 'postgresql://quantstream:password@localhost:5432/quantstream_db',
            'pool_size': 20,
            'max_overflow': 50,
            'echo': False
        },
        'redis': {
            'host': 'localhost',
            'port': 6379,
            'db': 0,
            'password': None,
            'max_connections': 100,
            'socket_timeout': 5,
            'socket_connect_timeout': 5
        },
        'websocket': {
            'host': 'localhost',
            'port': 8765,
            'max_connections': 1000,
            'ping_interval': 20,
            'ping_timeout': 10
        },
        'api': {
            'base_url': 'http://localhost:8000',
            'timeout': 30,
            'retry_attempts': 3,
            'rate_limit': 1000
        },
        'dashboard': {
            'refresh_rate': 1000,
            'max_data_points': 10000,
            'cache_duration': 60,
            'auto_refresh': True
        },
        'charts': {
            'default_theme': 'plotly_white',
            'candlestick': {
                'height': 500,
                'width': 800
            },
            'indicators': {
                'bollinger_bands': {
                    'period': 20,
                    'std_dev': 2
                },
                'rsi': {
                    'period': 14
                },
                'macd': {
                    'fast_period': 12,
                    'slow_period': 26,
                    'signal_period': 9
                }
            }
        },
        'monitoring': {
            'metrics_port': 8502,
            'health_check_interval': 30,
            'alert_threshold_cpu': 80,
            'alert_threshold_memory': 85,
            'alert_threshold_disk': 90
        },
        'security': {
            'secret_key': 'your-secret-key-here',
            'algorithm': 'HS256',
            'access_token_expire_minutes': 30,
            'allowed_origins': ['*'],
            'allowed_methods': ['*'],
            'allowed_headers': ['*']
        },
        'features': {
            'real_time_streaming': True,
            'technical_indicators': True,
            'anomaly_detection': True,
            'portfolio_tracking': True,
            'historical_analysis': True,
            'mobile_responsive': True
        }
    }

def get_config_value(config: Dict[str, Any], key_path: str, default: Any = None) -> Any:
    """Get nested configuration value using dot notation"""
    
    keys = key_path.split('.')
    value = config
    
    try:
        for key in keys:
            value = value[key]
        return value
    except (KeyError, TypeError):
        return default

def update_config_value(config: Dict[str, Any], key_path: str, value: Any) -> Dict[str, Any]:
    """Update nested configuration value using dot notation"""
    
    keys = key_path.split('.')
    current = config
    
    # Navigate to the parent dictionary
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    
    # Set the final value
    current[keys[-1]] = value
    
    return config

def save_config(config: Dict[str, Any], config_path: Optional[str] = None) -> bool:
    """Save configuration to YAML file"""
    
    if config_path is None:
        config_path = os.getenv('CONFIG_PATH', 'config/dashboard/config.yaml')
    
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        
        with open(config_path, 'w') as file:
            yaml.dump(config, file, default_flow_style=False, indent=2)
        
        return True
    except Exception as e:
        st.error(f"Error saving configuration: {str(e)}")
        return False

def validate_config(config: Dict[str, Any]) -> List[str]:
    """Validate configuration and return list of errors"""
    
    errors = []
    
    # Required sections
    required_sections = ['app', 'server', 'database', 'redis', 'api', 'dashboard']
    
    for section in required_sections:
        if section not in config:
            errors.append(f"Missing required section: {section}")
    
    # Validate specific settings
    if 'server' in config:
        server_config = config['server']
        
        if 'port' in server_config:
            port = server_config['port']
            if not isinstance(port, int) or port < 1 or port > 65535:
                errors.append("Server port must be a valid integer between 1 and 65535")
    
    if 'dashboard' in config:
        dashboard_config = config['dashboard']
        
        if 'refresh_rate' in dashboard_config:
            refresh_rate = dashboard_config['refresh_rate']
            if not isinstance(refresh_rate, int) or refresh_rate < 100:
                errors.append("Dashboard refresh rate must be an integer >= 100ms")
    
    return errors

@st.cache_data(ttl=300)
def get_environment_config() -> Dict[str, str]:
    """Get environment-specific configuration"""
    
    environment = os.getenv('ENVIRONMENT', 'development')
    
    env_configs = {
        'development': {
            'debug': 'true',
            'log_level': 'DEBUG',
            'auto_refresh': 'true'
        },
        'staging': {
            'debug': 'false',
            'log_level': 'INFO',
            'auto_refresh': 'true'
        },
        'production': {
            'debug': 'false',
            'log_level': 'WARNING',
            'auto_refresh': 'false'
        }
    }
    
    return env_configs.get(environment, env_configs['development'])

def merge_configs(base_config: Dict[str, Any], override_config: Dict[str, Any]) -> Dict[str, Any]:
    """Merge two configuration dictionaries recursively"""
    
    result = base_config.copy()
    
    for key, value in override_config.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_configs(result[key], value)
        else:
            result[key] = value
    
    return result