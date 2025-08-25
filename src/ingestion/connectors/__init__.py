"""Connector modules for market data ingestion."""

from .base_connector import (
    BaseConnector, ConnectorConfig, ConnectorState, ConnectorManager,
    ConnectorError, ConfigurationError, ConnectionError, DataError
)
from .rest_api_connector import (
    RestAPIConnector, AlphaVantageConnector, YahooFinanceConnector, IEXCloudConnector,
    APIConnectorConfig, APIProvider, create_api_connector,
    ALPHA_VANTAGE_CONFIG, YAHOO_FINANCE_CONFIG, IEX_CLOUD_CONFIG
)
from .websocket_connector import (
    BaseWebSocketConnector, FinnhubWebSocketConnector, PolygonWebSocketConnector,
    WebSocketConnectorConfig, WebSocketProvider, CircularBuffer, create_websocket_connector,
    FINNHUB_WEBSOCKET_CONFIG, POLYGON_WEBSOCKET_CONFIG
)
from .csv_file_connector import (
    CSVFileConnector, CSVConnectorConfig, CSVSchema, CSVFormat, CompressionType,
    SchemaDetector, YAHOO_FINANCE_SCHEMA, ALPHA_VANTAGE_SCHEMA, GENERIC_TRADE_SCHEMA,
    CSV_CONNECTOR_CONFIG
)

__all__ = [
    # Base connector
    "BaseConnector", "ConnectorConfig", "ConnectorState", "ConnectorManager",
    "ConnectorError", "ConfigurationError", "ConnectionError", "DataError",
    
    # REST API connectors
    "RestAPIConnector", "AlphaVantageConnector", "YahooFinanceConnector", "IEXCloudConnector",
    "APIConnectorConfig", "APIProvider", "create_api_connector",
    "ALPHA_VANTAGE_CONFIG", "YAHOO_FINANCE_CONFIG", "IEX_CLOUD_CONFIG",
    
    # WebSocket connectors
    "BaseWebSocketConnector", "FinnhubWebSocketConnector", "PolygonWebSocketConnector",
    "WebSocketConnectorConfig", "WebSocketProvider", "CircularBuffer", "create_websocket_connector",
    "FINNHUB_WEBSOCKET_CONFIG", "POLYGON_WEBSOCKET_CONFIG",
    
    # CSV file connectors
    "CSVFileConnector", "CSVConnectorConfig", "CSVSchema", "CSVFormat", "CompressionType",
    "SchemaDetector", "YAHOO_FINANCE_SCHEMA", "ALPHA_VANTAGE_SCHEMA", "GENERIC_TRADE_SCHEMA",
    "CSV_CONNECTOR_CONFIG"
]