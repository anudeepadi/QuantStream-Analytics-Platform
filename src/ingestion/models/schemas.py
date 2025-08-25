"""
Schema definitions and serialization for market data.

This module provides Avro schemas and JSON serialization/deserialization
for all market data types.
"""

import json
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, Type, Optional, Union
from dataclasses import asdict, is_dataclass

from .market_data import (
    MarketData, MarketDataMessage, Quote, Trade, Bar, OrderBook, 
    NewsItem, FundamentalData, Symbol, MarketDataMetadata,
    AssetClass, DataSource, DataType, DataQuality
)


class MarketDataEncoder(json.JSONEncoder):
    """Custom JSON encoder for market data objects."""
    
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, (AssetClass, DataSource, DataType, DataQuality)):
            return obj.value
        elif is_dataclass(obj):
            return asdict(obj)
        return super().default(obj)


class MarketDataDecoder:
    """Custom decoder for market data objects."""
    
    @staticmethod
    def decode_datetime(value: str) -> datetime:
        """Decode ISO format datetime string."""
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            return datetime.fromisoformat(value)
    
    @staticmethod
    def decode_decimal(value: Union[str, float, int]) -> Decimal:
        """Decode decimal value from various formats."""
        return Decimal(str(value))
    
    @classmethod
    def decode_symbol(cls, data: Dict[str, Any]) -> Symbol:
        """Decode Symbol from dictionary."""
        asset_class = AssetClass(data['asset_class']) if data.get('asset_class') else None
        return Symbol(
            ticker=data['ticker'],
            exchange=data.get('exchange'),
            asset_class=asset_class,
            currency=data.get('currency', 'USD')
        )
    
    @classmethod
    def decode_metadata(cls, data: Dict[str, Any]) -> MarketDataMetadata:
        """Decode MarketDataMetadata from dictionary."""
        source = DataSource(data['source']) if data.get('source') else None
        source_timestamp = cls.decode_datetime(data['source_timestamp']) if data.get('source_timestamp') else None
        ingestion_timestamp = cls.decode_datetime(data['ingestion_timestamp'])
        quality = DataQuality(data['quality'])
        
        return MarketDataMetadata(
            message_id=data['message_id'],
            source=source,
            source_timestamp=source_timestamp,
            ingestion_timestamp=ingestion_timestamp,
            quality=quality,
            raw_data=data.get('raw_data'),
            correlation_id=data.get('correlation_id'),
            partition_key=data.get('partition_key')
        )


# Avro schema definitions
SYMBOL_SCHEMA = {
    "type": "record",
    "name": "Symbol",
    "fields": [
        {"name": "ticker", "type": "string"},
        {"name": "exchange", "type": ["null", "string"], "default": None},
        {"name": "asset_class", "type": ["null", "string"], "default": None},
        {"name": "currency", "type": "string", "default": "USD"}
    ]
}

METADATA_SCHEMA = {
    "type": "record",
    "name": "MarketDataMetadata",
    "fields": [
        {"name": "message_id", "type": "string"},
        {"name": "source", "type": ["null", "string"], "default": None},
        {"name": "source_timestamp", "type": ["null", "long"], "default": None},
        {"name": "ingestion_timestamp", "type": "long"},
        {"name": "quality", "type": "string"},
        {"name": "raw_data", "type": ["null", "string"], "default": None},
        {"name": "correlation_id", "type": ["null", "string"], "default": None},
        {"name": "partition_key", "type": ["null", "string"], "default": None}
    ]
}

QUOTE_SCHEMA = {
    "type": "record",
    "name": "Quote",
    "fields": [
        {"name": "symbol", "type": SYMBOL_SCHEMA},
        {"name": "timestamp", "type": "long"},
        {"name": "bid_price", "type": ["null", "double"], "default": None},
        {"name": "ask_price", "type": ["null", "double"], "default": None},
        {"name": "bid_size", "type": ["null", "int"], "default": None},
        {"name": "ask_size", "type": ["null", "int"], "default": None},
        {"name": "metadata", "type": METADATA_SCHEMA}
    ]
}

TRADE_SCHEMA = {
    "type": "record",
    "name": "Trade",
    "fields": [
        {"name": "symbol", "type": SYMBOL_SCHEMA},
        {"name": "timestamp", "type": "long"},
        {"name": "price", "type": "double"},
        {"name": "size", "type": "int"},
        {"name": "trade_id", "type": ["null", "string"], "default": None},
        {"name": "conditions", "type": ["null", {"type": "array", "items": "string"}], "default": None},
        {"name": "metadata", "type": METADATA_SCHEMA}
    ]
}

BAR_SCHEMA = {
    "type": "record",
    "name": "Bar",
    "fields": [
        {"name": "symbol", "type": SYMBOL_SCHEMA},
        {"name": "timestamp", "type": "long"},
        {"name": "timeframe", "type": "string"},
        {"name": "open_price", "type": "double"},
        {"name": "high_price", "type": "double"},
        {"name": "low_price", "type": "double"},
        {"name": "close_price", "type": "double"},
        {"name": "volume", "type": "int"},
        {"name": "vwap", "type": ["null", "double"], "default": None},
        {"name": "trade_count", "type": ["null", "int"], "default": None},
        {"name": "metadata", "type": METADATA_SCHEMA}
    ]
}

ORDER_BOOK_LEVEL_SCHEMA = {
    "type": "record",
    "name": "OrderBookLevel",
    "fields": [
        {"name": "price", "type": "double"},
        {"name": "size", "type": "int"},
        {"name": "order_count", "type": ["null", "int"], "default": None}
    ]
}

ORDER_BOOK_SCHEMA = {
    "type": "record",
    "name": "OrderBook",
    "fields": [
        {"name": "symbol", "type": SYMBOL_SCHEMA},
        {"name": "timestamp", "type": "long"},
        {"name": "bids", "type": {"type": "array", "items": ORDER_BOOK_LEVEL_SCHEMA}},
        {"name": "asks", "type": {"type": "array", "items": ORDER_BOOK_LEVEL_SCHEMA}},
        {"name": "sequence_number", "type": ["null", "int"], "default": None},
        {"name": "metadata", "type": METADATA_SCHEMA}
    ]
}

NEWS_SCHEMA = {
    "type": "record",
    "name": "NewsItem",
    "fields": [
        {"name": "symbol", "type": ["null", SYMBOL_SCHEMA], "default": None},
        {"name": "timestamp", "type": "long"},
        {"name": "headline", "type": "string"},
        {"name": "summary", "type": ["null", "string"], "default": None},
        {"name": "source", "type": ["null", "string"], "default": None},
        {"name": "url", "type": ["null", "string"], "default": None},
        {"name": "sentiment", "type": ["null", "double"], "default": None},
        {"name": "relevance_score", "type": ["null", "double"], "default": None},
        {"name": "metadata", "type": METADATA_SCHEMA}
    ]
}

FUNDAMENTAL_SCHEMA = {
    "type": "record",
    "name": "FundamentalData",
    "fields": [
        {"name": "symbol", "type": SYMBOL_SCHEMA},
        {"name": "timestamp", "type": "long"},
        {"name": "data_type", "type": "string"},
        {"name": "period", "type": "string"},
        {"name": "fiscal_year", "type": "int"},
        {"name": "data", "type": "string"},  # JSON string
        {"name": "metadata", "type": METADATA_SCHEMA}
    ]
}

# Schema registry mapping
SCHEMA_REGISTRY = {
    "quote": QUOTE_SCHEMA,
    "trade": TRADE_SCHEMA,
    "bar": BAR_SCHEMA,
    "order_book": ORDER_BOOK_SCHEMA,
    "news": NEWS_SCHEMA,
    "fundamental": FUNDAMENTAL_SCHEMA
}


class SchemaRegistry:
    """Registry for managing Avro schemas."""
    
    def __init__(self):
        self.schemas = SCHEMA_REGISTRY.copy()
    
    def get_schema(self, data_type: str) -> Optional[Dict[str, Any]]:
        """Get schema for a given data type."""
        return self.schemas.get(data_type)
    
    def register_schema(self, data_type: str, schema: Dict[str, Any]) -> None:
        """Register a new schema."""
        self.schemas[data_type] = schema
    
    def list_schemas(self) -> list[str]:
        """List all registered schema types."""
        return list(self.schemas.keys())


class MarketDataSerializer:
    """Serializer for market data objects."""
    
    def __init__(self, schema_registry: Optional[SchemaRegistry] = None):
        self.schema_registry = schema_registry or SchemaRegistry()
        self.encoder = MarketDataEncoder()
        self.decoder = MarketDataDecoder()
    
    def serialize_to_json(self, data: Union[MarketData, MarketDataMessage]) -> str:
        """Serialize market data to JSON string."""
        return json.dumps(data, cls=MarketDataEncoder, separators=(',', ':'))
    
    def deserialize_from_json(self, json_str: str, data_type: Type[MarketData]) -> MarketData:
        """Deserialize market data from JSON string."""
        data = json.loads(json_str)
        return self._deserialize_object(data, data_type)
    
    def _deserialize_object(self, data: Dict[str, Any], data_type: Type[MarketData]) -> MarketData:
        """Deserialize object from dictionary based on type."""
        # This is a simplified version - in production, you'd use proper deserialization
        # with type checking and validation
        if data_type == Quote:
            return Quote(
                symbol=self.decoder.decode_symbol(data['symbol']),
                timestamp=self.decoder.decode_datetime(data['timestamp']),
                bid_price=self.decoder.decode_decimal(data['bid_price']) if data.get('bid_price') else None,
                ask_price=self.decoder.decode_decimal(data['ask_price']) if data.get('ask_price') else None,
                bid_size=data.get('bid_size'),
                ask_size=data.get('ask_size'),
                metadata=self.decoder.decode_metadata(data['metadata'])
            )
        # Add other types as needed
        raise ValueError(f"Unsupported data type: {data_type}")
    
    def get_schema_for_type(self, data_type: DataType) -> Optional[Dict[str, Any]]:
        """Get Avro schema for a specific data type."""
        return self.schema_registry.get_schema(data_type.value)