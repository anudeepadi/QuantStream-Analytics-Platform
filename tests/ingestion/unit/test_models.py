"""
Unit tests for market data models.
"""

import pytest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import Mock, patch

from src.ingestion.models import (
    Symbol, Quote, Trade, Bar, MarketDataMetadata, DataSource, DataQuality,
    AssetClass, MarketDataMessage, DataType, IngestionMetrics
)
from src.ingestion.models.validation import (
    ValidationResult, ValidationReport, ValidationSeverity,
    BaseValidator, QuoteValidator, TradeValidator, BarValidator,
    ValidatorFactory, DataQualityChecker
)
from src.ingestion.models.schemas import (
    MarketDataEncoder, MarketDataDecoder, MarketDataSerializer
)


class TestSymbol:
    """Test Symbol model."""
    
    def test_symbol_creation(self):
        """Test basic symbol creation."""
        symbol = Symbol(ticker="AAPL", exchange="NASDAQ", asset_class=AssetClass.EQUITY)
        
        assert symbol.ticker == "AAPL"
        assert symbol.exchange == "NASDAQ"
        assert symbol.asset_class == AssetClass.EQUITY
        assert symbol.currency == "USD"  # Default value
    
    def test_symbol_str_representation(self):
        """Test symbol string representation."""
        symbol = Symbol(ticker="AAPL", exchange="NASDAQ")
        assert str(symbol) == "AAPL:NASDAQ"
        
        symbol_no_exchange = Symbol(ticker="AAPL")
        assert str(symbol_no_exchange) == "AAPL"
    
    def test_symbol_defaults(self):
        """Test symbol default values."""
        symbol = Symbol(ticker="AAPL")
        
        assert symbol.ticker == "AAPL"
        assert symbol.exchange is None
        assert symbol.asset_class is None
        assert symbol.currency == "USD"


class TestQuote:
    """Test Quote model."""
    
    def test_quote_creation(self):
        """Test quote creation with all fields."""
        symbol = Symbol(ticker="AAPL")
        timestamp = datetime.now(timezone.utc)
        metadata = MarketDataMetadata(source=DataSource.ALPHA_VANTAGE)
        
        quote = Quote(
            symbol=symbol,
            timestamp=timestamp,
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52"),
            bid_size=100,
            ask_size=200,
            metadata=metadata
        )
        
        assert quote.symbol == symbol
        assert quote.timestamp == timestamp
        assert quote.bid_price == Decimal("150.50")
        assert quote.ask_price == Decimal("150.52")
        assert quote.bid_size == 100
        assert quote.ask_size == 200
        assert quote.metadata == metadata
    
    def test_quote_mid_price(self):
        """Test mid price calculation."""
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        
        assert quote.mid_price == Decimal("150.51")
    
    def test_quote_mid_price_missing_prices(self):
        """Test mid price when prices are missing."""
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=None
        )
        
        assert quote.mid_price is None
    
    def test_quote_spread(self):
        """Test spread calculation."""
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        
        assert quote.spread == Decimal("0.02")
    
    def test_quote_spread_missing_prices(self):
        """Test spread when prices are missing."""
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=None,
            ask_price=Decimal("150.52")
        )
        
        assert quote.spread is None


class TestTrade:
    """Test Trade model."""
    
    def test_trade_creation(self):
        """Test trade creation."""
        symbol = Symbol(ticker="AAPL")
        timestamp = datetime.now(timezone.utc)
        metadata = MarketDataMetadata(source=DataSource.POLYGON)
        
        trade = Trade(
            symbol=symbol,
            timestamp=timestamp,
            price=Decimal("150.51"),
            size=500,
            trade_id="12345",
            conditions=["@", "F"],
            metadata=metadata
        )
        
        assert trade.symbol == symbol
        assert trade.timestamp == timestamp
        assert trade.price == Decimal("150.51")
        assert trade.size == 500
        assert trade.trade_id == "12345"
        assert trade.conditions == ["@", "F"]
        assert trade.metadata == metadata


class TestBar:
    """Test Bar model."""
    
    def test_bar_creation(self):
        """Test bar creation."""
        symbol = Symbol(ticker="AAPL")
        timestamp = datetime.now(timezone.utc)
        metadata = MarketDataMetadata(source=DataSource.YAHOO_FINANCE)
        
        bar = Bar(
            symbol=symbol,
            timestamp=timestamp,
            timeframe="1m",
            open_price=Decimal("150.00"),
            high_price=Decimal("150.75"),
            low_price=Decimal("149.50"),
            close_price=Decimal("150.50"),
            volume=10000,
            vwap=Decimal("150.25"),
            trade_count=100,
            metadata=metadata
        )
        
        assert bar.symbol == symbol
        assert bar.timestamp == timestamp
        assert bar.timeframe == "1m"
        assert bar.open_price == Decimal("150.00")
        assert bar.high_price == Decimal("150.75")
        assert bar.low_price == Decimal("149.50")
        assert bar.close_price == Decimal("150.50")
        assert bar.volume == 10000
        assert bar.vwap == Decimal("150.25")
        assert bar.trade_count == 100
        assert bar.metadata == metadata


class TestMarketDataMetadata:
    """Test MarketDataMetadata model."""
    
    def test_metadata_creation(self):
        """Test metadata creation with defaults."""
        metadata = MarketDataMetadata(source=DataSource.IEX_CLOUD)
        
        assert metadata.source == DataSource.IEX_CLOUD
        assert isinstance(metadata.message_id, str)
        assert len(metadata.message_id) > 0
        assert isinstance(metadata.ingestion_timestamp, datetime)
        assert metadata.quality == DataQuality.MEDIUM
    
    def test_metadata_with_all_fields(self):
        """Test metadata with all fields."""
        timestamp = datetime.now(timezone.utc)
        raw_data = {"test": "data"}
        
        metadata = MarketDataMetadata(
            message_id="custom-id",
            source=DataSource.FINNHUB,
            source_timestamp=timestamp,
            quality=DataQuality.HIGH,
            raw_data=raw_data,
            correlation_id="corr-123",
            partition_key="AAPL"
        )
        
        assert metadata.message_id == "custom-id"
        assert metadata.source == DataSource.FINNHUB
        assert metadata.source_timestamp == timestamp
        assert metadata.quality == DataQuality.HIGH
        assert metadata.raw_data == raw_data
        assert metadata.correlation_id == "corr-123"
        assert metadata.partition_key == "AAPL"


class TestMarketDataMessage:
    """Test MarketDataMessage wrapper."""
    
    def test_message_creation(self):
        """Test message wrapper creation."""
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        
        message = MarketDataMessage(
            data_type=DataType.QUOTE,
            data=quote,
            topic="market_data_quotes",
            partition_key="AAPL",
            headers={"source": "test"}
        )
        
        assert message.data_type == DataType.QUOTE
        assert message.data == quote
        assert message.topic == "market_data_quotes"
        assert message.partition_key == "AAPL"
        assert message.headers == {"source": "test"}
    
    def test_message_defaults(self):
        """Test message wrapper with defaults."""
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        
        message = MarketDataMessage(
            data_type=DataType.QUOTE,
            data=quote
        )
        
        assert message.partition_key == "AAPL"  # From symbol
        assert message.topic == "market_data_quote"  # From data type


class TestIngestionMetrics:
    """Test IngestionMetrics model."""
    
    def test_metrics_creation(self):
        """Test metrics creation."""
        metrics = IngestionMetrics(source=DataSource.ALPHA_VANTAGE)
        
        assert metrics.source == DataSource.ALPHA_VANTAGE
        assert metrics.messages_received == 0
        assert metrics.messages_processed == 0
        assert metrics.messages_failed == 0
        assert metrics.bytes_processed == 0
        assert metrics.processing_time_ms == 0.0
        assert metrics.last_message_timestamp is None
        assert metrics.errors == []
    
    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        metrics = IngestionMetrics(source=DataSource.ALPHA_VANTAGE)
        
        # No messages received
        assert metrics.success_rate == 0.0
        
        # Some messages processed
        metrics.messages_received = 100
        metrics.messages_processed = 90
        metrics.messages_failed = 10
        
        assert metrics.success_rate == 90.0
    
    def test_throughput_calculation(self):
        """Test throughput calculation."""
        metrics = IngestionMetrics(source=DataSource.ALPHA_VANTAGE)
        
        # No processing time
        assert metrics.throughput_per_second == 0.0
        
        # With processing time
        metrics.messages_processed = 1000
        metrics.processing_time_ms = 10000.0  # 10 seconds
        
        assert metrics.throughput_per_second == 100.0  # 1000/10


class TestValidation:
    """Test data validation components."""
    
    def test_validation_result(self):
        """Test validation result creation."""
        result = ValidationResult(
            field="price",
            severity=ValidationSeverity.ERROR,
            message="Price cannot be negative",
            actual_value=Decimal("-10.0"),
            expected_value="positive value"
        )
        
        assert result.field == "price"
        assert result.severity == ValidationSeverity.ERROR
        assert result.message == "Price cannot be negative"
        assert result.actual_value == Decimal("-10.0")
        assert result.expected_value == "positive value"
    
    def test_validation_report(self):
        """Test validation report."""
        report = ValidationReport(
            is_valid=False,
            data_quality=DataQuality.LOW,
            results=[]
        )
        
        # Add some results
        report.add_result("price", ValidationSeverity.ERROR, "Negative price")
        report.add_result("volume", ValidationSeverity.WARNING, "High volume")
        
        assert len(report.results) == 2
        assert len(report.errors) == 1
        assert len(report.warnings) == 1
        assert report.errors[0].field == "price"
        assert report.warnings[0].field == "volume"
    
    def test_quote_validator(self):
        """Test quote validator."""
        validator = QuoteValidator()
        
        # Valid quote
        symbol = Symbol(ticker="AAPL")
        valid_quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52"),
            bid_size=100,
            ask_size=200
        )
        
        report = validator.validate(valid_quote)
        assert report.is_valid
        assert len(report.errors) == 0
    
    def test_quote_validator_invalid_spread(self):
        """Test quote validator with invalid spread."""
        validator = QuoteValidator()
        
        # Invalid quote (bid > ask)
        symbol = Symbol(ticker="AAPL")
        invalid_quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.52"),  # Bid higher than ask
            ask_price=Decimal("150.50"),
            bid_size=100,
            ask_size=200
        )
        
        report = validator.validate(invalid_quote)
        assert not report.is_valid
        assert len(report.errors) == 1
        assert "bid price cannot be higher than ask price" in report.errors[0].message.lower()
    
    def test_trade_validator(self):
        """Test trade validator."""
        validator = TradeValidator()
        
        # Valid trade
        symbol = Symbol(ticker="AAPL")
        valid_trade = Trade(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            price=Decimal("150.51"),
            size=500
        )
        
        report = validator.validate(valid_trade)
        assert report.is_valid
        assert len(report.errors) == 0
    
    def test_trade_validator_invalid_size(self):
        """Test trade validator with invalid size."""
        validator = TradeValidator()
        
        # Invalid trade (zero size)
        symbol = Symbol(ticker="AAPL")
        invalid_trade = Trade(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            price=Decimal("150.51"),
            size=0  # Invalid size
        )
        
        report = validator.validate(invalid_trade)
        assert not report.is_valid
        assert len(report.errors) == 1
        assert "trade size must be positive" in report.errors[0].message.lower()
    
    def test_bar_validator(self):
        """Test bar validator."""
        validator = BarValidator()
        
        # Valid bar
        symbol = Symbol(ticker="AAPL")
        valid_bar = Bar(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            timeframe="1m",
            open_price=Decimal("150.00"),
            high_price=Decimal("150.75"),
            low_price=Decimal("149.50"),
            close_price=Decimal("150.50"),
            volume=10000
        )
        
        report = validator.validate(valid_bar)
        assert report.is_valid
        assert len(report.errors) == 0
    
    def test_bar_validator_invalid_ohlc(self):
        """Test bar validator with invalid OHLC relationships."""
        validator = BarValidator()
        
        # Invalid bar (high < open)
        symbol = Symbol(ticker="AAPL")
        invalid_bar = Bar(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            timeframe="1m",
            open_price=Decimal("150.00"),
            high_price=Decimal("149.00"),  # High lower than open
            low_price=Decimal("149.50"),
            close_price=Decimal("150.50"),
            volume=10000
        )
        
        report = validator.validate(invalid_bar)
        assert not report.is_valid
        assert len(report.errors) >= 1
    
    def test_validator_factory(self):
        """Test validator factory."""
        quote_validator = ValidatorFactory.create_validator(Quote)
        assert isinstance(quote_validator, QuoteValidator)
        
        trade_validator = ValidatorFactory.create_validator(Trade)
        assert isinstance(trade_validator, TradeValidator)
        
        bar_validator = ValidatorFactory.create_validator(Bar)
        assert isinstance(bar_validator, BarValidator)
    
    def test_data_quality_checker(self):
        """Test data quality checker."""
        checker = DataQualityChecker()
        
        # Valid quote
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        
        report = checker.check_data_quality(quote)
        assert report.is_valid
        assert report.data_quality in [DataQuality.HIGH, DataQuality.MEDIUM]
    
    def test_data_quality_checker_with_message(self):
        """Test data quality checker with MarketDataMessage."""
        checker = DataQualityChecker()
        
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        
        message = MarketDataMessage(data_type=DataType.QUOTE, data=quote)
        
        report = checker.check_data_quality(message)
        assert report.is_valid


class TestSerialization:
    """Test serialization components."""
    
    def test_market_data_encoder(self):
        """Test market data JSON encoder."""
        encoder = MarketDataEncoder()
        
        # Test decimal encoding
        decimal_value = Decimal("150.50")
        result = encoder.default(decimal_value)
        assert result == 150.50
        assert isinstance(result, float)
        
        # Test datetime encoding
        dt = datetime.now(timezone.utc)
        result = encoder.default(dt)
        assert isinstance(result, str)
        assert dt.isoformat() in result
        
        # Test enum encoding
        source = DataSource.ALPHA_VANTAGE
        result = encoder.default(source)
        assert result == "alpha_vantage"
    
    def test_market_data_serializer(self):
        """Test market data serializer."""
        serializer = MarketDataSerializer()
        
        # Create a quote
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        
        # Serialize to JSON
        json_str = serializer.serialize_to_json(quote)
        assert isinstance(json_str, str)
        assert "AAPL" in json_str
        assert "150.50" in json_str or "150.5" in json_str
        
        # Test that it's valid JSON
        import json
        parsed = json.loads(json_str)
        assert isinstance(parsed, dict)
        assert "symbol" in parsed
        assert "timestamp" in parsed


if __name__ == "__main__":
    pytest.main([__file__])