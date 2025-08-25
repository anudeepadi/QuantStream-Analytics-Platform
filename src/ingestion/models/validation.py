"""
Data validation utilities for market data ingestion.

This module provides comprehensive validation for market data to ensure
data quality and consistency across different sources.
"""

import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Any, Optional, Union, Callable
from dataclasses import dataclass
from enum import Enum

from .market_data import (
    MarketData, Quote, Trade, Bar, OrderBook, NewsItem, FundamentalData,
    Symbol, AssetClass, DataQuality, MarketDataMessage
)


class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass


class ValidationSeverity(Enum):
    """Validation error severity levels."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationResult:
    """Result of a validation check."""
    field: str
    severity: ValidationSeverity
    message: str
    actual_value: Any = None
    expected_value: Any = None


@dataclass
class ValidationReport:
    """Comprehensive validation report."""
    is_valid: bool
    data_quality: DataQuality
    results: List[ValidationResult]
    
    @property
    def errors(self) -> List[ValidationResult]:
        """Get only error-level validation results."""
        return [r for r in self.results if r.severity == ValidationSeverity.ERROR]
    
    @property
    def warnings(self) -> List[ValidationResult]:
        """Get only warning-level validation results."""
        return [r for r in self.results if r.severity == ValidationSeverity.WARNING]
    
    def add_result(self, field: str, severity: ValidationSeverity, 
                   message: str, actual_value: Any = None, expected_value: Any = None):
        """Add a validation result."""
        self.results.append(ValidationResult(
            field=field,
            severity=severity,
            message=message,
            actual_value=actual_value,
            expected_value=expected_value
        ))


class BaseValidator:
    """Base class for all validators."""
    
    def __init__(self, strict_mode: bool = False):
        self.strict_mode = strict_mode
    
    def validate(self, data: MarketData) -> ValidationReport:
        """Validate market data and return a report."""
        report = ValidationReport(is_valid=True, data_quality=DataQuality.HIGH, results=[])
        
        try:
            self._validate_common_fields(data, report)
            self._validate_specific_fields(data, report)
            self._determine_data_quality(report)
        except Exception as e:
            report.add_result(
                field="validation",
                severity=ValidationSeverity.ERROR,
                message=f"Validation failed with exception: {str(e)}"
            )
        
        report.is_valid = len(report.errors) == 0
        return report
    
    def _validate_common_fields(self, data: MarketData, report: ValidationReport):
        """Validate fields common to all market data types."""
        # Validate symbol
        if hasattr(data, 'symbol'):
            self._validate_symbol(data.symbol, report)
        
        # Validate timestamp
        if hasattr(data, 'timestamp'):
            self._validate_timestamp(data.timestamp, report)
        
        # Validate metadata if present
        if hasattr(data, 'metadata') and data.metadata:
            self._validate_metadata(data.metadata, report)
    
    def _validate_specific_fields(self, data: MarketData, report: ValidationReport):
        """Validate fields specific to the data type. Override in subclasses."""
        pass
    
    def _validate_symbol(self, symbol: Symbol, report: ValidationReport):
        """Validate symbol fields."""
        if not symbol.ticker:
            report.add_result(
                field="symbol.ticker",
                severity=ValidationSeverity.ERROR,
                message="Ticker symbol cannot be empty"
            )
        
        # Validate ticker format (basic check)
        if symbol.ticker and not re.match(r'^[A-Z0-9._-]+$', symbol.ticker.upper()):
            report.add_result(
                field="symbol.ticker",
                severity=ValidationSeverity.WARNING,
                message="Ticker contains unusual characters",
                actual_value=symbol.ticker
            )
        
        # Validate currency code
        if symbol.currency and len(symbol.currency) != 3:
            report.add_result(
                field="symbol.currency",
                severity=ValidationSeverity.WARNING,
                message="Currency code should be 3 characters",
                actual_value=symbol.currency
            )
    
    def _validate_timestamp(self, timestamp: datetime, report: ValidationReport):
        """Validate timestamp fields."""
        if not timestamp:
            report.add_result(
                field="timestamp",
                severity=ValidationSeverity.ERROR,
                message="Timestamp cannot be None"
            )
            return
        
        # Check if timestamp is too far in the future
        now = datetime.now(timezone.utc)
        if timestamp.replace(tzinfo=timezone.utc) > now:
            time_diff = timestamp.replace(tzinfo=timezone.utc) - now
            if time_diff.total_seconds() > 60:  # More than 1 minute in the future
                report.add_result(
                    field="timestamp",
                    severity=ValidationSeverity.WARNING,
                    message="Timestamp is in the future",
                    actual_value=timestamp
                )
        
        # Check if timestamp is too old (older than 10 years)
        ten_years_ago = now.replace(year=now.year - 10)
        if timestamp.replace(tzinfo=timezone.utc) < ten_years_ago:
            report.add_result(
                field="timestamp",
                severity=ValidationSeverity.WARNING,
                message="Timestamp is very old",
                actual_value=timestamp
            )
    
    def _validate_metadata(self, metadata, report: ValidationReport):
        """Validate metadata fields."""
        if not metadata.message_id:
            report.add_result(
                field="metadata.message_id",
                severity=ValidationSeverity.ERROR,
                message="Message ID cannot be empty"
            )
    
    def _validate_price(self, price: Optional[Decimal], field_name: str, 
                       report: ValidationReport, allow_negative: bool = False):
        """Validate price fields."""
        if price is None:
            return
        
        if not allow_negative and price < 0:
            report.add_result(
                field=field_name,
                severity=ValidationSeverity.ERROR,
                message="Price cannot be negative",
                actual_value=price
            )
        
        # Check for extremely high prices (potential data error)
        if price > Decimal('1000000'):  # $1M per share/unit
            report.add_result(
                field=field_name,
                severity=ValidationSeverity.WARNING,
                message="Price seems unusually high",
                actual_value=price
            )
    
    def _validate_volume(self, volume: Optional[int], field_name: str, report: ValidationReport):
        """Validate volume/size fields."""
        if volume is None:
            return
        
        if volume < 0:
            report.add_result(
                field=field_name,
                severity=ValidationSeverity.ERROR,
                message="Volume/size cannot be negative",
                actual_value=volume
            )
        
        # Check for extremely high volumes
        if volume > 1_000_000_000:  # 1B shares/units
            report.add_result(
                field=field_name,
                severity=ValidationSeverity.WARNING,
                message="Volume seems unusually high",
                actual_value=volume
            )
    
    def _determine_data_quality(self, report: ValidationReport):
        """Determine overall data quality based on validation results."""
        error_count = len(report.errors)
        warning_count = len(report.warnings)
        
        if error_count > 0:
            report.data_quality = DataQuality.QUESTIONABLE
        elif warning_count > 2:
            report.data_quality = DataQuality.LOW
        elif warning_count > 0:
            report.data_quality = DataQuality.MEDIUM
        else:
            report.data_quality = DataQuality.HIGH


class QuoteValidator(BaseValidator):
    """Validator for Quote data."""
    
    def _validate_specific_fields(self, data: Quote, report: ValidationReport):
        """Validate quote-specific fields."""
        # Validate bid/ask prices
        self._validate_price(data.bid_price, "bid_price", report)
        self._validate_price(data.ask_price, "ask_price", report)
        
        # Validate bid/ask sizes
        self._validate_volume(data.bid_size, "bid_size", report)
        self._validate_volume(data.ask_size, "ask_size", report)
        
        # Check bid-ask spread sanity
        if (data.bid_price is not None and data.ask_price is not None and 
            data.bid_price > data.ask_price):
            report.add_result(
                field="spread",
                severity=ValidationSeverity.ERROR,
                message="Bid price cannot be higher than ask price",
                actual_value=f"bid: {data.bid_price}, ask: {data.ask_price}"
            )
        
        # Check for crossed market (warning level)
        if (data.bid_price is not None and data.ask_price is not None and 
            data.bid_price == data.ask_price):
            report.add_result(
                field="spread",
                severity=ValidationSeverity.WARNING,
                message="Zero spread detected (crossed market)",
                actual_value=data.bid_price
            )


class TradeValidator(BaseValidator):
    """Validator for Trade data."""
    
    def _validate_specific_fields(self, data: Trade, report: ValidationReport):
        """Validate trade-specific fields."""
        # Validate price and size
        self._validate_price(data.price, "price", report)
        self._validate_volume(data.size, "size", report)
        
        # Size should be positive for trades
        if data.size <= 0:
            report.add_result(
                field="size",
                severity=ValidationSeverity.ERROR,
                message="Trade size must be positive",
                actual_value=data.size
            )


class BarValidator(BaseValidator):
    """Validator for Bar (OHLCV) data."""
    
    def _validate_specific_fields(self, data: Bar, report: ValidationReport):
        """Validate bar-specific fields."""
        # Validate all OHLC prices
        for field_name in ['open_price', 'high_price', 'low_price', 'close_price']:
            price = getattr(data, field_name)
            self._validate_price(price, field_name, report)
        
        # Validate volume
        self._validate_volume(data.volume, "volume", report)
        
        # Validate OHLC relationships
        if all(getattr(data, f) is not None for f in ['open_price', 'high_price', 'low_price', 'close_price']):
            high = data.high_price
            low = data.low_price
            open_price = data.open_price
            close_price = data.close_price
            
            if high < max(open_price, close_price, low):
                report.add_result(
                    field="high_price",
                    severity=ValidationSeverity.ERROR,
                    message="High price should be the highest of OHLC",
                    actual_value=high
                )
            
            if low > min(open_price, close_price, high):
                report.add_result(
                    field="low_price",
                    severity=ValidationSeverity.ERROR,
                    message="Low price should be the lowest of OHLC",
                    actual_value=low
                )
        
        # Validate timeframe format
        if not re.match(r'^\d+[smhdwMy]$', data.timeframe):
            report.add_result(
                field="timeframe",
                severity=ValidationSeverity.WARNING,
                message="Timeframe format may be invalid",
                actual_value=data.timeframe
            )


class OrderBookValidator(BaseValidator):
    """Validator for OrderBook data."""
    
    def _validate_specific_fields(self, data: OrderBook, report: ValidationReport):
        """Validate order book specific fields."""
        # Validate bids and asks are not empty
        if not data.bids and not data.asks:
            report.add_result(
                field="order_book",
                severity=ValidationSeverity.ERROR,
                message="Order book must have at least bids or asks"
            )
        
        # Validate bid levels (should be descending order)
        if data.bids:
            for i in range(len(data.bids) - 1):
                if data.bids[i].price < data.bids[i + 1].price:
                    report.add_result(
                        field="bids",
                        severity=ValidationSeverity.ERROR,
                        message="Bid levels should be in descending price order",
                        actual_value=f"Level {i}: {data.bids[i].price} < Level {i+1}: {data.bids[i+1].price}"
                    )
        
        # Validate ask levels (should be ascending order)
        if data.asks:
            for i in range(len(data.asks) - 1):
                if data.asks[i].price > data.asks[i + 1].price:
                    report.add_result(
                        field="asks",
                        severity=ValidationSeverity.ERROR,
                        message="Ask levels should be in ascending price order",
                        actual_value=f"Level {i}: {data.asks[i].price} > Level {i+1}: {data.asks[i+1].price}"
                    )


class ValidatorFactory:
    """Factory for creating appropriate validators."""
    
    _validators = {
        Quote: QuoteValidator,
        Trade: TradeValidator,
        Bar: BarValidator,
        OrderBook: OrderBookValidator,
    }
    
    @classmethod
    def create_validator(cls, data_type: type, strict_mode: bool = False) -> BaseValidator:
        """Create appropriate validator for the data type."""
        validator_class = cls._validators.get(data_type, BaseValidator)
        return validator_class(strict_mode=strict_mode)
    
    @classmethod
    def register_validator(cls, data_type: type, validator_class: type):
        """Register a custom validator for a data type."""
        cls._validators[data_type] = validator_class


class DataQualityChecker:
    """High-level data quality checker."""
    
    def __init__(self, strict_mode: bool = False):
        self.strict_mode = strict_mode
        self.validation_history: Dict[str, List[ValidationReport]] = {}
    
    def check_data_quality(self, data: Union[MarketData, MarketDataMessage]) -> ValidationReport:
        """Check data quality and return validation report."""
        if isinstance(data, MarketDataMessage):
            market_data = data.data
        else:
            market_data = data
        
        validator = ValidatorFactory.create_validator(type(market_data), self.strict_mode)
        report = validator.validate(market_data)
        
        # Store validation history
        symbol_key = str(market_data.symbol) if hasattr(market_data, 'symbol') else 'unknown'
        if symbol_key not in self.validation_history:
            self.validation_history[symbol_key] = []
        self.validation_history[symbol_key].append(report)
        
        return report
    
    def get_quality_statistics(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Get quality statistics for a symbol or overall."""
        if symbol:
            reports = self.validation_history.get(symbol, [])
        else:
            reports = []
            for symbol_reports in self.validation_history.values():
                reports.extend(symbol_reports)
        
        if not reports:
            return {}
        
        total_reports = len(reports)
        quality_counts = {}
        for quality in DataQuality:
            count = sum(1 for r in reports if r.data_quality == quality)
            quality_counts[quality.value] = count
        
        error_count = sum(len(r.errors) for r in reports)
        warning_count = sum(len(r.warnings) for r in reports)
        
        return {
            "total_validations": total_reports,
            "quality_distribution": quality_counts,
            "total_errors": error_count,
            "total_warnings": warning_count,
            "average_errors_per_message": error_count / total_reports if total_reports > 0 else 0,
            "average_warnings_per_message": warning_count / total_reports if total_reports > 0 else 0
        }