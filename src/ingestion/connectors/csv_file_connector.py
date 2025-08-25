"""
CSV file connector for batch processing of historical market data.

This module provides connectors for processing CSV files with schema inference,
validation, Delta Lake integration, and support for compressed files.
"""

import asyncio
import csv
import gzip
import zipfile
import os
import io
from pathlib import Path
from datetime import datetime
from decimal import Decimal
from typing import AsyncIterator, Dict, Any, List, Optional, Union, TextIO
from dataclasses import dataclass, field
from enum import Enum
import logging
import aiofiles
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

from .base_connector import BaseConnector, ConnectorConfig, ConnectorState, DataError
from ..models import (
    MarketData, Quote, Trade, Bar, Symbol, DataSource, DataQuality,
    MarketDataMetadata, AssetClass
)
from ..models.validation import DataQualityChecker, ValidatorFactory
from ..utils import get_logger


class CSVFormat(Enum):
    """Supported CSV formats."""
    GENERIC = "generic"
    YAHOO_FINANCE = "yahoo_finance"
    ALPHA_VANTAGE = "alpha_vantage"
    POLYGON = "polygon"
    IEX = "iex"
    CUSTOM = "custom"


class CompressionType(Enum):
    """Supported compression types."""
    NONE = "none"
    GZIP = "gzip"
    ZIP = "zip"


@dataclass
class CSVSchema:
    """CSV schema definition."""
    columns: Dict[str, str]  # column_name -> data_type
    required_columns: List[str] = field(default_factory=list)
    date_format: str = "%Y-%m-%d %H:%M:%S"
    decimal_separator: str = "."
    thousands_separator: Optional[str] = None
    skip_rows: int = 0
    delimiter: str = ","
    quote_char: str = '"'
    escape_char: Optional[str] = None
    
    def validate(self) -> bool:
        """Validate schema definition."""
        if not self.columns:
            return False
        
        # Check if required columns exist in schema
        for col in self.required_columns:
            if col not in self.columns:
                return False
        
        return True


@dataclass
class CSVConnectorConfig(ConnectorConfig):
    """Configuration for CSV file connector."""
    file_path: str = ""
    file_pattern: str = "*.csv"
    directory_path: str = ""
    csv_format: CSVFormat = CSVFormat.GENERIC
    compression_type: CompressionType = CompressionType.NONE
    batch_size: int = 1000
    chunk_size: int = 10000
    schema: Optional[CSVSchema] = None
    auto_detect_schema: bool = True
    data_validation: bool = True
    skip_errors: bool = False
    max_errors: int = 100
    parallel_processing: bool = True
    max_workers: int = 4
    output_format: str = "delta"  # "delta", "parquet", "json"
    output_path: Optional[str] = None
    incremental_processing: bool = False
    checkpoint_file: Optional[str] = None
    
    def __post_init__(self):
        if not self.file_path and not self.directory_path:
            raise ValueError("Either file_path or directory_path must be specified")


class SchemaDetector:
    """Automatic schema detection for CSV files."""
    
    def __init__(self, sample_size: int = 1000):
        self.sample_size = sample_size
        self.logger = get_logger(self.__class__.__name__)
    
    async def detect_schema(self, file_path: str, delimiter: str = ",") -> CSVSchema:
        """Detect schema from CSV file."""
        try:
            # Read sample data
            sample_data = await self._read_sample(file_path, delimiter)
            
            # Analyze columns and data types
            columns = {}
            for col_name, series in sample_data.items():
                data_type = self._infer_data_type(series)
                columns[col_name] = data_type
            
            # Detect date format
            date_format = self._detect_date_format(sample_data)
            
            # Identify required columns based on common market data patterns
            required_columns = self._identify_required_columns(columns.keys())
            
            return CSVSchema(
                columns=columns,
                required_columns=required_columns,
                date_format=date_format,
                delimiter=delimiter
            )
            
        except Exception as e:
            self.logger.error(f"Error detecting schema for {file_path}: {e}")
            raise
    
    async def _read_sample(self, file_path: str, delimiter: str) -> pd.DataFrame:
        """Read sample data from CSV file."""
        def _read_sync():
            return pd.read_csv(
                file_path,
                delimiter=delimiter,
                nrows=self.sample_size,
                dtype=str  # Read as string for type inference
            )
        
        # Run pandas read_csv in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            return await loop.run_in_executor(executor, _read_sync)
    
    def _infer_data_type(self, series: pd.Series) -> str:
        """Infer data type from pandas series."""
        # Remove null values for analysis
        non_null_series = series.dropna()
        
        if non_null_series.empty:
            return "string"
        
        # Try to convert to numeric
        try:
            pd.to_numeric(non_null_series)
            # Check if all values are integers
            if all(self._is_integer(val) for val in non_null_series):
                return "integer"
            else:
                return "decimal"
        except (ValueError, TypeError):
            pass
        
        # Try to convert to datetime
        try:
            pd.to_datetime(non_null_series)
            return "datetime"
        except (ValueError, TypeError):
            pass
        
        # Check if boolean
        bool_values = {"true", "false", "1", "0", "yes", "no", "y", "n"}
        unique_values = set(str(val).lower() for val in non_null_series.unique())
        if unique_values.issubset(bool_values):
            return "boolean"
        
        return "string"
    
    def _is_integer(self, value: str) -> bool:
        """Check if string value represents an integer."""
        try:
            float_val = float(value)
            return float_val.is_integer()
        except (ValueError, TypeError):
            return False
    
    def _detect_date_format(self, sample_data: pd.DataFrame) -> str:
        """Detect date format from sample data."""
        # Common date patterns to try
        date_patterns = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y",
            "%Y%m%d",
            "%Y%m%d%H%M%S"
        ]
        
        # Look for columns that might contain dates
        date_columns = []
        for col_name in sample_data.columns:
            col_lower = col_name.lower()
            if any(keyword in col_lower for keyword in ['date', 'time', 'timestamp']):
                date_columns.append(col_name)
        
        if not date_columns:
            return "%Y-%m-%d %H:%M:%S"  # Default format
        
        # Try to parse the first date column with different patterns
        for col_name in date_columns:
            series = sample_data[col_name].dropna()
            if series.empty:
                continue
            
            for pattern in date_patterns:
                try:
                    pd.to_datetime(series.iloc[0], format=pattern)
                    return pattern
                except (ValueError, TypeError):
                    continue
        
        return "%Y-%m-%d %H:%M:%S"  # Default format
    
    def _identify_required_columns(self, column_names: List[str]) -> List[str]:
        """Identify required columns based on common market data patterns."""
        required = []
        column_names_lower = [col.lower() for col in column_names]
        
        # Look for timestamp column
        for col in column_names:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ['date', 'time', 'timestamp']):
                required.append(col)
                break
        
        # Look for symbol column
        for col in column_names:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ['symbol', 'ticker', 'instrument']):
                required.append(col)
                break
        
        # Look for price columns
        price_keywords = ['price', 'open', 'high', 'low', 'close', 'bid', 'ask']
        for keyword in price_keywords:
            for col in column_names:
                if keyword in col.lower():
                    required.append(col)
                    break
        
        return required


class CSVFileConnector(BaseConnector):
    """CSV file connector for batch processing of historical market data."""
    
    def __init__(self, config: CSVConnectorConfig):
        super().__init__(config, DataSource.CSV_FILE)
        self.csv_config = config
        self.schema_detector = SchemaDetector()
        self.data_quality_checker = DataQualityChecker()
        self.processed_files: List[str] = []
        self.error_count = 0
        
        # Load checkpoint if incremental processing is enabled
        if config.incremental_processing and config.checkpoint_file:
            self._load_checkpoint()
    
    async def _initialize(self) -> None:
        """Initialize CSV connector."""
        # Validate configuration
        if self.csv_config.file_path and not Path(self.csv_config.file_path).exists():
            raise DataError(f"File not found: {self.csv_config.file_path}")
        
        if self.csv_config.directory_path and not Path(self.csv_config.directory_path).exists():
            raise DataError(f"Directory not found: {self.csv_config.directory_path}")
        
        # Detect or validate schema
        if not self.csv_config.schema and self.csv_config.auto_detect_schema:
            sample_file = self._get_sample_file()
            if sample_file:
                self.csv_config.schema = await self.schema_detector.detect_schema(sample_file)
                self.logger.info(f"Detected schema: {self.csv_config.schema.columns}")
        
        if self.csv_config.schema and not self.csv_config.schema.validate():
            raise DataError("Invalid CSV schema configuration")
    
    async def _connect(self) -> None:
        """CSV connector doesn't need a connection."""
        self.logger.info("CSV file connector ready")
    
    async def _disconnect(self) -> None:
        """Save checkpoint if incremental processing is enabled."""
        if (self.csv_config.incremental_processing and 
            self.csv_config.checkpoint_file and 
            self.processed_files):
            self._save_checkpoint()
    
    def _get_sample_file(self) -> Optional[str]:
        """Get a sample file for schema detection."""
        if self.csv_config.file_path:
            return self.csv_config.file_path
        
        if self.csv_config.directory_path:
            directory = Path(self.csv_config.directory_path)
            for file_path in directory.glob(self.csv_config.file_pattern):
                return str(file_path)
        
        return None
    
    async def _fetch_data(self) -> AsyncIterator[MarketData]:
        """Process CSV files and yield market data."""
        files_to_process = self._get_files_to_process()
        
        if self.csv_config.parallel_processing:
            # Process files in parallel
            tasks = []
            semaphore = asyncio.Semaphore(self.csv_config.max_workers)
            
            for file_path in files_to_process:
                task = asyncio.create_task(
                    self._process_file_with_semaphore(semaphore, file_path)
                )
                tasks.append(task)
            
            # Process tasks as they complete
            for coro in asyncio.as_completed(tasks):
                try:
                    async for data in await coro:
                        yield data
                except Exception as e:
                    self.logger.error(f"Error processing file: {e}")
                    if not self.csv_config.skip_errors:
                        raise
        else:
            # Process files sequentially
            for file_path in files_to_process:
                try:
                    async for data in self._process_file(file_path):
                        yield data
                except Exception as e:
                    self.logger.error(f"Error processing file {file_path}: {e}")
                    if not self.csv_config.skip_errors:
                        raise
    
    async def _process_file_with_semaphore(self, semaphore: asyncio.Semaphore, 
                                         file_path: str) -> AsyncIterator[MarketData]:
        """Process file with semaphore for concurrency control."""
        async with semaphore:
            async for data in self._process_file(file_path):
                yield data
    
    def _get_files_to_process(self) -> List[str]:
        """Get list of files to process."""
        files = []
        
        if self.csv_config.file_path:
            files.append(self.csv_config.file_path)
        elif self.csv_config.directory_path:
            directory = Path(self.csv_config.directory_path)
            for file_path in directory.glob(self.csv_config.file_pattern):
                if str(file_path) not in self.processed_files:
                    files.append(str(file_path))
        
        return sorted(files)
    
    async def _process_file(self, file_path: str) -> AsyncIterator[MarketData]:
        """Process a single CSV file."""
        self.logger.info(f"Processing file: {file_path}")
        
        try:
            # Open file (handle compression)
            file_obj = await self._open_file(file_path)
            
            # Process in chunks
            chunk_count = 0
            async for chunk_data in self._read_csv_chunks(file_obj):
                chunk_count += 1
                self.logger.debug(f"Processing chunk {chunk_count} from {file_path}")
                
                for row_data in chunk_data:
                    try:
                        market_data = await self._parse_row(row_data, file_path)
                        if market_data:
                            # Validate data if enabled
                            if self.csv_config.data_validation:
                                validation_report = self.data_quality_checker.check_data_quality(market_data)
                                if not validation_report.is_valid and not self.csv_config.skip_errors:
                                    self.logger.error(f"Data validation failed: {validation_report.errors}")
                                    continue
                            
                            yield market_data
                    except Exception as e:
                        self.error_count += 1
                        self.logger.error(f"Error parsing row in {file_path}: {e}")
                        
                        if self.error_count >= self.csv_config.max_errors:
                            raise DataError(f"Too many errors ({self.error_count})")
                        
                        if not self.csv_config.skip_errors:
                            raise
            
            # Mark file as processed
            self.processed_files.append(file_path)
            self.logger.info(f"Completed processing file: {file_path}")
            
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {e}")
            raise
    
    async def _open_file(self, file_path: str) -> TextIO:
        """Open file with appropriate decompression."""
        if self.csv_config.compression_type == CompressionType.GZIP:
            return gzip.open(file_path, 'rt', encoding='utf-8')
        elif self.csv_config.compression_type == CompressionType.ZIP:
            with zipfile.ZipFile(file_path, 'r') as zip_file:
                # Get first CSV file in zip
                csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                if not csv_files:
                    raise DataError(f"No CSV files found in {file_path}")
                return io.StringIO(zip_file.read(csv_files[0]).decode('utf-8'))
        else:
            return open(file_path, 'r', encoding='utf-8')
    
    async def _read_csv_chunks(self, file_obj: TextIO) -> AsyncIterator[List[Dict[str, Any]]]:
        """Read CSV file in chunks."""
        def _read_chunk():
            # Skip header rows if specified
            for _ in range(self.csv_config.schema.skip_rows):
                next(file_obj, None)
            
            reader = csv.DictReader(
                file_obj,
                delimiter=self.csv_config.schema.delimiter,
                quotechar=self.csv_config.schema.quote_char,
                escapechar=self.csv_config.schema.escape_char
            )
            
            chunk = []
            for i, row in enumerate(reader):
                chunk.append(row)
                
                if (i + 1) % self.csv_config.chunk_size == 0:
                    yield chunk
                    chunk = []
            
            if chunk:  # Yield remaining rows
                yield chunk
        
        # Run in executor to avoid blocking the event loop
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            for chunk in await loop.run_in_executor(executor, lambda: list(_read_chunk())):
                yield chunk
    
    async def _parse_row(self, row_data: Dict[str, str], source_file: str) -> Optional[MarketData]:
        """Parse a CSV row into market data object."""
        if not self.csv_config.schema:
            raise DataError("Schema is required for row parsing")
        
        try:
            # Extract common fields
            symbol_str = self._extract_symbol(row_data)
            timestamp = self._extract_timestamp(row_data)
            
            if not symbol_str or not timestamp:
                return None
            
            symbol = Symbol(ticker=symbol_str)
            
            # Create metadata
            metadata = MarketDataMetadata(
                source=DataSource.CSV_FILE,
                source_timestamp=timestamp,
                quality=DataQuality.MEDIUM,
                raw_data=row_data
            )
            
            # Determine data type and create appropriate object
            if self._is_trade_data(row_data):
                return self._create_trade(row_data, symbol, timestamp, metadata)
            elif self._is_quote_data(row_data):
                return self._create_quote(row_data, symbol, timestamp, metadata)
            elif self._is_bar_data(row_data):
                return self._create_bar(row_data, symbol, timestamp, metadata)
            else:
                self.logger.warning(f"Unable to determine data type for row: {row_data}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error parsing row from {source_file}: {e}")
            raise
    
    def _extract_symbol(self, row_data: Dict[str, str]) -> Optional[str]:
        """Extract symbol from row data."""
        symbol_columns = ['symbol', 'ticker', 'instrument', 'Symbol', 'Ticker']
        for col in symbol_columns:
            if col in row_data and row_data[col]:
                return row_data[col].strip()
        return None
    
    def _extract_timestamp(self, row_data: Dict[str, str]) -> Optional[datetime]:
        """Extract timestamp from row data."""
        timestamp_columns = ['timestamp', 'date', 'time', 'datetime', 'Date', 'Time', 'Timestamp']
        
        for col in timestamp_columns:
            if col in row_data and row_data[col]:
                try:
                    return datetime.strptime(row_data[col], self.csv_config.schema.date_format)
                except ValueError:
                    continue
        
        return None
    
    def _is_trade_data(self, row_data: Dict[str, str]) -> bool:
        """Check if row contains trade data."""
        trade_indicators = ['price', 'size', 'volume', 'quantity', 'Price', 'Size', 'Volume']
        return any(col in row_data for col in trade_indicators)
    
    def _is_quote_data(self, row_data: Dict[str, str]) -> bool:
        """Check if row contains quote data."""
        quote_indicators = ['bid', 'ask', 'bid_price', 'ask_price', 'Bid', 'Ask']
        return any(col in row_data for col in quote_indicators)
    
    def _is_bar_data(self, row_data: Dict[str, str]) -> bool:
        """Check if row contains OHLC bar data."""
        bar_indicators = ['open', 'high', 'low', 'close', 'Open', 'High', 'Low', 'Close']
        return len([col for col in bar_indicators if col in row_data]) >= 3
    
    def _create_trade(self, row_data: Dict[str, str], symbol: Symbol, 
                     timestamp: datetime, metadata: MarketDataMetadata) -> Trade:
        """Create Trade object from row data."""
        price_columns = ['price', 'Price', 'trade_price']
        size_columns = ['size', 'Size', 'volume', 'Volume', 'quantity', 'Quantity']
        
        price = None
        for col in price_columns:
            if col in row_data and row_data[col]:
                price = Decimal(row_data[col])
                break
        
        size = None
        for col in size_columns:
            if col in row_data and row_data[col]:
                size = int(float(row_data[col]))
                break
        
        if price is None or size is None:
            raise ValueError("Required fields missing for trade data")
        
        return Trade(
            symbol=symbol,
            timestamp=timestamp,
            price=price,
            size=size,
            trade_id=row_data.get('trade_id'),
            metadata=metadata
        )
    
    def _create_quote(self, row_data: Dict[str, str], symbol: Symbol,
                     timestamp: datetime, metadata: MarketDataMetadata) -> Quote:
        """Create Quote object from row data."""
        bid_price = None
        ask_price = None
        bid_size = None
        ask_size = None
        
        bid_columns = ['bid', 'bid_price', 'Bid', 'Bid_Price']
        ask_columns = ['ask', 'ask_price', 'Ask', 'Ask_Price']
        bid_size_columns = ['bid_size', 'bid_quantity', 'Bid_Size']
        ask_size_columns = ['ask_size', 'ask_quantity', 'Ask_Size']
        
        for col in bid_columns:
            if col in row_data and row_data[col]:
                bid_price = Decimal(row_data[col])
                break
        
        for col in ask_columns:
            if col in row_data and row_data[col]:
                ask_price = Decimal(row_data[col])
                break
        
        for col in bid_size_columns:
            if col in row_data and row_data[col]:
                bid_size = int(float(row_data[col]))
                break
        
        for col in ask_size_columns:
            if col in row_data and row_data[col]:
                ask_size = int(float(row_data[col]))
                break
        
        return Quote(
            symbol=symbol,
            timestamp=timestamp,
            bid_price=bid_price,
            ask_price=ask_price,
            bid_size=bid_size,
            ask_size=ask_size,
            metadata=metadata
        )
    
    def _create_bar(self, row_data: Dict[str, str], symbol: Symbol,
                   timestamp: datetime, metadata: MarketDataMetadata) -> Bar:
        """Create Bar object from row data."""
        ohlc_columns = {
            'open': ['open', 'Open', 'open_price'],
            'high': ['high', 'High', 'high_price'],
            'low': ['low', 'Low', 'low_price'],
            'close': ['close', 'Close', 'close_price']
        }
        
        ohlc_values = {}
        for price_type, columns in ohlc_columns.items():
            for col in columns:
                if col in row_data and row_data[col]:
                    ohlc_values[price_type] = Decimal(row_data[col])
                    break
        
        if len(ohlc_values) < 4:
            raise ValueError("Required OHLC fields missing for bar data")
        
        # Extract volume
        volume = 0
        volume_columns = ['volume', 'Volume', 'vol']
        for col in volume_columns:
            if col in row_data and row_data[col]:
                volume = int(float(row_data[col]))
                break
        
        # Extract timeframe
        timeframe = row_data.get('timeframe', '1d')
        
        return Bar(
            symbol=symbol,
            timestamp=timestamp,
            timeframe=timeframe,
            open_price=ohlc_values['open'],
            high_price=ohlc_values['high'],
            low_price=ohlc_values['low'],
            close_price=ohlc_values['close'],
            volume=volume,
            metadata=metadata
        )
    
    def _load_checkpoint(self) -> None:
        """Load checkpoint file with processed files."""
        if not self.csv_config.checkpoint_file:
            return
        
        try:
            checkpoint_path = Path(self.csv_config.checkpoint_file)
            if checkpoint_path.exists():
                with open(checkpoint_path, 'r') as f:
                    self.processed_files = [line.strip() for line in f.readlines()]
                self.logger.info(f"Loaded checkpoint with {len(self.processed_files)} processed files")
        except Exception as e:
            self.logger.error(f"Error loading checkpoint: {e}")
    
    def _save_checkpoint(self) -> None:
        """Save checkpoint file with processed files."""
        if not self.csv_config.checkpoint_file:
            return
        
        try:
            checkpoint_path = Path(self.csv_config.checkpoint_file)
            checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(checkpoint_path, 'w') as f:
                for file_path in self.processed_files:
                    f.write(f"{file_path}\n")
            
            self.logger.info(f"Saved checkpoint with {len(self.processed_files)} processed files")
        except Exception as e:
            self.logger.error(f"Error saving checkpoint: {e}")


# Predefined schemas for common formats
YAHOO_FINANCE_SCHEMA = CSVSchema(
    columns={
        "Date": "datetime",
        "Open": "decimal",
        "High": "decimal",
        "Low": "decimal",
        "Close": "decimal",
        "Adj Close": "decimal",
        "Volume": "integer"
    },
    required_columns=["Date", "Open", "High", "Low", "Close", "Volume"],
    date_format="%Y-%m-%d"
)

ALPHA_VANTAGE_SCHEMA = CSVSchema(
    columns={
        "timestamp": "datetime",
        "open": "decimal",
        "high": "decimal",
        "low": "decimal",
        "close": "decimal",
        "volume": "integer"
    },
    required_columns=["timestamp", "open", "high", "low", "close", "volume"],
    date_format="%Y-%m-%d %H:%M:%S"
)

GENERIC_TRADE_SCHEMA = CSVSchema(
    columns={
        "timestamp": "datetime",
        "symbol": "string",
        "price": "decimal",
        "size": "integer"
    },
    required_columns=["timestamp", "symbol", "price", "size"],
    date_format="%Y-%m-%d %H:%M:%S"
)

# Example usage configuration
CSV_CONNECTOR_CONFIG = {
    'name': 'csv_historical',
    'directory_path': './data/historical',
    'file_pattern': '*.csv',
    'csv_format': CSVFormat.GENERIC,
    'batch_size': 1000,
    'auto_detect_schema': True,
    'data_validation': True,
    'parallel_processing': True,
    'incremental_processing': True,
    'checkpoint_file': './data/checkpoints/csv_processed.txt'
}