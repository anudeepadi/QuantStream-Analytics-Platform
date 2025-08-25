"""
Market Data Models

Pydantic models for market data API requests and responses.
"""

from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum

class TimeInterval(str, Enum):
    """Time intervals for market data"""
    ONE_MINUTE = "1m"
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    THIRTY_MINUTES = "30m"
    ONE_HOUR = "1h"
    ONE_DAY = "1d"
    ONE_WEEK = "1wk"
    ONE_MONTH = "1mo"

class MarketDataPoint(BaseModel):
    """Single market data point"""
    timestamp: datetime
    symbol: str
    open: float = Field(..., description="Opening price")
    high: float = Field(..., description="Highest price")
    low: float = Field(..., description="Lowest price")
    close: float = Field(..., description="Closing price")
    volume: int = Field(..., description="Trading volume")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class TechnicalIndicator(BaseModel):
    """Technical indicator data"""
    name: str = Field(..., description="Indicator name (e.g., RSI_14)")
    value: float = Field(..., description="Indicator value")
    signal: str = Field(..., description="Signal interpretation (bullish/bearish/neutral)")
    period: Optional[int] = Field(None, description="Period used for calculation")
    timestamp: Optional[datetime] = Field(default_factory=datetime.now)

class MarketDataResponse(BaseModel):
    """Market data API response"""
    data: MarketDataPoint
    indicators: List[TechnicalIndicator] = Field(default_factory=list)
    timestamp: datetime
    source: str = Field(default="yfinance")

class HistoricalDataRequest(BaseModel):
    """Request for historical market data"""
    symbol: str = Field(..., description="Stock symbol (e.g., AAPL)")
    start_date: datetime = Field(..., description="Start date for historical data")
    end_date: datetime = Field(..., description="End date for historical data")
    interval: TimeInterval = Field(default=TimeInterval.ONE_DAY, description="Data interval")

class TechnicalIndicatorsRequest(BaseModel):
    """Request for technical indicators calculation"""
    symbol: str = Field(..., description="Stock symbol")
    indicators: List[str] = Field(..., description="List of indicators to calculate (RSI, SMA, EMA, MACD, BOLLINGER_BANDS)")
    periods: Dict[str, List[int]] = Field(
        default_factory=lambda: {
            "RSI": [14],
            "SMA": [20, 50],
            "EMA": [20, 50],
            "BOLLINGER_BANDS": [20]
        },
        description="Periods for each indicator"
    )

class MarketStatusResponse(BaseModel):
    """Market status response"""
    is_open: bool
    next_open: Optional[str]
    next_close: Optional[str]
    timezone: str
    current_time: str

class SymbolInfo(BaseModel):
    """Symbol information"""
    symbol: str
    name: str
    sector: Optional[str] = None
    industry: Optional[str] = None
    market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None

class MarketOverviewResponse(BaseModel):
    """Market overview response"""
    major_indices: Dict[str, Dict[str, Any]]
    top_gainers: List[Dict[str, Any]]
    top_losers: List[Dict[str, Any]]
    most_active: List[Dict[str, Any]]
    timestamp: datetime

class OrderBookEntry(BaseModel):
    """Order book entry"""
    price: float
    quantity: int
    side: str  # 'bid' or 'ask'

class OrderBookResponse(BaseModel):
    """Order book response"""
    symbol: str
    bids: List[OrderBookEntry]
    asks: List[OrderBookEntry]
    timestamp: datetime

class TradeData(BaseModel):
    """Individual trade data"""
    timestamp: datetime
    price: float
    quantity: int
    side: str  # 'buy' or 'sell'

class RecentTradesResponse(BaseModel):
    """Recent trades response"""
    symbol: str
    trades: List[TradeData]
    timestamp: datetime

class MarketDepthResponse(BaseModel):
    """Market depth response"""
    symbol: str
    bid_depth: List[OrderBookEntry]
    ask_depth: List[OrderBookEntry]
    spread: float
    timestamp: datetime