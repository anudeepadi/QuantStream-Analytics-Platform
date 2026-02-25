"""
Market Data API Endpoints

REST API endpoints for real-time market data, historical data, and technical indicators.
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import numpy as np

from ...models.market_data import (
    MarketDataResponse,
    HistoricalDataRequest,
    TechnicalIndicatorsRequest,
    MarketDataPoint,
    TechnicalIndicator
)
from ...services.redis_service import RedisService

router = APIRouter()
_redis_service = None

def set_services(redis_svc):
    """Called from main.py lifespan to inject initialized Redis service."""
    global _redis_service
    _redis_service = redis_svc

def _redis():
    return _redis_service

@router.get("/current/{symbol}", response_model=MarketDataResponse)
async def get_current_market_data(
    symbol: str,
    include_indicators: bool = Query(False, description="Include technical indicators")
):
    """Get current market data for a symbol"""
    
    try:
        # Check Redis cache first
        cached_data = await _redis().get(f"market_data:{symbol}")
        
        if cached_data:
            return MarketDataResponse.parse_raw(cached_data)
        
        # Fetch from data source
        ticker = yf.Ticker(symbol.upper())
        
        # Get current data
        current_data = ticker.history(period="1d", interval="1m")
        if current_data.empty:
            raise HTTPException(status_code=404, detail=f"No data found for symbol {symbol}")
        
        latest = current_data.iloc[-1]
        
        # Create response
        market_data_point = MarketDataPoint(
            timestamp=datetime.now(),
            symbol=symbol.upper(),
            open=float(latest['Open']),
            high=float(latest['High']),
            low=float(latest['Low']),
            close=float(latest['Close']),
            volume=int(latest['Volume'])
        )
        
        technical_indicators = []
        if include_indicators:
            # Calculate basic indicators
            close_prices = current_data['Close'].values
            
            if len(close_prices) >= 20:
                # Simple moving average
                sma_20 = np.mean(close_prices[-20:])
                technical_indicators.append(
                    TechnicalIndicator(
                        name="SMA_20",
                        value=float(sma_20),
                        signal="neutral"
                    )
                )
            
            if len(close_prices) >= 14:
                # RSI calculation
                rsi = calculate_rsi(close_prices, 14)
                if not np.isnan(rsi):
                    signal = "overbought" if rsi > 70 else "oversold" if rsi < 30 else "neutral"
                    technical_indicators.append(
                        TechnicalIndicator(
                            name="RSI_14",
                            value=float(rsi),
                            signal=signal
                        )
                    )
        
        response = MarketDataResponse(
            data=market_data_point,
            indicators=technical_indicators,
            timestamp=datetime.now(),
            source="yfinance"
        )
        
        # Cache the response
        await _redis().set(
            f"market_data:{symbol}",
            response.json(),
            expire=30  # Cache for 30 seconds
        )
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching market data: {str(e)}")

@router.post("/historical", response_model=List[MarketDataPoint])
async def get_historical_data(request: HistoricalDataRequest):
    """Get historical market data"""
    
    try:
        # Validate date range
        if request.start_date >= request.end_date:
            raise HTTPException(status_code=400, detail="Start date must be before end date")
        
        # Check cache
        cache_key = f"historical:{request.symbol}:{request.start_date}:{request.end_date}:{request.interval}"
        cached_data = await _redis().get(cache_key)
        
        if cached_data:
            return [MarketDataPoint.parse_raw(item) for item in cached_data]
        
        # Fetch historical data
        ticker = yf.Ticker(request.symbol.upper())
        
        # Map interval to yfinance format
        interval_mapping = {
            "1m": "1m",
            "5m": "5m", 
            "15m": "15m",
            "30m": "30m",
            "1h": "1h",
            "1d": "1d",
            "1wk": "1wk",
            "1mo": "1mo"
        }
        
        yf_interval = interval_mapping.get(request.interval, "1d")
        
        historical_data = ticker.history(
            start=request.start_date,
            end=request.end_date,
            interval=yf_interval
        )
        
        if historical_data.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No historical data found for {request.symbol}"
            )
        
        # Convert to MarketDataPoint objects
        data_points = []
        for index, row in historical_data.iterrows():
            data_points.append(
                MarketDataPoint(
                    timestamp=index.to_pydatetime(),
                    symbol=request.symbol.upper(),
                    open=float(row['Open']),
                    high=float(row['High']),
                    low=float(row['Low']),
                    close=float(row['Close']),
                    volume=int(row['Volume'])
                )
            )
        
        # Cache the result
        await _redis().set(
            cache_key,
            [point.json() for point in data_points],
            expire=300  # Cache for 5 minutes
        )
        
        return data_points
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching historical data: {str(e)}")

@router.post("/indicators", response_model=List[TechnicalIndicator])
async def calculate_technical_indicators(request: TechnicalIndicatorsRequest):
    """Calculate technical indicators for a symbol"""
    
    try:
        # Get historical data first
        ticker = yf.Ticker(request.symbol.upper())
        data = ticker.history(period="6mo", interval="1d")  # 6 months of daily data
        
        if data.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No data available for {request.symbol}"
            )
        
        close_prices = data['Close'].values
        high_prices = data['High'].values
        low_prices = data['Low'].values
        volume = data['Volume'].values
        
        indicators = []
        
        # Calculate requested indicators
        if "RSI" in request.indicators:
            for period in request.periods.get("RSI", [14]):
                rsi = calculate_rsi(close_prices, period)
                if not np.isnan(rsi):
                    signal = "overbought" if rsi > 70 else "oversold" if rsi < 30 else "neutral"
                    indicators.append(
                        TechnicalIndicator(
                            name=f"RSI_{period}",
                            value=float(rsi),
                            signal=signal,
                            period=period
                        )
                    )
        
        if "SMA" in request.indicators:
            for period in request.periods.get("SMA", [20, 50]):
                if len(close_prices) >= period:
                    sma = np.mean(close_prices[-period:])
                    signal = "bullish" if close_prices[-1] > sma else "bearish"
                    indicators.append(
                        TechnicalIndicator(
                            name=f"SMA_{period}",
                            value=float(sma),
                            signal=signal,
                            period=period
                        )
                    )
        
        if "EMA" in request.indicators:
            for period in request.periods.get("EMA", [20, 50]):
                ema = calculate_ema(close_prices, period)
                if not np.isnan(ema):
                    signal = "bullish" if close_prices[-1] > ema else "bearish"
                    indicators.append(
                        TechnicalIndicator(
                            name=f"EMA_{period}",
                            value=float(ema),
                            signal=signal,
                            period=period
                        )
                    )
        
        if "MACD" in request.indicators:
            macd_line, signal_line, histogram = calculate_macd(close_prices)
            if not np.isnan(macd_line):
                signal = "bullish" if macd_line > signal_line else "bearish"
                indicators.extend([
                    TechnicalIndicator(
                        name="MACD_line",
                        value=float(macd_line),
                        signal=signal
                    ),
                    TechnicalIndicator(
                        name="MACD_signal",
                        value=float(signal_line),
                        signal="neutral"
                    ),
                    TechnicalIndicator(
                        name="MACD_histogram",
                        value=float(histogram),
                        signal="neutral"
                    )
                ])
        
        if "BOLLINGER_BANDS" in request.indicators:
            period = request.periods.get("BOLLINGER_BANDS", [20])[0]
            upper, middle, lower = calculate_bollinger_bands(close_prices, period, 2.0)
            
            if not np.isnan(upper):
                current_price = close_prices[-1]
                if current_price > upper:
                    signal = "overbought"
                elif current_price < lower:
                    signal = "oversold"
                else:
                    signal = "neutral"
                
                indicators.extend([
                    TechnicalIndicator(
                        name=f"BB_upper_{period}",
                        value=float(upper),
                        signal="resistance",
                        period=period
                    ),
                    TechnicalIndicator(
                        name=f"BB_middle_{period}",
                        value=float(middle),
                        signal="neutral",
                        period=period
                    ),
                    TechnicalIndicator(
                        name=f"BB_lower_{period}",
                        value=float(lower),
                        signal="support",
                        period=period
                    )
                ])
        
        return indicators
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating indicators: {str(e)}")

@router.get("/symbols", response_model=List[str])
async def get_available_symbols():
    """Get list of available symbols"""
    
    # Common symbols - in production, this would come from a database
    symbols = [
        "AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NFLX", "NVDA", "AMD", "INTC",
        "JPM", "BAC", "WFC", "GS", "MS", "V", "MA", "PYPL", "SQ",
        "SPY", "QQQ", "IWM", "VTI", "VOO"
    ]
    
    return symbols

@router.get("/market-status")
async def get_market_status():
    """Get current market status"""
    
    # Simple market hours check (US markets)
    now = datetime.now()
    
    # Market is open Monday-Friday, 9:30 AM - 4:00 PM EST
    weekday = now.weekday()
    hour = now.hour
    minute = now.minute
    
    is_open = (
        weekday < 5 and  # Monday-Friday
        (hour > 9 or (hour == 9 and minute >= 30)) and  # After 9:30 AM
        hour < 16  # Before 4:00 PM
    )
    
    return {
        "is_open": is_open,
        "next_open": "2024-01-23 09:30:00" if not is_open else None,
        "next_close": "2024-01-23 16:00:00" if is_open else None,
        "timezone": "America/New_York",
        "current_time": now.isoformat()
    }

# Helper functions for technical indicators

def calculate_rsi(prices: np.ndarray, period: int = 14) -> float:
    """Calculate RSI"""
    if len(prices) < period + 1:
        return np.nan
    
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    
    if avg_loss == 0:
        return 100.0
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi

def calculate_ema(prices: np.ndarray, period: int) -> float:
    """Calculate Exponential Moving Average"""
    if len(prices) < period:
        return np.nan
    
    multiplier = 2 / (period + 1)
    ema = prices[0]
    
    for price in prices[1:]:
        ema = (price * multiplier) + (ema * (1 - multiplier))
    
    return ema

def calculate_macd(prices: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9) -> tuple:
    """Calculate MACD"""
    if len(prices) < slow:
        return np.nan, np.nan, np.nan
    
    ema_fast = calculate_ema(prices, fast)
    ema_slow = calculate_ema(prices, slow)
    
    macd_line = ema_fast - ema_slow
    
    # For signal line, we need MACD history
    # Simplified calculation for this example
    signal_line = macd_line  # In real implementation, this would be EMA of MACD
    histogram = macd_line - signal_line
    
    return macd_line, signal_line, histogram

def calculate_bollinger_bands(prices: np.ndarray, period: int = 20, num_std: float = 2.0) -> tuple:
    """Calculate Bollinger Bands"""
    if len(prices) < period:
        return np.nan, np.nan, np.nan
    
    sma = np.mean(prices[-period:])
    std = np.std(prices[-period:])
    
    upper = sma + (num_std * std)
    lower = sma - (num_std * std)
    
    return upper, sma, lower


# ── Dashboard-facing aggregate endpoints ──────────────────────

WATCHED_SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "JPM"]
SECTOR_MAP = {
    "AAPL": "Technology", "GOOGL": "Technology", "MSFT": "Technology",
    "NVDA": "Technology", "META": "Technology", "AMZN": "Consumer Cyclical",
    "TSLA": "Automotive", "JPM": "Financial",
}

async def _fetch_quote(symbol: str) -> dict:
    """Fetch a single symbol quote via yfinance with Redis cache."""
    cache_key = f"quote:{symbol}"
    rs = _redis()
    if rs:
        cached = await rs.get(cache_key)
        if cached:
            import json
            return json.loads(cached) if isinstance(cached, str) else cached

    ticker = yf.Ticker(symbol)
    info = ticker.fast_info
    hist = ticker.history(period="2d")

    price = float(info.get("lastPrice", 0) if hasattr(info, "get") else getattr(info, "last_price", 0))
    prev_close = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else price
    change = price - prev_close
    change_pct = (change / prev_close * 100) if prev_close else 0
    volume = int(info.get("lastVolume", 0) if hasattr(info, "get") else getattr(info, "last_volume", 0))
    mcap = int(info.get("marketCap", 0) if hasattr(info, "get") else getattr(info, "market_cap", 0))

    result = {
        "symbol": symbol,
        "name": symbol,
        "price": round(price, 2),
        "change": round(change, 2),
        "change_percent": round(change_pct, 2),
        "volume": volume,
        "market_cap": mcap,
        "sector": SECTOR_MAP.get(symbol, "Other"),
    }

    if rs:
        import json
        await rs.set(cache_key, json.dumps(result), expire=30)

    return result


@router.get("/overview")
async def get_market_overview():
    """Aggregated overview of watched symbols — used by the Next.js dashboard."""
    import asyncio
    results = await asyncio.gather(*[_fetch_quote(s) for s in WATCHED_SYMBOLS], return_exceptions=True)
    return [r for r in results if isinstance(r, dict)]


@router.get("/sectors")
async def get_sector_performance():
    """Sector-level performance aggregation."""
    import asyncio
    quotes = await asyncio.gather(*[_fetch_quote(s) for s in WATCHED_SYMBOLS], return_exceptions=True)
    valid = [q for q in quotes if isinstance(q, dict)]

    sector_agg: dict = {}
    for q in valid:
        sec = q["sector"]
        if sec not in sector_agg:
            sector_agg[sec] = {"sector": sec, "change_percent": 0.0, "market_cap": 0, "volume": 0, "count": 0}
        sector_agg[sec]["change_percent"] += q["change_percent"]
        sector_agg[sec]["market_cap"] += q["market_cap"]
        sector_agg[sec]["volume"] += q["volume"]
        sector_agg[sec]["count"] += 1

    result = []
    for s in sector_agg.values():
        result.append({
            "sector": s["sector"],
            "change_percent": round(s["change_percent"] / max(s["count"], 1), 2),
            "market_cap": s["market_cap"],
            "volume": s["volume"],
        })
    return result


@router.get("/top-gainers")
async def get_top_gainers():
    """Top gaining symbols."""
    import asyncio
    quotes = await asyncio.gather(*[_fetch_quote(s) for s in WATCHED_SYMBOLS], return_exceptions=True)
    valid = [q for q in quotes if isinstance(q, dict)]
    return sorted(valid, key=lambda x: x["change_percent"], reverse=True)[:5]


@router.get("/top-losers")
async def get_top_losers():
    """Top losing symbols."""
    import asyncio
    quotes = await asyncio.gather(*[_fetch_quote(s) for s in WATCHED_SYMBOLS], return_exceptions=True)
    valid = [q for q in quotes if isinstance(q, dict)]
    return sorted(valid, key=lambda x: x["change_percent"])[:5]