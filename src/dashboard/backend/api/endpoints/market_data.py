"""
Market Data API Endpoints

REST API endpoints for real-time market data, historical data, and technical indicators.
Powered by Finnhub API.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime, timedelta

from ...models.market_data import (
    MarketDataResponse,
    HistoricalDataRequest,
    TechnicalIndicatorsRequest,
    MarketDataPoint,
    TechnicalIndicator,
)
from ...services.finnhub_service import FinnhubService

router = APIRouter()
_finnhub: Optional[FinnhubService] = None


def set_services(finnhub_svc: FinnhubService) -> None:
    """Called from main.py lifespan to inject initialized Finnhub service."""
    global _finnhub
    _finnhub = finnhub_svc


def _fh() -> FinnhubService:
    if _finnhub is None:
        raise HTTPException(status_code=503, detail="Market data service not initialized")
    return _finnhub


# ── Single-symbol endpoints ───────────────────────────────────

@router.get("/current/{symbol}", response_model=MarketDataResponse)
async def get_current_market_data(
    symbol: str,
    include_indicators: bool = Query(False, description="Include technical indicators"),
):
    """Get current market data for a symbol."""
    quote = await _fh().get_quote(symbol.upper())
    if not quote:
        raise HTTPException(status_code=404, detail=f"No data found for symbol {symbol}")

    data_point = MarketDataPoint(
        timestamp=datetime.utcnow(),
        symbol=symbol.upper(),
        open=quote["open"],
        high=quote["high"],
        low=quote["low"],
        close=quote["price"],
        volume=quote["volume"],
    )

    indicators: list[TechnicalIndicator] = []
    if include_indicators:
        indicators = await _fetch_indicators_for_symbol(symbol.upper())

    return MarketDataResponse(
        data=data_point,
        indicators=indicators,
        timestamp=datetime.utcnow(),
        source="finnhub",
    )


@router.post("/historical", response_model=List[dict])
async def get_historical_data(request: HistoricalDataRequest):
    """Get historical market data (OHLCV candles)."""
    if request.start_date >= request.end_date:
        raise HTTPException(status_code=400, detail="Start date must be before end date")

    from_ts = int(request.start_date.timestamp())
    to_ts = int(request.end_date.timestamp())

    resolution_map = {
        "1m": "1", "5m": "5", "15m": "15", "30m": "30",
        "1h": "60", "1d": "D", "1wk": "W", "1mo": "M",
    }
    resolution = resolution_map.get(request.interval.value, "D")

    sym = request.symbol.upper()

    # Route to the right candle endpoint
    if sym.startswith("BINANCE:"):
        candles = await _fh().get_crypto_candles(sym, resolution, from_ts, to_ts)
    elif sym.startswith("OANDA:"):
        candles = await _fh().get_forex_candles(sym, resolution, from_ts, to_ts)
    else:
        candles = await _fh().get_candles(sym, resolution, from_ts, to_ts)

    if not candles:
        raise HTTPException(status_code=404, detail=f"No historical data found for {sym}")

    return candles


@router.post("/indicators", response_model=List[TechnicalIndicator])
async def calculate_technical_indicators(request: TechnicalIndicatorsRequest):
    """Calculate technical indicators for a symbol via Finnhub."""
    return await _fetch_indicators_for_symbol(
        request.symbol.upper(), request.indicators, request.periods
    )


@router.get("/symbols", response_model=List[str])
async def get_available_symbols():
    """Get list of available symbols (stocks + crypto + forex)."""
    from ...services.finnhub_service import COMPANY_INFO, CRYPTO_SYMBOLS, FOREX_SYMBOLS

    stocks = list(COMPANY_INFO.keys())
    crypto = list(CRYPTO_SYMBOLS.keys())
    forex = list(FOREX_SYMBOLS.keys())
    return stocks + crypto + forex


@router.get("/market-status")
async def get_market_status():
    """Get current market status from Finnhub."""
    status = await _fh().get_market_status("US")
    if status:
        return {
            "is_open": status.get("isOpen", False),
            "exchange": status.get("exchange", "US"),
            "timezone": "America/New_York",
            "current_time": datetime.utcnow().isoformat(),
        }

    # Fallback: simple hours check
    now = datetime.utcnow()
    is_open = now.weekday() < 5 and 14 <= now.hour < 21  # ~9:30-4 ET in UTC
    return {
        "is_open": is_open,
        "exchange": "US",
        "timezone": "America/New_York",
        "current_time": now.isoformat(),
    }


# ── Dashboard aggregate endpoints ─────────────────────────────

@router.get("/overview")
async def get_market_overview():
    """Aggregated overview of watched symbols for the dashboard."""
    return await _fh().get_market_overview()


@router.get("/sectors")
async def get_sector_performance():
    """Sector-level performance aggregation."""
    return await _fh().get_sector_performance()


@router.get("/top-gainers")
async def get_top_gainers():
    """Top gaining symbols."""
    return await _fh().get_top_gainers()


@router.get("/top-losers")
async def get_top_losers():
    """Top losing symbols."""
    return await _fh().get_top_losers()


# ── News endpoints ────────────────────────────────────────────

@router.get("/news")
async def get_market_news(category: str = Query("general")):
    """Get market news headlines."""
    news = await _fh().get_market_news(category)
    if news is None:
        return []
    return news[:20]


@router.get("/news/{symbol}")
async def get_company_news(symbol: str, days: int = Query(7, ge=1, le=30)):
    """Get news for a specific company."""
    news = await _fh().get_company_news(symbol.upper(), days)
    if news is None:
        return []
    return news[:20]


# ── Internal helpers ──────────────────────────────────────────

async def _fetch_indicators_for_symbol(
    symbol: str,
    requested: list[str] | None = None,
    periods: dict[str, list[int]] | None = None,
) -> list[TechnicalIndicator]:
    """Fetch technical indicators from Finnhub for a symbol."""
    if requested is None:
        requested = ["RSI", "SMA", "EMA", "MACD", "BOLLINGER_BANDS"]
    if periods is None:
        periods = {"RSI": [14], "SMA": [20, 50], "EMA": [20, 50], "BOLLINGER_BANDS": [20]}

    now_ts = int(datetime.utcnow().timestamp())
    year_ago = now_ts - 365 * 86400
    indicators: list[TechnicalIndicator] = []

    indicator_map = {
        "RSI": "rsi",
        "SMA": "sma",
        "EMA": "ema",
        "MACD": "macd",
        "BOLLINGER_BANDS": "bbands",
    }

    for ind_name in requested:
        fh_name = indicator_map.get(ind_name)
        if not fh_name:
            continue

        for period in periods.get(ind_name, [14]):
            data = await _fh().get_indicator(
                symbol, fh_name, "D", year_ago, now_ts, period
            )
            if not data:
                continue

            # Extract the latest value from the response
            if fh_name == "rsi" and "rsi" in data:
                vals = data["rsi"]
                if vals:
                    latest = vals[-1]
                    sig = "buy" if latest < 30 else ("sell" if latest > 70 else "neutral")
                    indicators.append(TechnicalIndicator(
                        name=f"RSI_{period}", value=round(latest, 2), signal=sig, period=period
                    ))

            elif fh_name == "sma" and "sma" in data:
                vals = data["sma"]
                if vals:
                    indicators.append(TechnicalIndicator(
                        name=f"SMA_{period}", value=round(vals[-1], 2), signal="neutral", period=period
                    ))

            elif fh_name == "ema" and "ema" in data:
                vals = data["ema"]
                if vals:
                    indicators.append(TechnicalIndicator(
                        name=f"EMA_{period}", value=round(vals[-1], 2), signal="neutral", period=period
                    ))

            elif fh_name == "macd":
                macd_line = (data.get("macd") or [None])[-1]
                signal_line = (data.get("macdSignal") or [None])[-1]
                hist = (data.get("macdHist") or [None])[-1]
                if macd_line is not None:
                    sig = "buy" if (hist or 0) > 0 else "sell"
                    indicators.append(TechnicalIndicator(
                        name="MACD", value=round(macd_line, 4), signal=sig
                    ))
                    if signal_line is not None:
                        indicators.append(TechnicalIndicator(
                            name="MACD_signal", value=round(signal_line, 4), signal="neutral"
                        ))

            elif fh_name == "bbands":
                upper = (data.get("upperband") or [None])[-1]
                middle = (data.get("middleband") or [None])[-1]
                lower = (data.get("lowerband") or [None])[-1]
                if upper is not None:
                    indicators.append(TechnicalIndicator(
                        name=f"BOLLINGER_UPPER", value=round(upper, 2), signal="neutral", period=period
                    ))
                if middle is not None:
                    indicators.append(TechnicalIndicator(
                        name=f"BOLLINGER_MIDDLE", value=round(middle, 2), signal="neutral", period=period
                    ))
                if lower is not None:
                    indicators.append(TechnicalIndicator(
                        name=f"BOLLINGER_LOWER", value=round(lower, 2), signal="neutral", period=period
                    ))

    return indicators
