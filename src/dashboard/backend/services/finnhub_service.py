"""
Finnhub Service

Centralized client for Finnhub REST API and WebSocket.
Replaces yfinance with production-grade market data.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Optional

import httpx
import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)

# ── Company name + sector lookup ──────────────────────────────
COMPANY_INFO: dict[str, dict[str, str]] = {
    "AAPL": {"name": "Apple Inc.", "sector": "Technology"},
    "GOOGL": {"name": "Alphabet Inc.", "sector": "Technology"},
    "MSFT": {"name": "Microsoft Corp.", "sector": "Technology"},
    "AMZN": {"name": "Amazon.com Inc.", "sector": "Consumer Cyclical"},
    "TSLA": {"name": "Tesla Inc.", "sector": "Automotive"},
    "NVDA": {"name": "NVIDIA Corp.", "sector": "Technology"},
    "META": {"name": "Meta Platforms", "sector": "Technology"},
    "JPM": {"name": "JPMorgan Chase", "sector": "Financial"},
    "NFLX": {"name": "Netflix Inc.", "sector": "Technology"},
    "AMD": {"name": "AMD Inc.", "sector": "Technology"},
    "V": {"name": "Visa Inc.", "sector": "Financial"},
    "MA": {"name": "Mastercard Inc.", "sector": "Financial"},
    "BAC": {"name": "Bank of America", "sector": "Financial"},
    "WFC": {"name": "Wells Fargo", "sector": "Financial"},
    "GS": {"name": "Goldman Sachs", "sector": "Financial"},
    "DIS": {"name": "Walt Disney Co.", "sector": "Entertainment"},
    "PYPL": {"name": "PayPal Holdings", "sector": "Financial"},
    "INTC": {"name": "Intel Corp.", "sector": "Technology"},
    "SPY": {"name": "S&P 500 ETF", "sector": "ETF"},
    "QQQ": {"name": "Nasdaq 100 ETF", "sector": "ETF"},
}

FOREX_SYMBOLS: dict[str, str] = {
    "OANDA:EUR_USD": "EUR/USD",
    "OANDA:GBP_USD": "GBP/USD",
    "OANDA:USD_JPY": "USD/JPY",
    "OANDA:AUD_USD": "AUD/USD",
}

CRYPTO_SYMBOLS: dict[str, str] = {
    "BINANCE:BTCUSDT": "BTC/USD",
    "BINANCE:ETHUSDT": "ETH/USD",
    "BINANCE:SOLUSDT": "SOL/USD",
}

BASE_URL = "https://finnhub.io/api/v1"
WS_URL = "wss://ws.finnhub.io"


class RateLimiter:
    """Token-bucket rate limiter for Finnhub free tier (60 req/min)."""

    def __init__(self, max_calls: int = 55, period: float = 60.0):
        self._max = max_calls
        self._period = period
        self._timestamps: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            cutoff = now - self._period
            self._timestamps = [t for t in self._timestamps if t > cutoff]
            if len(self._timestamps) >= self._max:
                wait = self._timestamps[0] - cutoff
                logger.debug("Rate limit: sleeping %.1fs", wait)
                await asyncio.sleep(wait)
            self._timestamps.append(time.monotonic())


class FinnhubService:
    """Async Finnhub API client with rate limiting and caching."""

    def __init__(self) -> None:
        self._api_key: str = ""
        self._client: Optional[httpx.AsyncClient] = None
        self._limiter = RateLimiter()
        self._redis = None
        self._ws_task: Optional[asyncio.Task] = None
        self._ws_running = False
        self._trade_callbacks: list = []

    async def initialize(self, redis_service=None) -> None:
        self._api_key = os.getenv("FINNHUB_API_KEY", "")
        if not self._api_key:
            logger.warning("FINNHUB_API_KEY not set — market data will be unavailable")
        self._client = httpx.AsyncClient(timeout=15.0)
        self._redis = redis_service
        logger.info("FinnhubService initialized (key=%s…)", self._api_key[:8] if self._api_key else "NONE")

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
        await self.stop_ws()

    @property
    def is_configured(self) -> bool:
        return bool(self._api_key)

    # ── REST helpers ──────────────────────────────────────────

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> dict | list | None:
        if not self._client or not self._api_key:
            return None
        await self._limiter.acquire()
        merged = {"token": self._api_key, **(params or {})}
        try:
            resp = await self._client.get(f"{BASE_URL}{path}", params=merged)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            logger.error("Finnhub %s → %s", path, exc.response.status_code)
            return None
        except Exception as exc:
            logger.error("Finnhub request error (%s): %s", path, exc)
            return None

    async def _cached_get(self, cache_key: str, path: str, params: dict | None, ttl: int = 30) -> Any:
        if self._redis:
            cached = await self._redis.get(cache_key)
            if cached is not None:
                return cached
        data = await self._get(path, params)
        if data is not None and self._redis:
            await self._redis.set(cache_key, data, expire=ttl)
        return data

    # ── Stock quote ───────────────────────────────────────────

    async def get_quote(self, symbol: str) -> dict | None:
        """Finnhub /quote → {c, d, dp, h, l, o, pc, t}"""
        raw = await self._cached_get(f"fh:quote:{symbol}", "/quote", {"symbol": symbol}, ttl=15)
        if not raw or raw.get("c") is None or raw.get("c") == 0:
            return None

        info = COMPANY_INFO.get(symbol, {"name": symbol, "sector": "Other"})
        return {
            "symbol": symbol,
            "name": info["name"],
            "price": round(raw["c"], 2),
            "change": round(raw["d"] or 0, 2),
            "change_percent": round(raw["dp"] or 0, 2),
            "volume": 0,  # quote endpoint doesn't have volume
            "market_cap": 0,
            "sector": info["sector"],
            "high": round(raw["h"], 2),
            "low": round(raw["l"], 2),
            "open": round(raw["o"], 2),
            "prev_close": round(raw["pc"], 2),
        }

    async def get_quote_with_profile(self, symbol: str) -> dict | None:
        """Quote + company profile for market cap and volume."""
        quote = await self.get_quote(symbol)
        if not quote:
            return None

        profile = await self._cached_get(
            f"fh:profile:{symbol}", "/stock/profile2", {"symbol": symbol}, ttl=3600
        )
        if profile:
            quote["name"] = profile.get("name", quote["name"])
            quote["market_cap"] = int(profile.get("marketCapitalization", 0) * 1_000_000)
            quote["sector"] = profile.get("finnhubIndustry", quote["sector"])

        return quote

    # ── Historical candles ────────────────────────────────────

    async def get_candles(
        self, symbol: str, resolution: str, from_ts: int, to_ts: int
    ) -> list[dict] | None:
        """GET /stock/candle → list of OHLCV dicts."""
        raw = await self._cached_get(
            f"fh:candle:{symbol}:{resolution}:{from_ts}",
            "/stock/candle",
            {"symbol": symbol, "resolution": resolution, "from": from_ts, "to": to_ts},
            ttl=120,
        )
        if not raw or raw.get("s") != "ok":
            return None

        points = []
        for i in range(len(raw["t"])):
            points.append({
                "date": datetime.utcfromtimestamp(raw["t"][i]).strftime("%Y-%m-%d"),
                "open": round(raw["o"][i], 2),
                "high": round(raw["h"][i], 2),
                "low": round(raw["l"][i], 2),
                "close": round(raw["c"][i], 2),
                "volume": int(raw["v"][i]),
            })
        return points

    # ── Technical indicators ──────────────────────────────────

    async def get_indicator(
        self, symbol: str, indicator: str, resolution: str = "D",
        from_ts: int = 0, to_ts: int = 0, timeperiod: int = 14
    ) -> dict | None:
        """GET /indicator → technical indicator data."""
        if not from_ts:
            to_ts = int(datetime.utcnow().timestamp())
            from_ts = to_ts - 365 * 86400

        return await self._cached_get(
            f"fh:ind:{symbol}:{indicator}:{timeperiod}",
            "/indicator",
            {
                "symbol": symbol,
                "resolution": resolution,
                "from": from_ts,
                "to": to_ts,
                "indicator": indicator,
                "timeperiod": timeperiod,
            },
            ttl=120,
        )

    # ── Forex ─────────────────────────────────────────────────

    async def get_forex_rates(self, base: str = "USD") -> dict | None:
        return await self._cached_get(f"fh:forex:{base}", "/forex/rates", {"base": base}, ttl=60)

    async def get_forex_candles(
        self, symbol: str, resolution: str, from_ts: int, to_ts: int
    ) -> list[dict] | None:
        raw = await self._cached_get(
            f"fh:fxcandle:{symbol}:{resolution}:{from_ts}",
            "/forex/candle",
            {"symbol": symbol, "resolution": resolution, "from": from_ts, "to": to_ts},
            ttl=120,
        )
        if not raw or raw.get("s") != "ok":
            return None
        return [
            {
                "date": datetime.utcfromtimestamp(raw["t"][i]).strftime("%Y-%m-%d"),
                "open": round(raw["o"][i], 4),
                "high": round(raw["h"][i], 4),
                "low": round(raw["l"][i], 4),
                "close": round(raw["c"][i], 4),
                "volume": int(raw["v"][i]),
            }
            for i in range(len(raw["t"]))
        ]

    # ── Crypto ────────────────────────────────────────────────

    async def get_crypto_candles(
        self, symbol: str, resolution: str, from_ts: int, to_ts: int
    ) -> list[dict] | None:
        raw = await self._cached_get(
            f"fh:ccandle:{symbol}:{resolution}:{from_ts}",
            "/crypto/candle",
            {"symbol": symbol, "resolution": resolution, "from": from_ts, "to": to_ts},
            ttl=60,
        )
        if not raw or raw.get("s") != "ok":
            return None
        return [
            {
                "date": datetime.utcfromtimestamp(raw["t"][i]).strftime("%Y-%m-%d"),
                "open": round(raw["o"][i], 2),
                "high": round(raw["h"][i], 2),
                "low": round(raw["l"][i], 2),
                "close": round(raw["c"][i], 2),
                "volume": int(raw["v"][i]),
            }
            for i in range(len(raw["t"]))
        ]

    # ── News ──────────────────────────────────────────────────

    async def get_market_news(self, category: str = "general") -> list | None:
        return await self._cached_get(
            f"fh:news:{category}", "/news", {"category": category}, ttl=300
        )

    async def get_company_news(self, symbol: str, days: int = 7) -> list | None:
        to_date = datetime.utcnow().strftime("%Y-%m-%d")
        from_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        return await self._cached_get(
            f"fh:cnews:{symbol}",
            "/company-news",
            {"symbol": symbol, "from": from_date, "to": to_date},
            ttl=300,
        )

    # ── Market status ─────────────────────────────────────────

    async def get_market_status(self, exchange: str = "US") -> dict | None:
        return await self._cached_get(
            f"fh:mstatus:{exchange}", "/stock/market-status", {"exchange": exchange}, ttl=60
        )

    # ── Aggregate endpoints for dashboard ─────────────────────

    WATCHED_STOCKS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "JPM"]

    async def get_market_overview(self) -> list[dict]:
        """Parallel quote fetch for all watched symbols."""
        tasks = [self.get_quote(s) for s in self.WATCHED_STOCKS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if isinstance(r, dict)]

    async def get_sector_performance(self) -> list[dict]:
        overview = await self.get_market_overview()
        sector_agg: dict[str, dict] = {}
        for q in overview:
            sec = q["sector"]
            if sec not in sector_agg:
                sector_agg[sec] = {"sector": sec, "change_percent": 0.0, "market_cap": 0, "volume": 0, "count": 0}
            sector_agg[sec]["change_percent"] += q["change_percent"]
            sector_agg[sec]["market_cap"] += q["market_cap"]
            sector_agg[sec]["volume"] += q["volume"]
            sector_agg[sec]["count"] += 1
        return [
            {
                "sector": s["sector"],
                "change_percent": round(s["change_percent"] / max(s["count"], 1), 2),
                "market_cap": s["market_cap"],
                "volume": s["volume"],
            }
            for s in sector_agg.values()
        ]

    async def get_top_gainers(self, limit: int = 5) -> list[dict]:
        overview = await self.get_market_overview()
        return sorted(overview, key=lambda x: x["change_percent"], reverse=True)[:limit]

    async def get_top_losers(self, limit: int = 5) -> list[dict]:
        overview = await self.get_market_overview()
        return sorted(overview, key=lambda x: x["change_percent"])[:limit]

    # ── WebSocket (real-time trades) ──────────────────────────

    def on_trade(self, callback) -> None:
        """Register a callback for incoming trade data."""
        self._trade_callbacks.append(callback)

    async def start_ws(self, symbols: list[str] | None = None) -> None:
        """Connect to Finnhub WebSocket and stream trades.
        Only starts once per process (safe with multi-worker uvicorn)."""
        if self._ws_running or not self._api_key:
            return
        self._ws_running = True
        target_symbols = symbols or self.WATCHED_STOCKS
        # Small random jitter so workers don't all connect simultaneously
        await asyncio.sleep(asyncio.get_event_loop().time() % 3)
        self._ws_task = asyncio.create_task(self._ws_loop(target_symbols))

    async def stop_ws(self) -> None:
        self._ws_running = False
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass

    async def _ws_loop(self, symbols: list[str]) -> None:
        """Reconnecting WebSocket loop with exponential backoff."""
        backoff = 5
        max_backoff = 120
        while self._ws_running:
            try:
                async with websockets.connect(f"{WS_URL}?token={self._api_key}") as ws:
                    logger.info("Finnhub WS connected, subscribing to %d symbols", len(symbols))
                    backoff = 5  # reset on successful connect
                    for sym in symbols:
                        await ws.send(json.dumps({"type": "subscribe", "symbol": sym}))

                    async for raw_msg in ws:
                        msg = json.loads(raw_msg)
                        if msg.get("type") == "trade":
                            for cb in self._trade_callbacks:
                                try:
                                    await cb(msg["data"])
                                except Exception as exc:
                                    logger.error("Trade callback error: %s", exc)

            except ConnectionClosed:
                logger.warning("Finnhub WS disconnected, reconnecting in %ds", backoff)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Finnhub WS error: %s, retry in %ds", exc, backoff)

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
