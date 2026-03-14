"""
WebSocket Manager

Handles real-time data streaming via WebSocket connections for the dashboard.
Receives live trades from Finnhub WS and relays to connected clients.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Optional, Set

import psutil
from fastapi import WebSocket, WebSocketDisconnect, APIRouter

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages WebSocket connections and broadcasting."""

    def __init__(self) -> None:
        self.active_connections: Dict[str, WebSocket] = {}
        self.subscriptions: Dict[str, Set[str]] = {}  # symbol -> connection_ids
        self.connection_metadata: Dict[str, Dict] = {}
        self.is_streaming = False
        self._latest_prices: Dict[str, dict] = {}  # symbol -> latest trade data

    async def connect(self, websocket: WebSocket, connection_id: str) -> None:
        await websocket.accept()
        self.active_connections[connection_id] = websocket
        self.connection_metadata[connection_id] = {
            "connected_at": datetime.utcnow(),
            "last_ping": datetime.utcnow(),
        }
        logger.info("WebSocket connected: %s (total: %d)", connection_id, len(self.active_connections))

    def disconnect(self, connection_id: str) -> None:
        self.active_connections.pop(connection_id, None)
        self.connection_metadata.pop(connection_id, None)
        for subs in self.subscriptions.values():
            subs.discard(connection_id)
        logger.info("WebSocket disconnected: %s", connection_id)

    async def send(self, message: dict, connection_id: str) -> None:
        ws = self.active_connections.get(connection_id)
        if not ws:
            return
        try:
            await ws.send_text(json.dumps(message, default=str))
        except Exception:
            self.disconnect(connection_id)

    async def broadcast_to_subscribers(self, symbol: str, message: dict) -> None:
        for cid in list(self.subscriptions.get(symbol, set())):
            await self.send(message, cid)

    async def broadcast_to_all(self, message: dict) -> None:
        for cid in list(self.active_connections):
            await self.send(message, cid)

    def subscribe(self, connection_id: str, symbol: str) -> None:
        self.subscriptions.setdefault(symbol, set()).add(connection_id)

    def unsubscribe(self, connection_id: str, symbol: str) -> None:
        if symbol in self.subscriptions:
            self.subscriptions[symbol].discard(connection_id)

    def get_connection_count(self) -> int:
        return len(self.active_connections)

    def get_subscription_count(self, symbol: str) -> int:
        return len(self.subscriptions.get(symbol, set()))

    def update_price(self, symbol: str, data: dict) -> None:
        self._latest_prices[symbol] = data

    def get_latest_price(self, symbol: str) -> Optional[dict]:
        return self._latest_prices.get(symbol)


# Global singleton
manager = ConnectionManager()
router = APIRouter()

# ── Finnhub trade callback ────────────────────────────────────

async def on_finnhub_trades(trades: list) -> None:
    """Called by FinnhubService when trade data arrives via WebSocket."""
    for trade in trades:
        symbol = trade.get("s", "")
        price = trade.get("p", 0)
        volume = trade.get("v", 0)
        ts = trade.get("t", 0)

        data = {
            "symbol": symbol,
            "price": round(price, 2),
            "volume": volume,
            "timestamp": datetime.utcfromtimestamp(ts / 1000).isoformat() if ts else datetime.utcnow().isoformat(),
        }
        manager.update_price(symbol, data)

        await manager.broadcast_to_subscribers(symbol, {
            "type": "trade",
            "symbol": symbol,
            "data": data,
        })
        # Also broadcast as general market_data for default listeners
        await manager.broadcast_to_all({
            "type": "market_data",
            "symbol": symbol,
            "data": data,
            "timestamp": datetime.utcnow().isoformat(),
        })


# ── Start / stop streaming ────────────────────────────────────

_streaming_task: Optional[asyncio.Task] = None


async def start_data_streaming() -> None:
    """Start the Finnhub WS relay. Called from main.py lifespan."""
    global _streaming_task
    manager.is_streaming = True
    logger.info("Data streaming enabled (Finnhub WS relay)")


async def stop_data_streaming() -> None:
    global _streaming_task
    manager.is_streaming = False
    if _streaming_task:
        _streaming_task.cancel()
    logger.info("Data streaming stopped")


# ── Client-facing WebSocket endpoints ─────────────────────────

@router.websocket("/market-data")
async def websocket_market_data(websocket: WebSocket) -> None:
    connection_id = f"ws_{datetime.utcnow().timestamp()}"
    try:
        await manager.connect(websocket, connection_id)
        await manager.send({
            "type": "welcome",
            "connection_id": connection_id,
            "timestamp": datetime.utcnow().isoformat(),
            "message": "Connected to QuantStream market data stream",
        }, connection_id)

        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            await _handle_client_message(msg, connection_id)

    except WebSocketDisconnect:
        manager.disconnect(connection_id)
    except Exception as exc:
        logger.error("WS error (%s): %s", connection_id, exc)
        manager.disconnect(connection_id)


async def _handle_client_message(message: dict, connection_id: str) -> None:
    msg_type = message.get("type")

    if msg_type == "subscribe":
        for sym in message.get("symbols", []):
            manager.subscribe(connection_id, sym.upper())
        await manager.send({
            "type": "subscription_confirmed",
            "symbols": message.get("symbols", []),
            "timestamp": datetime.utcnow().isoformat(),
        }, connection_id)

    elif msg_type == "unsubscribe":
        for sym in message.get("symbols", []):
            manager.unsubscribe(connection_id, sym.upper())
        await manager.send({
            "type": "unsubscription_confirmed",
            "symbols": message.get("symbols", []),
            "timestamp": datetime.utcnow().isoformat(),
        }, connection_id)

    elif msg_type == "ping":
        meta = manager.connection_metadata.get(connection_id)
        if meta:
            meta["last_ping"] = datetime.utcnow()
        await manager.send({"type": "pong", "timestamp": datetime.utcnow().isoformat()}, connection_id)

    elif msg_type == "get_status":
        await manager.send({
            "type": "status",
            "connections": manager.get_connection_count(),
            "subscriptions": {
                sym: manager.get_subscription_count(sym)
                for sym in manager.subscriptions
            },
            "timestamp": datetime.utcnow().isoformat(),
        }, connection_id)


@router.websocket("/system-metrics")
async def websocket_system_metrics(websocket: WebSocket) -> None:
    connection_id = f"sys_{datetime.utcnow().timestamp()}"
    try:
        await manager.connect(websocket, connection_id)
        while True:
            metrics = _get_system_metrics()
            await manager.send({
                "type": "system_metrics",
                "data": metrics,
                "timestamp": datetime.utcnow().isoformat(),
            }, connection_id)
            await asyncio.sleep(10)
    except WebSocketDisconnect:
        manager.disconnect(connection_id)
    except Exception as exc:
        logger.error("System metrics WS error: %s", exc)
        manager.disconnect(connection_id)


def _get_system_metrics() -> dict:
    """Gather real system metrics via psutil."""
    cpu = psutil.cpu_percent(interval=0)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    net = psutil.net_io_counters()
    return {
        "cpu_usage": cpu,
        "memory_usage": mem.percent,
        "disk_usage": disk.percent,
        "network_in": round(net.bytes_recv / 1_048_576, 1),
        "network_out": round(net.bytes_sent / 1_048_576, 1),
        "active_connections": manager.get_connection_count(),
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.get("/websocket/status")
async def get_websocket_status():
    return {
        "active_connections": manager.get_connection_count(),
        "subscriptions": {
            sym: manager.get_subscription_count(sym)
            for sym in manager.subscriptions
        },
        "is_streaming": manager.is_streaming,
        "timestamp": datetime.utcnow().isoformat(),
    }
