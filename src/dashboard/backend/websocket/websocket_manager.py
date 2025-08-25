"""
WebSocket Manager

Handles real-time data streaming via WebSocket connections for the dashboard.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Set, Optional
import websockets
from websockets.exceptions import ConnectionClosed
from fastapi import WebSocket, WebSocketDisconnect, APIRouter
import yfinance as yf
import numpy as np

logger = logging.getLogger(__name__)

class ConnectionManager:
    """Manages WebSocket connections and broadcasting"""
    
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.subscriptions: Dict[str, Set[str]] = {}  # symbol -> set of connection_ids
        self.connection_metadata: Dict[str, Dict] = {}
        self.streaming_task: Optional[asyncio.Task] = None
        self.is_streaming = False
    
    async def connect(self, websocket: WebSocket, connection_id: str, user_id: str = None):
        """Accept new WebSocket connection"""
        await websocket.accept()
        
        self.active_connections[connection_id] = websocket
        self.connection_metadata[connection_id] = {
            "user_id": user_id,
            "connected_at": datetime.now(),
            "last_ping": datetime.now()
        }
        
        logger.info(f"WebSocket connection established: {connection_id}")
    
    def disconnect(self, connection_id: str):
        """Remove WebSocket connection"""
        if connection_id in self.active_connections:
            del self.active_connections[connection_id]
        
        if connection_id in self.connection_metadata:
            del self.connection_metadata[connection_id]
        
        # Remove from all subscriptions
        for symbol in self.subscriptions:
            self.subscriptions[symbol].discard(connection_id)
        
        logger.info(f"WebSocket connection closed: {connection_id}")
    
    async def send_personal_message(self, message: dict, connection_id: str):
        """Send message to specific connection"""
        if connection_id in self.active_connections:
            websocket = self.active_connections[connection_id]
            try:
                await websocket.send_text(json.dumps(message))
            except ConnectionClosed:
                self.disconnect(connection_id)
            except Exception as e:
                logger.error(f"Error sending message to {connection_id}: {e}")
                self.disconnect(connection_id)
    
    async def broadcast_to_subscribers(self, symbol: str, message: dict):
        """Broadcast message to all subscribers of a symbol"""
        if symbol in self.subscriptions:
            subscribers = self.subscriptions[symbol].copy()
            
            for connection_id in subscribers:
                await self.send_personal_message(message, connection_id)
    
    async def broadcast_to_all(self, message: dict):
        """Broadcast message to all connected clients"""
        if self.active_connections:
            connection_ids = list(self.active_connections.keys())
            
            for connection_id in connection_ids:
                await self.send_personal_message(message, connection_id)
    
    def subscribe_to_symbol(self, connection_id: str, symbol: str):
        """Subscribe connection to symbol updates"""
        if symbol not in self.subscriptions:
            self.subscriptions[symbol] = set()
        
        self.subscriptions[symbol].add(connection_id)
        logger.info(f"Connection {connection_id} subscribed to {symbol}")
    
    def unsubscribe_from_symbol(self, connection_id: str, symbol: str):
        """Unsubscribe connection from symbol updates"""
        if symbol in self.subscriptions:
            self.subscriptions[symbol].discard(connection_id)
            logger.info(f"Connection {connection_id} unsubscribed from {symbol}")
    
    def get_connection_count(self) -> int:
        """Get number of active connections"""
        return len(self.active_connections)
    
    def get_subscription_count(self, symbol: str) -> int:
        """Get number of subscribers for a symbol"""
        return len(self.subscriptions.get(symbol, set()))

# Global connection manager
manager = ConnectionManager()

# FastAPI router for WebSocket endpoints
router = APIRouter()

@router.websocket("/market-data")
async def websocket_market_data(websocket: WebSocket):
    """WebSocket endpoint for real-time market data"""
    
    connection_id = f"ws_{datetime.now().timestamp()}"
    
    try:
        await manager.connect(websocket, connection_id)
        
        # Send welcome message
        await manager.send_personal_message({
            "type": "welcome",
            "connection_id": connection_id,
            "timestamp": datetime.now().isoformat(),
            "message": "Connected to QuantStream market data stream"
        }, connection_id)
        
        # Listen for client messages
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            await handle_client_message(message, connection_id)
            
    except WebSocketDisconnect:
        manager.disconnect(connection_id)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(connection_id)

async def handle_client_message(message: dict, connection_id: str):
    """Handle messages from WebSocket clients"""
    
    message_type = message.get("type")
    
    if message_type == "subscribe":
        symbols = message.get("symbols", [])
        for symbol in symbols:
            manager.subscribe_to_symbol(connection_id, symbol.upper())
        
        await manager.send_personal_message({
            "type": "subscription_confirmed",
            "symbols": symbols,
            "timestamp": datetime.now().isoformat()
        }, connection_id)
    
    elif message_type == "unsubscribe":
        symbols = message.get("symbols", [])
        for symbol in symbols:
            manager.unsubscribe_from_symbol(connection_id, symbol.upper())
        
        await manager.send_personal_message({
            "type": "unsubscription_confirmed",
            "symbols": symbols,
            "timestamp": datetime.now().isoformat()
        }, connection_id)
    
    elif message_type == "ping":
        # Update last ping time
        if connection_id in manager.connection_metadata:
            manager.connection_metadata[connection_id]["last_ping"] = datetime.now()
        
        await manager.send_personal_message({
            "type": "pong",
            "timestamp": datetime.now().isoformat()
        }, connection_id)
    
    elif message_type == "get_status":
        await manager.send_personal_message({
            "type": "status",
            "connections": manager.get_connection_count(),
            "subscriptions": {
                symbol: manager.get_subscription_count(symbol)
                for symbol in manager.subscriptions.keys()
            },
            "timestamp": datetime.now().isoformat()
        }, connection_id)

async def start_data_streaming():
    """Start background data streaming task"""
    if not manager.is_streaming:
        manager.is_streaming = True
        manager.streaming_task = asyncio.create_task(data_streaming_loop())
        logger.info("Data streaming started")

async def stop_data_streaming():
    """Stop background data streaming task"""
    if manager.streaming_task:
        manager.streaming_task.cancel()
        manager.is_streaming = False
        logger.info("Data streaming stopped")

async def data_streaming_loop():
    """Main data streaming loop"""
    
    # Common symbols to stream
    default_symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
    
    while manager.is_streaming:
        try:
            # Get all subscribed symbols
            subscribed_symbols = set(manager.subscriptions.keys())
            
            # Always include default symbols if there are active connections
            if manager.active_connections:
                symbols_to_update = subscribed_symbols.union(default_symbols)
            else:
                symbols_to_update = subscribed_symbols
            
            if symbols_to_update:
                await fetch_and_broadcast_data(list(symbols_to_update))
            
            # Wait before next update
            await asyncio.sleep(5)  # Update every 5 seconds
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in data streaming loop: {e}")
            await asyncio.sleep(1)  # Brief pause before retrying

async def fetch_and_broadcast_data(symbols: List[str]):
    """Fetch market data and broadcast to subscribers"""
    
    for symbol in symbols:
        try:
            # Fetch current market data
            data = await fetch_real_time_data(symbol)
            
            if data:
                # Broadcast to symbol subscribers
                await manager.broadcast_to_subscribers(symbol, {
                    "type": "market_data",
                    "symbol": symbol,
                    "data": data,
                    "timestamp": datetime.now().isoformat()
                })
                
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")

async def fetch_real_time_data(symbol: str) -> Optional[dict]:
    """Fetch real-time market data for a symbol"""
    
    try:
        # In production, this would connect to a real-time data feed
        # For demo purposes, we'll use yfinance with some mock real-time elements
        
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1d", interval="1m")
        
        if hist.empty:
            return None
        
        latest = hist.iloc[-1]
        
        # Add some mock real-time variation
        price_variation = np.random.normal(0, 0.001)  # Small random variation
        current_price = latest['Close'] * (1 + price_variation)
        
        # Mock volume spike occasionally
        volume_multiplier = np.random.choice([1, 1, 1, 1, 2], p=[0.7, 0.1, 0.1, 0.05, 0.05])
        current_volume = int(latest['Volume'] * volume_multiplier)
        
        return {
            "open": float(latest['Open']),
            "high": max(float(latest['High']), current_price),
            "low": min(float(latest['Low']), current_price),
            "close": current_price,
            "volume": current_volume,
            "change": current_price - float(latest['Open']),
            "change_percent": ((current_price - float(latest['Open'])) / float(latest['Open'])) * 100,
            "last_update": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error fetching real-time data for {symbol}: {e}")
        return None

@router.websocket("/system-metrics")
async def websocket_system_metrics(websocket: WebSocket):
    """WebSocket endpoint for real-time system metrics"""
    
    connection_id = f"sys_{datetime.now().timestamp()}"
    
    try:
        await manager.connect(websocket, connection_id)
        
        # Send system metrics every 10 seconds
        while True:
            metrics = await get_system_metrics()
            
            await manager.send_personal_message({
                "type": "system_metrics",
                "data": metrics,
                "timestamp": datetime.now().isoformat()
            }, connection_id)
            
            await asyncio.sleep(10)
            
    except WebSocketDisconnect:
        manager.disconnect(connection_id)
    except Exception as e:
        logger.error(f"System metrics WebSocket error: {e}")
        manager.disconnect(connection_id)

async def get_system_metrics() -> dict:
    """Get current system metrics"""
    
    # Mock system metrics - in production, would gather real metrics
    return {
        "cpu_usage": np.random.uniform(20, 80),
        "memory_usage": np.random.uniform(40, 90),
        "disk_usage": np.random.uniform(60, 85),
        "network_in": np.random.uniform(10, 100),  # MB/s
        "network_out": np.random.uniform(5, 50),   # MB/s
        "active_connections": manager.get_connection_count(),
        "response_time": np.random.uniform(10, 100),  # ms
        "error_rate": np.random.uniform(0, 5),  # %
        "uptime": 86400,  # seconds
        "timestamp": datetime.now().isoformat()
    }

@router.get("/websocket/status")
async def get_websocket_status():
    """Get WebSocket server status"""
    
    return {
        "active_connections": manager.get_connection_count(),
        "subscriptions": {
            symbol: manager.get_subscription_count(symbol)
            for symbol in manager.subscriptions.keys()
        },
        "is_streaming": manager.is_streaming,
        "uptime": "24h 15m",  # Mock uptime
        "timestamp": datetime.now().isoformat()
    }