"""
WebSocket Manager Router

Router for WebSocket-related HTTP endpoints.
"""

from fastapi import APIRouter
from datetime import datetime
from ...websocket.websocket_manager import manager, start_data_streaming, stop_data_streaming

# Create router
router = APIRouter()

@router.get("/status")
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