"""
Alerts API Endpoints

REST API endpoints for alert management and notifications.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import logging
import uuid

from ...models.alerts import (
    AlertResponse,
    Alert,
    AlertHistory,
    AlertRule,
    NotificationChannel
)

router = APIRouter()
logger = logging.getLogger(__name__)

_db_service = None

def set_services(db_svc):
    """Called from main.py lifespan to inject initialized DB service."""
    global _db_service
    _db_service = db_svc

# Mock alert data for demonstration
# Mock alert data for demonstration
MOCK_ACTIVE_ALERTS = [
    {
        "id": "ALT001",
        "type": "price",
        "symbol": "AAPL",
        "title": "Price Target Reached",
        "message": "AAPL has reached your target price of $180.00",
        "severity": "warning",
        "timestamp": datetime.now() - timedelta(minutes=5),
        "status": "active",
        "conditions": {"price": 180.00, "condition": "above"},
        "actions_taken": ["dashboard_notification"],
        "user_id": "demo_user"
    },
    {
        "id": "ALT002",
        "type": "volume",
        "symbol": "TSLA",
        "title": "Unusual Volume Activity",
        "message": "TSLA volume is 300% above average",
        "severity": "critical",
        "timestamp": datetime.now() - timedelta(minutes=12),
        "status": "active",
        "conditions": {"volume_multiplier": 3.0},
        "actions_taken": ["email_sent", "dashboard_notification"],
        "user_id": "demo_user"
    },
    {
        "id": "ALT003",
        "type": "technical",
        "symbol": "GOOGL",
        "title": "RSI Oversold Signal",
        "message": "GOOGL RSI has dropped below 30 (currently 28.5)",
        "severity": "info",
        "timestamp": datetime.now() - timedelta(minutes=8),
        "status": "active",
        "conditions": {"rsi": 28.5, "threshold": 30},
        "actions_taken": ["dashboard_notification"],
        "user_id": "demo_user"
    },
    {
        "id": "ALT004",
        "type": "anomaly",
        "symbol": "MSFT",
        "title": "Price Anomaly Detected",
        "message": "MSFT price movement outside normal range",
        "severity": "critical",
        "timestamp": datetime.now() - timedelta(minutes=3),
        "status": "active",
        "conditions": {"z_score": 3.2, "threshold": 3.0},
        "actions_taken": ["alert_generated"],
        "user_id": "demo_user"
    }
]

@router.get("/")
async def get_alerts(
    user_id: str = "demo_user",
    status: Optional[str] = Query(None, description="Filter by status"),
    severity: Optional[str] = Query(None, description="Filter by severity"),
    alert_type: Optional[str] = Query(None, description="Filter by type"),
    limit: int = Query(50, ge=1, le=100)
):
    """Get user alerts with filtering"""
    
    try:
        alerts = []
        
        for alert_data in MOCK_ACTIVE_ALERTS:
            if alert_data["user_id"] != user_id:
                continue
            
            if status and alert_data["status"] != status:
                continue
            
            if severity and alert_data["severity"] != severity:
                continue
                
            if alert_type and alert_data["type"] != alert_type:
                continue
            
            alerts.append({
                "id": alert_data["id"],
                "type": alert_data["type"],
                "severity": alert_data["severity"],
                "status": alert_data["status"],
                "title": alert_data["title"],
                "message": alert_data["message"],
                "symbol": alert_data.get("symbol"),
                "created_at": alert_data["timestamp"].isoformat(),
                "triggered_at": alert_data["timestamp"].isoformat(),
            })
        
        return alerts[:limit]
        
    except Exception as e:
        logger.error(f"Error fetching alerts: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching alerts: {str(e)}")

        logger.error(f"Error fetching alerts: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching alerts: {str(e)}")

@router.get("/{alert_id}", response_model=Alert)
async def get_alert(alert_id: str, user_id: str = "demo_user"):
    """Get specific alert by ID"""
    
    try:
        for alert_data in MOCK_ACTIVE_ALERTS:
            if alert_data["id"] == alert_id and alert_data["user_id"] == user_id:
                return Alert(
                    id=alert_data["id"],
                    type=alert_data["type"],
                    symbol=alert_data.get("symbol"),
                    title=alert_data["title"],
                    message=alert_data["message"],
                    severity=alert_data["severity"],
                    status=alert_data["status"],
                    conditions=alert_data["conditions"],
                    actions_taken=alert_data["actions_taken"],
                    created_at=alert_data["timestamp"],
                    triggered_at=alert_data["timestamp"],
                    user_id=alert_data["user_id"]
                )
        
        raise HTTPException(status_code=404, detail="Alert not found")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching alert {alert_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching alert: {str(e)}")

@router.post("/", response_model=Alert)
async def create_alert(alert_data: Dict[str, Any], user_id: str = "demo_user"):
    """Create new alert rule"""
    
    try:
        alert_id = f"ALT_{uuid.uuid4().hex[:6].upper()}"
        
        new_alert = Alert(
            id=alert_id,
            type=alert_data["type"],
            symbol=alert_data.get("symbol"),
            title=alert_data["title"],
            message=alert_data.get("message", ""),
            severity=alert_data.get("severity", "medium"),
            status="active",
            conditions=alert_data.get("conditions", {}),
            actions_taken=[],
            created_at=datetime.now(),
            user_id=user_id
        )
        
        # In production, would save to database
        logger.info(f"Created alert {alert_id} for user {user_id}")
        
        return new_alert
        
    except Exception as e:
        logger.error(f"Error creating alert: {e}")
        raise HTTPException(status_code=500, detail=f"Error creating alert: {str(e)}")

@router.put("/{alert_id}", response_model=Alert)
async def update_alert(
    alert_id: str,
    alert_data: Dict[str, Any],
    user_id: str = "demo_user"
):
    """Update existing alert"""
    
    try:
        # Find existing alert
        existing_alert = None
        for alert in MOCK_ACTIVE_ALERTS:
            if alert["id"] == alert_id and alert["user_id"] == user_id:
                existing_alert = alert
                break
        
        if not existing_alert:
            raise HTTPException(status_code=404, detail="Alert not found")
        
        # Update fields
        existing_alert.update(alert_data)
        existing_alert["updated_at"] = datetime.now()
        
        updated_alert = Alert(
            id=existing_alert["id"],
            type=existing_alert["type"],
            symbol=existing_alert.get("symbol"),
            title=existing_alert["title"],
            message=existing_alert["message"],
            severity=existing_alert["severity"],
            status=existing_alert["status"],
            conditions=existing_alert["conditions"],
            actions_taken=existing_alert["actions_taken"],
            created_at=existing_alert["timestamp"],
            updated_at=existing_alert.get("updated_at"),
            user_id=existing_alert["user_id"]
        )
        
        return updated_alert
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating alert {alert_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error updating alert: {str(e)}")

@router.delete("/{alert_id}")
async def delete_alert(alert_id: str, user_id: str = "demo_user"):
    """Delete alert"""
    
    try:
        # Find and remove alert
        for i, alert in enumerate(MOCK_ACTIVE_ALERTS):
            if alert["id"] == alert_id and alert["user_id"] == user_id:
                MOCK_ACTIVE_ALERTS.pop(i)
                logger.info(f"Deleted alert {alert_id}")
                return {"message": "Alert deleted successfully"}
        
        raise HTTPException(status_code=404, detail="Alert not found")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting alert {alert_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error deleting alert: {str(e)}")

@router.post("/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: str, user_id: str = "demo_user"):
    """Acknowledge alert"""
    
    try:
        for alert in MOCK_ACTIVE_ALERTS:
            if alert["id"] == alert_id and alert["user_id"] == user_id:
                alert["status"] = "acknowledged"
                alert["acknowledged_at"] = datetime.now()
                
                logger.info(f"Alert {alert_id} acknowledged by user {user_id}")
                return {"message": "Alert acknowledged"}
        
        raise HTTPException(status_code=404, detail="Alert not found")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error acknowledging alert {alert_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error acknowledging alert: {str(e)}")

@router.get("/history/", response_model=List[AlertHistory])
async def get_alert_history(
    user_id: str = "demo_user",
    days: int = Query(7, ge=1, le=90),
    limit: int = Query(100, ge=1, le=1000)
):
    """Get alert history"""
    
    try:
        # Mock alert history
        history = []
        
        for i in range(min(limit, 50)):  # Generate mock history
            alert_id = f"HIST_{i:04d}"
            
            history_item = AlertHistory(
                id=alert_id,
                original_alert_id=f"ALT{i:03d}",
                type=["price", "volume", "technical", "anomaly"][i % 4],
                symbol=["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"][i % 5],
                title=f"Historical Alert {i}",
                message=f"Historical alert message {i}",
                severity=["low", "medium", "high"][i % 3],
                triggered_at=datetime.now() - timedelta(days=i % days, hours=i % 24),
                resolved_at=datetime.now() - timedelta(days=i % days, hours=(i % 24) - 1),
                resolution_time_minutes=(i % 120) + 5,
                actions_taken=["email_sent", "dashboard_notification"][:(i % 2) + 1],
                user_id=user_id
            )
            history.append(history_item)
        
        return sorted(history, key=lambda x: x.triggered_at, reverse=True)
        
    except Exception as e:
        logger.error(f"Error fetching alert history: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching history: {str(e)}")

@router.get("/statistics/")
async def get_alert_statistics(
    user_id: str = "demo_user",
    days: int = Query(30, ge=1, le=365)
):
    """Get alert statistics"""
    
    try:
        active = [a for a in MOCK_ACTIVE_ALERTS if a["user_id"] == user_id]
        critical_count = len([a for a in active if a["severity"] == "critical"])
        
        # Mock statistics
        return {
            "total_alerts": 127,
            "active_alerts": len(active),
            "triggered_today": 8,
            "critical_count": critical_count,
            "alerts_by_type": {
                "price": 45,
                "volume": 23,
                "technical": 38,
                "anomaly": 21
            },
            "alerts_by_severity": {
                "info": 32,
                "warning": 67,
                "critical": 28
            },
            "avg_resolution_time_minutes": 18.5,
            "false_positive_rate": 0.03,
            "most_triggered_symbols": [
                {"symbol": "TSLA", "count": 15},
                {"symbol": "AAPL", "count": 12},
                {"symbol": "GOOGL", "count": 10}
            ],
            "alert_frequency_by_hour": {
                str(hour): (hour % 8) + 2 for hour in range(24)
            },
            "period_days": days,
            "generated_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error generating alert statistics: {e}")
        raise HTTPException(status_code=500, detail=f"Error generating statistics: {str(e)}")

@router.post("/rules/", response_model=AlertRule)
async def create_alert_rule(rule_data: Dict[str, Any], user_id: str = "demo_user"):
    """Create new alert rule"""
    
    try:
        rule_id = f"RULE_{uuid.uuid4().hex[:6].upper()}"
        
        rule = AlertRule(
            id=rule_id,
            name=rule_data["name"],
            type=rule_data["type"],
            symbol=rule_data.get("symbol"),
            conditions=rule_data["conditions"],
            notification_channels=rule_data.get("notification_channels", ["dashboard"]),
            is_active=rule_data.get("is_active", True),
            created_at=datetime.now(),
            user_id=user_id
        )
        
        logger.info(f"Created alert rule {rule_id} for user {user_id}")
        return rule
        
    except Exception as e:
        logger.error(f"Error creating alert rule: {e}")
        raise HTTPException(status_code=500, detail=f"Error creating rule: {str(e)}")

@router.get("/rules/")
async def get_alert_rules(user_id: str = "demo_user"):
    """Get user alert rules"""
    
    try:
        rules = [
            {
                "id": "RULE_001",
                "name": "AAPL Price Alert",
                "type": "price",
                "condition": "Price above $180",
                "threshold": 180.00,
                "symbol": "AAPL",
                "enabled": True,
                "created_at": (datetime.now() - timedelta(days=5)).isoformat(),
            },
            {
                "id": "RULE_002",
                "name": "Volume Spike Detection",
                "type": "volume",
                "condition": "Volume above 2.5x average",
                "threshold": 2.5,
                "symbol": None,
                "enabled": True,
                "created_at": (datetime.now() - timedelta(days=10)).isoformat(),
            },
        ]
        
        return rules
        
    except Exception as e:
        logger.error(f"Error fetching alert rules: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching rules: {str(e)}")


@router.get("/channels/", response_model=List[NotificationChannel])
async def get_notification_channels(user_id: str = "demo_user"):
    """Get available notification channels"""
    
    try:
        channels = [
            NotificationChannel(
                id="email",
                name="Email",
                type="email",
                is_enabled=True,
                configuration={"address": "user@example.com"},
                last_used=datetime.now() - timedelta(hours=2)
            ),
            NotificationChannel(
                id="slack",
                name="Slack",
                type="webhook",
                is_enabled=False,
                configuration={"webhook_url": "", "channel": "#alerts"},
                last_used=None
            ),
            NotificationChannel(
                id="dashboard",
                name="Dashboard",
                type="internal",
                is_enabled=True,
                configuration={},
                last_used=datetime.now() - timedelta(minutes=5)
            )
        ]
        
        return channels
        
    except Exception as e:
        logger.error(f"Error fetching notification channels: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching channels: {str(e)}")

@router.post("/test-notification")
async def test_notification(
    channel_id: str,
    message: str = "Test notification from QuantStream Dashboard",
    user_id: str = "demo_user"
):
    """Test notification channel"""
    
    try:
        # Mock notification test
        if channel_id == "email":
            # Mock email test
            logger.info(f"Test email sent to user {user_id}")
        elif channel_id == "slack":
            # Mock Slack test
            logger.info(f"Test Slack message sent for user {user_id}")
        elif channel_id == "dashboard":
            # Mock dashboard notification
            logger.info(f"Test dashboard notification for user {user_id}")
        else:
            raise HTTPException(status_code=400, detail="Unknown notification channel")
        
        return {
            "success": True,
            "message": f"Test notification sent via {channel_id}",
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error testing notification: {e}")
        raise HTTPException(status_code=500, detail=f"Error testing notification: {str(e)}")

@router.post("/bulk-acknowledge")
async def bulk_acknowledge_alerts(
    alert_ids: List[str],
    user_id: str = "demo_user"
):
    """Acknowledge multiple alerts"""
    
    try:
        acknowledged = []
        not_found = []
        
        for alert_id in alert_ids:
            found = False
            for alert in MOCK_ACTIVE_ALERTS:
                if alert["id"] == alert_id and alert["user_id"] == user_id:
                    alert["status"] = "acknowledged"
                    alert["acknowledged_at"] = datetime.now()
                    acknowledged.append(alert_id)
                    found = True
                    break
            
            if not found:
                not_found.append(alert_id)
        
        return {
            "acknowledged": acknowledged,
            "not_found": not_found,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error bulk acknowledging alerts: {e}")
        raise HTTPException(status_code=500, detail=f"Error bulk acknowledging: {str(e)}")