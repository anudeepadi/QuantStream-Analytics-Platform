"""
Alert Models

Pydantic models for alert-related API requests and responses.
"""

from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum

class AlertType(str, Enum):
    """Alert types"""
    PRICE = "price"
    VOLUME = "volume"
    TECHNICAL = "technical"
    ANOMALY = "anomaly"
    PORTFOLIO = "portfolio"
    SYSTEM = "system"

class AlertSeverity(str, Enum):
    """Alert severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class AlertStatus(str, Enum):
    """Alert status"""
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    DISMISSED = "dismissed"

class NotificationType(str, Enum):
    """Notification channel types"""
    EMAIL = "email"
    SLACK = "slack"
    WEBHOOK = "webhook"
    SMS = "sms"
    INTERNAL = "internal"

class Alert(BaseModel):
    """Alert model"""
    id: str = Field(..., description="Alert ID")
    type: AlertType = Field(..., description="Alert type")
    symbol: Optional[str] = Field(None, description="Stock symbol (if applicable)")
    title: str = Field(..., description="Alert title")
    message: str = Field(..., description="Alert message")
    severity: AlertSeverity = Field(..., description="Alert severity")
    status: AlertStatus = Field(..., description="Alert status")
    conditions: Dict[str, Any] = Field(..., description="Alert conditions")
    actions_taken: List[str] = Field(default_factory=list, description="Actions taken")
    created_at: datetime = Field(..., description="Creation timestamp")
    triggered_at: Optional[datetime] = Field(None, description="Trigger timestamp")
    acknowledged_at: Optional[datetime] = Field(None, description="Acknowledgment timestamp")
    resolved_at: Optional[datetime] = Field(None, description="Resolution timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    user_id: str = Field(..., description="User ID")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class AlertHistory(BaseModel):
    """Alert history model"""
    id: str = Field(..., description="History entry ID")
    original_alert_id: str = Field(..., description="Original alert ID")
    type: AlertType = Field(..., description="Alert type")
    symbol: Optional[str] = Field(None, description="Stock symbol")
    title: str = Field(..., description="Alert title")
    message: str = Field(..., description="Alert message")
    severity: AlertSeverity = Field(..., description="Alert severity")
    triggered_at: datetime = Field(..., description="Trigger timestamp")
    resolved_at: Optional[datetime] = Field(None, description="Resolution timestamp")
    resolution_time_minutes: Optional[int] = Field(None, description="Resolution time in minutes")
    actions_taken: List[str] = Field(default_factory=list, description="Actions taken")
    user_id: str = Field(..., description="User ID")

class AlertRule(BaseModel):
    """Alert rule model"""
    id: str = Field(..., description="Rule ID")
    name: str = Field(..., description="Rule name")
    type: AlertType = Field(..., description="Alert type")
    symbol: Optional[str] = Field(None, description="Stock symbol (null for all symbols)")
    conditions: Dict[str, Any] = Field(..., description="Alert conditions")
    notification_channels: List[str] = Field(..., description="Notification channels")
    is_active: bool = Field(default=True, description="Rule active status")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    last_triggered: Optional[datetime] = Field(None, description="Last trigger timestamp")
    trigger_count: int = Field(default=0, description="Number of times triggered")
    user_id: str = Field(..., description="User ID")

class NotificationChannel(BaseModel):
    """Notification channel model"""
    id: str = Field(..., description="Channel ID")
    name: str = Field(..., description="Channel name")
    type: NotificationType = Field(..., description="Channel type")
    is_enabled: bool = Field(..., description="Channel enabled status")
    configuration: Dict[str, Any] = Field(..., description="Channel configuration")
    last_used: Optional[datetime] = Field(None, description="Last use timestamp")
    success_rate: Optional[float] = Field(None, description="Success rate (0-1)")

class AlertStatistics(BaseModel):
    """Alert statistics model"""
    total_alerts: int = Field(..., description="Total number of alerts")
    active_alerts: int = Field(..., description="Number of active alerts")
    alerts_by_type: Dict[str, int] = Field(..., description="Alert count by type")
    alerts_by_severity: Dict[str, int] = Field(..., description="Alert count by severity")
    avg_resolution_time_minutes: float = Field(..., description="Average resolution time")
    false_positive_rate: float = Field(..., description="False positive rate")
    most_triggered_symbols: List[Dict[str, Any]] = Field(..., description="Most triggered symbols")
    alert_frequency_by_hour: Dict[str, int] = Field(..., description="Alert frequency by hour")
    period_days: int = Field(..., description="Statistics period in days")
    generated_at: datetime = Field(..., description="Generation timestamp")

class CreateAlertRequest(BaseModel):
    """Create alert request"""
    type: AlertType = Field(..., description="Alert type")
    symbol: Optional[str] = Field(None, description="Stock symbol")
    title: str = Field(..., description="Alert title")
    message: Optional[str] = Field(None, description="Alert message")
    severity: AlertSeverity = Field(default=AlertSeverity.MEDIUM, description="Alert severity")
    conditions: Dict[str, Any] = Field(..., description="Alert conditions")
    notification_channels: List[str] = Field(default=["dashboard"], description="Notification channels")

class UpdateAlertRequest(BaseModel):
    """Update alert request"""
    title: Optional[str] = Field(None, description="Alert title")
    message: Optional[str] = Field(None, description="Alert message")
    severity: Optional[AlertSeverity] = Field(None, description="Alert severity")
    status: Optional[AlertStatus] = Field(None, description="Alert status")
    conditions: Optional[Dict[str, Any]] = Field(None, description="Alert conditions")

class CreateAlertRuleRequest(BaseModel):
    """Create alert rule request"""
    name: str = Field(..., description="Rule name")
    type: AlertType = Field(..., description="Alert type")
    symbol: Optional[str] = Field(None, description="Stock symbol")
    conditions: Dict[str, Any] = Field(..., description="Alert conditions")
    notification_channels: List[str] = Field(default=["dashboard"], description="Notification channels")
    is_active: bool = Field(default=True, description="Rule active status")

class PriceAlertConditions(BaseModel):
    """Price alert conditions"""
    price: float = Field(..., description="Target price")
    condition: str = Field(..., description="Condition (above, below, crosses)")

class VolumeAlertConditions(BaseModel):
    """Volume alert conditions"""
    volume_multiplier: float = Field(..., description="Volume multiplier vs average")
    condition: str = Field(default="above", description="Condition")

class TechnicalAlertConditions(BaseModel):
    """Technical indicator alert conditions"""
    indicator: str = Field(..., description="Technical indicator name")
    threshold: float = Field(..., description="Threshold value")
    condition: str = Field(..., description="Condition (above, below, crosses)")
    period: Optional[int] = Field(None, description="Indicator period")

class AnomalyAlertConditions(BaseModel):
    """Anomaly detection alert conditions"""
    z_score_threshold: float = Field(default=3.0, description="Z-score threshold")
    lookback_periods: int = Field(default=50, description="Lookback periods")

class PortfolioAlertConditions(BaseModel):
    """Portfolio alert conditions"""
    metric: str = Field(..., description="Portfolio metric")
    threshold: float = Field(..., description="Threshold value")
    condition: str = Field(..., description="Condition")

class BulkAcknowledgeRequest(BaseModel):
    """Bulk acknowledge request"""
    alert_ids: List[str] = Field(..., description="List of alert IDs")

class BulkAcknowledgeResponse(BaseModel):
    """Bulk acknowledge response"""
    acknowledged: List[str] = Field(..., description="Successfully acknowledged alerts")
    not_found: List[str] = Field(..., description="Alerts not found")
    timestamp: datetime = Field(..., description="Response timestamp")

class TestNotificationRequest(BaseModel):
    """Test notification request"""
    channel_id: str = Field(..., description="Notification channel ID")
    message: str = Field(default="Test notification", description="Test message")

class TestNotificationResponse(BaseModel):
    """Test notification response"""
    success: bool = Field(..., description="Success status")
    message: str = Field(..., description="Response message")
    timestamp: datetime = Field(..., description="Response timestamp")

class AlertResponse(BaseModel):
    """Alert API response wrapper"""
    data: Any = Field(..., description="Response data")
    count: Optional[int] = Field(None, description="Total count (for list responses)")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    source: str = Field(default="quantstream", description="Data source")