"""
Common Response Models

Shared Pydantic models for API responses.
"""

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Dict, Any, Optional, List
from enum import Enum

class StatusEnum(str, Enum):
    """System status enumeration"""
    HEALTHY = "healthy"
    WARNING = "warning"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"

class HealthResponse(BaseModel):
    """Health check response"""
    status: StatusEnum
    timestamp: datetime
    services: Dict[str, str] = Field(..., description="Service health status")
    version: str
    uptime: Optional[float] = Field(None, description="Uptime in seconds")

class ErrorResponse(BaseModel):
    """Error response model"""
    error: str = Field(..., description="Error message")
    error_code: Optional[str] = Field(None, description="Error code")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")
    timestamp: datetime = Field(default_factory=datetime.now)
    request_id: Optional[str] = Field(None, description="Request ID for tracking")

class SuccessResponse(BaseModel):
    """Generic success response"""
    message: str
    data: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.now)

class PaginatedResponse(BaseModel):
    """Paginated response wrapper"""
    data: List[Any]
    total: int = Field(..., description="Total number of items")
    page: int = Field(..., description="Current page number")
    page_size: int = Field(..., description="Items per page")
    has_next: bool = Field(..., description="Whether there are more pages")
    has_previous: bool = Field(..., description="Whether there are previous pages")

class MetricsResponse(BaseModel):
    """Metrics response"""
    metrics: Dict[str, float]
    timestamp: datetime
    source: str

class ConfigResponse(BaseModel):
    """Configuration response"""
    config: Dict[str, Any]
    environment: str
    version: str
    timestamp: datetime

class ValidationErrorDetail(BaseModel):
    """Validation error detail"""
    field: str
    message: str
    invalid_value: Any

class ValidationErrorResponse(BaseModel):
    """Validation error response"""
    error: str = "Validation error"
    details: List[ValidationErrorDetail]
    timestamp: datetime = Field(default_factory=datetime.now)