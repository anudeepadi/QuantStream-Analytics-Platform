"""
Pydantic Models for API Request/Response Schemas

This module defines all the data models used by the REST API for request validation
and response serialization.
"""

from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field, validator
import numpy as np


class PredictionRequest(BaseModel):
    """Request model for single prediction."""
    
    features: List[List[float]] = Field(
        ...,
        description="Input features as a 2D array (samples x features)",
        example=[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]
    )
    model_name: Optional[str] = Field(
        None,
        description="Name of the model to use for prediction (uses default if not specified)",
        example="isolation_forest_v1"
    )
    request_id: Optional[str] = Field(
        None,
        description="Optional request identifier for tracking",
        example="req_12345"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="Optional metadata to include with the request",
        example={"source": "api", "user_id": "user123"}
    )
    
    @validator('features')
    def validate_features(cls, v):
        """Validate features array."""
        if not v:
            raise ValueError("Features cannot be empty")
        
        # Check if all rows have same length
        if len(set(len(row) for row in v)) > 1:
            raise ValueError("All feature rows must have the same length")
        
        # Check for invalid values
        for row in v:
            for val in row:
                if not isinstance(val, (int, float)) or np.isnan(val) or np.isinf(val):
                    raise ValueError("Features must contain valid numeric values")
        
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "features": [[1.5, -0.5, 2.1], [0.8, 1.2, -1.0]],
                "model_name": "ensemble_detector",
                "request_id": "req_001",
                "metadata": {"timestamp": "2024-01-01T00:00:00Z"}
            }
        }


class BatchPredictionRequest(BaseModel):
    """Request model for batch prediction."""
    
    features_batch: List[List[List[float]]] = Field(
        ...,
        description="Batch of input features as a 3D array (batch_size x samples x features)",
        example=[[[1.0, 2.0]], [[3.0, 4.0]]]
    )
    model_name: Optional[str] = Field(
        None,
        description="Name of the model to use for prediction",
        example="statistical_detector"
    )
    request_ids: Optional[List[str]] = Field(
        None,
        description="Optional request identifiers for tracking",
        example=["req_1", "req_2"]
    )
    metadata_batch: Optional[List[Dict[str, Any]]] = Field(
        None,
        description="Optional metadata for each request in the batch"
    )
    
    @validator('features_batch')
    def validate_features_batch(cls, v):
        """Validate batch features array."""
        if not v:
            raise ValueError("Features batch cannot be empty")
        
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "features_batch": [
                    [[1.5, -0.5, 2.1]],
                    [[0.8, 1.2, -1.0]],
                    [[2.0, 0.0, 1.5]]
                ],
                "model_name": "ensemble_detector",
                "request_ids": ["batch_req_001", "batch_req_002", "batch_req_003"]
            }
        }


class PredictionResponse(BaseModel):
    """Response model for predictions."""
    
    request_id: str = Field(
        ...,
        description="Request identifier",
        example="req_12345"
    )
    prediction: int = Field(
        ...,
        description="Anomaly prediction (0 = normal, 1 = anomaly)",
        example=1
    )
    anomaly_score: float = Field(
        ...,
        ge=0.0,
        description="Anomaly score (higher values indicate higher likelihood of anomaly)",
        example=0.85
    )
    confidence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Confidence score for the prediction",
        example=0.92
    )
    timestamp: datetime = Field(
        ...,
        description="Prediction timestamp",
        example="2024-01-01T12:00:00Z"
    )
    latency_ms: float = Field(
        ...,
        ge=0.0,
        description="Prediction latency in milliseconds",
        example=15.5
    )
    model_name: str = Field(
        ...,
        description="Name of the model used for prediction",
        example="isolation_forest_v1"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="Optional metadata included with the response"
    )
    error: Optional[str] = Field(
        None,
        description="Error message if prediction failed"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "request_id": "req_001",
                "prediction": 1,
                "anomaly_score": 0.85,
                "confidence": 0.92,
                "timestamp": "2024-01-01T12:00:00Z",
                "latency_ms": 15.5,
                "model_name": "ensemble_detector",
                "metadata": {"source": "api"}
            }
        }


class BatchPredictionResponse(BaseModel):
    """Response model for batch predictions."""
    
    predictions: List[PredictionResponse] = Field(
        ...,
        description="List of prediction responses"
    )
    batch_size: int = Field(
        ...,
        ge=1,
        description="Number of predictions in the batch",
        example=3
    )
    total_latency_ms: float = Field(
        ...,
        ge=0.0,
        description="Total batch processing latency in milliseconds",
        example=45.2
    )
    timestamp: datetime = Field(
        ...,
        description="Batch processing timestamp"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "predictions": [
                    {
                        "request_id": "batch_req_001",
                        "prediction": 0,
                        "anomaly_score": 0.25,
                        "confidence": 0.75,
                        "timestamp": "2024-01-01T12:00:00Z",
                        "latency_ms": 5.0,
                        "model_name": "ensemble_detector"
                    }
                ],
                "batch_size": 1,
                "total_latency_ms": 45.2,
                "timestamp": "2024-01-01T12:00:00Z"
            }
        }


class HealthStatus(str, Enum):
    """Health status enumeration."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"  
    UNHEALTHY = "unhealthy"


class HealthResponse(BaseModel):
    """Health check response model."""
    
    status: HealthStatus = Field(
        ...,
        description="Overall health status",
        example=HealthStatus.HEALTHY
    )
    timestamp: datetime = Field(
        ...,
        description="Health check timestamp"
    )
    version: str = Field(
        ...,
        description="API version",
        example="1.0.0"
    )
    issues: List[str] = Field(
        default_factory=list,
        description="List of health issues if any",
        example=["High queue size: 850"]
    )
    performance_stats: Dict[str, Any] = Field(
        default_factory=dict,
        description="Performance statistics"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "status": "healthy",
                "timestamp": "2024-01-01T12:00:00Z",
                "version": "1.0.0",
                "issues": [],
                "performance_stats": {
                    "requests_per_minute": 120,
                    "avg_latency_ms": 25.5,
                    "error_rate": 0.01,
                    "queue_size": 15
                }
            }
        }


class ModelInfo(BaseModel):
    """Model information response."""
    
    name: str = Field(
        ...,
        description="Model name",
        example="isolation_forest_v1"
    )
    type: str = Field(
        ...,
        description="Model type",
        example="unsupervised"
    )
    version: str = Field(
        ...,
        description="Model version",
        example="1.0.0"
    )
    is_loaded: bool = Field(
        ...,
        description="Whether the model is currently loaded",
        example=True
    )
    is_default: bool = Field(
        ...,
        description="Whether this is the default model",
        example=False
    )
    created_at: Optional[datetime] = Field(
        None,
        description="Model creation timestamp"
    )
    performance_metrics: Optional[Dict[str, float]] = Field(
        None,
        description="Model performance metrics"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "name": "ensemble_detector",
                "type": "ensemble",
                "version": "1.2.0",
                "is_loaded": True,
                "is_default": True,
                "created_at": "2024-01-01T10:00:00Z",
                "performance_metrics": {
                    "f1_score": 0.92,
                    "precision": 0.89,
                    "recall": 0.95
                }
            }
        }


class ModelListResponse(BaseModel):
    """Response model for listing available models."""
    
    models: List[ModelInfo] = Field(
        ...,
        description="List of available models"
    )
    total_count: int = Field(
        ...,
        ge=0,
        description="Total number of models",
        example=3
    )
    default_model: Optional[str] = Field(
        None,
        description="Name of the default model",
        example="ensemble_detector"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "models": [
                    {
                        "name": "isolation_forest_v1",
                        "type": "unsupervised", 
                        "version": "1.0.0",
                        "is_loaded": True,
                        "is_default": False
                    }
                ],
                "total_count": 1,
                "default_model": "isolation_forest_v1"
            }
        }


class ModelLoadRequest(BaseModel):
    """Request model for loading a model."""
    
    model_path: str = Field(
        ...,
        description="Path to the model file or MLflow model URI",
        example="/models/isolation_forest_v2.pkl"
    )
    model_name: Optional[str] = Field(
        None,
        description="Optional name for the model (derived from path if not provided)",
        example="isolation_forest_v2"
    )
    set_as_default: bool = Field(
        False,
        description="Whether to set this model as the default",
        example=False
    )
    
    class Config:
        schema_extra = {
            "example": {
                "model_path": "models:/ensemble_detector/production",
                "model_name": "ensemble_detector_prod",
                "set_as_default": True
            }
        }


class ModelLoadResponse(BaseModel):
    """Response model for model loading."""
    
    model_name: str = Field(
        ...,
        description="Loaded model name",
        example="isolation_forest_v2"
    )
    model_path: str = Field(
        ...,
        description="Model source path",
        example="/models/isolation_forest_v2.pkl"
    )
    is_default: bool = Field(
        ...,
        description="Whether this model is now the default",
        example=True
    )
    loaded_at: datetime = Field(
        ...,
        description="Model loading timestamp"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "model_name": "ensemble_detector_prod",
                "model_path": "models:/ensemble_detector/production", 
                "is_default": True,
                "loaded_at": "2024-01-01T12:00:00Z"
            }
        }


class PerformanceStats(BaseModel):
    """Performance statistics model."""
    
    requests_per_minute: float = Field(
        ...,
        ge=0.0,
        description="Requests processed per minute"
    )
    avg_latency_ms: float = Field(
        ...,
        ge=0.0,
        description="Average prediction latency in milliseconds"
    )
    p95_latency_ms: float = Field(
        ...,
        ge=0.0,
        description="95th percentile latency in milliseconds"
    )
    p99_latency_ms: float = Field(
        ...,
        ge=0.0,
        description="99th percentile latency in milliseconds"
    )
    error_rate: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Error rate (0-1)"
    )
    cache_hit_rate: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Cache hit rate (0-1)"
    )
    queue_size: int = Field(
        ...,
        ge=0,
        description="Current request queue size"
    )
    total_requests: int = Field(
        ...,
        ge=0,
        description="Total requests processed"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "requests_per_minute": 150.5,
                "avg_latency_ms": 22.1,
                "p95_latency_ms": 45.2,
                "p99_latency_ms": 78.9,
                "error_rate": 0.005,
                "cache_hit_rate": 0.85,
                "queue_size": 12,
                "total_requests": 10485
            }
        }


class ErrorResponse(BaseModel):
    """Error response model."""
    
    error: str = Field(
        ...,
        description="Error type or category",
        example="ValidationError"
    )
    message: str = Field(
        ...,
        description="Detailed error message",
        example="Invalid feature dimensions"
    )
    details: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional error details"
    )
    timestamp: datetime = Field(
        ...,
        description="Error timestamp"
    )
    request_id: Optional[str] = Field(
        None,
        description="Request ID if available"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "error": "ModelNotFoundError",
                "message": "Model 'non_existent_model' not found",
                "details": {"available_models": ["isolation_forest_v1", "ensemble_detector"]},
                "timestamp": "2024-01-01T12:00:00Z",
                "request_id": "req_12345"
            }
        }


class APIConfigResponse(BaseModel):
    """API configuration response."""
    
    version: str = Field(
        ...,
        description="API version",
        example="1.0.0"
    )
    inference_config: Dict[str, Any] = Field(
        ...,
        description="Current inference engine configuration"
    )
    rate_limits: Dict[str, int] = Field(
        ...,
        description="Rate limiting configuration"
    )
    authentication_enabled: bool = Field(
        ...,
        description="Whether authentication is enabled"
    )
    cache_enabled: bool = Field(
        ...,
        description="Whether caching is enabled"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "version": "1.0.0",
                "inference_config": {
                    "batch_size": 32,
                    "max_batch_wait_time": 1.0,
                    "num_workers": 4
                },
                "rate_limits": {
                    "requests_per_minute": 1000,
                    "burst_limit": 100
                },
                "authentication_enabled": True,
                "cache_enabled": True
            }
        }