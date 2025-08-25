"""
Feature Serving Module

Provides high-performance feature serving APIs with Redis caching
and sub-50ms latency requirements.
"""

from .feature_server import FeatureServer
from .api_models import (
    FeatureRequest,
    FeatureResponse,
    BatchFeatureRequest,
    BatchFeatureResponse,
    FeatureVector,
    ServingMetrics
)
from .endpoints import create_feature_serving_app

__all__ = [
    'FeatureServer',
    'FeatureRequest',
    'FeatureResponse',
    'BatchFeatureRequest',
    'BatchFeatureResponse',
    'FeatureVector',
    'ServingMetrics',
    'create_feature_serving_app'
]