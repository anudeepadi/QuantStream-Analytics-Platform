"""
REST API for Anomaly Detection Model Serving

This package provides FastAPI-based REST API endpoints for serving anomaly detection models
with authentication, rate limiting, health checks, and comprehensive OpenAPI documentation.
"""

from .main import app, create_app
from .models import *
from .endpoints import *

__all__ = ['app', 'create_app']