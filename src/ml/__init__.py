"""
QuantStream Analytics Platform - ML Package

This package provides comprehensive ML-based anomaly detection capabilities
for financial market data analysis.

Key Features:
- Multiple anomaly detection models (Isolation Forest, LSTM Autoencoder, Statistical)
- Real-time inference with sub-50ms latency
- MLflow integration for model lifecycle management
- Comprehensive monitoring and drift detection
- Feature engineering pipeline for financial data
- Automated training and retraining pipelines

Usage:
    from src.ml.models import BaseModel, IsolationForestModel
    from src.ml.features import FeatureEngineer
    from src.ml.inference import InferenceEngine
"""

__version__ = "1.0.0"
__author__ = "QuantStream Analytics Team"

# Import main components
from .models import BaseAnomalyDetector, BaseEnsembleDetector, ModelMetadata

__all__ = [
    'BaseAnomalyDetector',
    'BaseEnsembleDetector',
    'ModelMetadata',
    '__version__',
    '__author__'
]