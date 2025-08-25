"""
ML Models Package

This package contains all machine learning models for anomaly detection
in the QuantStream Analytics Platform.

Available models:
- BaseAnomalyDetector: Abstract base class for all anomaly detection models
- BaseEnsembleDetector: Abstract base class for ensemble models
- ModelMetadata: Container for model metadata and versioning
- IsolationForestModel: Isolation Forest for price movement anomalies (to be implemented)
- LSTMAutoencoderModel: LSTM autoencoder for time-series pattern detection (to be implemented)
- StatisticalModels: Z-score and IQR models for volume spike detection (to be implemented)
- EnsembleModel: Ensemble combining multiple detection approaches (to be implemented)
"""

from .base_model import BaseAnomalyDetector, BaseEnsembleDetector, ModelMetadata

__all__ = [
    'BaseAnomalyDetector',
    'BaseEnsembleDetector', 
    'ModelMetadata'
]