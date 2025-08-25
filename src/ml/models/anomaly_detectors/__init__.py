"""
Anomaly Detection Models for QuantStream Analytics Platform

This package contains implementations of various anomaly detection algorithms
including Isolation Forest, LSTM Autoencoder, Statistical methods, and Ensemble approaches.
"""

from .isolation_forest import IsolationForestDetector
from .statistical_detector import StatisticalAnomalyDetector, DetectionMethod
from .ensemble_detector import EnsembleAnomalyDetector

# Try to import LSTM Autoencoder (requires TensorFlow)
try:
    from .lstm_autoencoder import LSTMAutoencoderDetector
    __all__ = [
        'IsolationForestDetector',
        'StatisticalAnomalyDetector', 
        'DetectionMethod',
        'LSTMAutoencoderDetector',
        'EnsembleAnomalyDetector'
    ]
except ImportError:
    __all__ = [
        'IsolationForestDetector',
        'StatisticalAnomalyDetector',
        'DetectionMethod', 
        'EnsembleAnomalyDetector'
    ]