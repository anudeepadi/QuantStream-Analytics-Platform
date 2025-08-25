"""
Technical Indicators Integration Module

Bridges the existing technical indicators with the feature store system,
providing standardized feature computation functions.
"""

from .feature_indicators import (
    FeaturizedIndicators,
    create_sma_feature,
    create_ema_feature,
    create_rsi_feature,
    create_macd_features,
    create_bollinger_bands_features,
    create_stochastic_features,
    create_volume_features,
    create_advanced_features,
    register_all_technical_indicators
)

__all__ = [
    'FeaturizedIndicators',
    'create_sma_feature',
    'create_ema_feature', 
    'create_rsi_feature',
    'create_macd_features',
    'create_bollinger_bands_features',
    'create_stochastic_features',
    'create_volume_features',
    'create_advanced_features',
    'register_all_technical_indicators'
]