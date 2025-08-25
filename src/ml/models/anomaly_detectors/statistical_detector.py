"""
Statistical Anomaly Detector Implementation

This module provides various statistical methods for anomaly detection
including Z-score, IQR, seasonal decomposition, and moving average-based detection.
"""

import logging
import warnings
from typing import Any, Dict, List, Optional, Union, Tuple, Callable
from enum import Enum
import numpy as np
import pandas as pd
from scipy import stats
from scipy.signal import find_peaks
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.stattools import adfuller
import sklearn.preprocessing as preprocessing

from ..base_model import BaseAnomalyDetector

logger = logging.getLogger(__name__)


class DetectionMethod(Enum):
    """Enumeration of available statistical detection methods."""
    ZSCORE = "zscore"
    MODIFIED_ZSCORE = "modified_zscore"
    IQR = "iqr"
    SEASONAL_DECOMPOSITION = "seasonal_decomposition"
    MOVING_AVERAGE = "moving_average"
    MACD = "macd"
    BOLLINGER_BANDS = "bollinger_bands"
    HAMPEL_FILTER = "hampel_filter"


class StatisticalAnomalyDetector(BaseAnomalyDetector):
    """
    Statistical anomaly detector with multiple detection methods.
    
    This detector provides various statistical methods for anomaly detection
    that are particularly effective for time-series financial data.
    """
    
    def __init__(
        self,
        name: str = "StatisticalDetector",
        methods: List[Union[DetectionMethod, str]] = None,
        method_weights: Optional[Dict[str, float]] = None,
        zscore_threshold: float = 3.0,
        iqr_multiplier: float = 1.5,
        window_size: int = 30,
        seasonal_period: Optional[int] = None,
        macd_fast: int = 12,
        macd_slow: int = 26,
        macd_signal: int = 9,
        bollinger_window: int = 20,
        bollinger_std: float = 2.0,
        hampel_window: int = 5,
        hampel_threshold: float = 3.0,
        min_periods: int = 10,
        combine_method: str = 'vote',
        vote_threshold: float = 0.5,
        **kwargs
    ):
        """
        Initialize Statistical Anomaly Detector.
        
        Args:
            name: Model name
            methods: List of detection methods to use
            method_weights: Weights for combining methods
            zscore_threshold: Threshold for Z-score method
            iqr_multiplier: Multiplier for IQR method
            window_size: Rolling window size for moving average methods
            seasonal_period: Period for seasonal decomposition
            macd_fast: Fast period for MACD
            macd_slow: Slow period for MACD  
            macd_signal: Signal period for MACD
            bollinger_window: Window size for Bollinger Bands
            bollinger_std: Standard deviation multiplier for Bollinger Bands
            hampel_window: Window size for Hampel filter
            hampel_threshold: Threshold for Hampel filter
            min_periods: Minimum periods required for calculations
            combine_method: Method to combine results ('vote', 'weighted_average')
            vote_threshold: Threshold for voting combination
        """
        if methods is None:
            methods = [DetectionMethod.ZSCORE, DetectionMethod.IQR, DetectionMethod.MOVING_AVERAGE]
        
        # Convert string methods to enum
        self.methods = []
        for method in methods:
            if isinstance(method, str):
                self.methods.append(DetectionMethod(method))
            else:
                self.methods.append(method)
        
        hyperparameters = {
            'methods': [m.value for m in self.methods],
            'method_weights': method_weights,
            'zscore_threshold': zscore_threshold,
            'iqr_multiplier': iqr_multiplier,
            'window_size': window_size,
            'seasonal_period': seasonal_period,
            'macd_fast': macd_fast,
            'macd_slow': macd_slow,
            'macd_signal': macd_signal,
            'bollinger_window': bollinger_window,
            'bollinger_std': bollinger_std,
            'hampel_window': hampel_window,
            'hampel_threshold': hampel_threshold,
            'min_periods': min_periods,
            'combine_method': combine_method,
            'vote_threshold': vote_threshold
        }
        
        super().__init__(
            name=name,
            model_type='statistical',
            hyperparameters=hyperparameters,
            **kwargs
        )
        
        # Method parameters
        self.method_weights = method_weights or {}
        self.zscore_threshold = zscore_threshold
        self.iqr_multiplier = iqr_multiplier
        self.window_size = window_size
        self.seasonal_period = seasonal_period
        self.macd_fast = macd_fast
        self.macd_slow = macd_slow
        self.macd_signal = macd_signal
        self.bollinger_window = bollinger_window
        self.bollinger_std = bollinger_std
        self.hampel_window = hampel_window
        self.hampel_threshold = hampel_threshold
        self.min_periods = min_periods
        self.combine_method = combine_method
        self.vote_threshold = vote_threshold
        
        # Training statistics
        self.training_stats_ = {}
        self.method_detectors_ = {}
        
    def _detect_zscore(self, X: np.ndarray, fit: bool = False) -> Tuple[np.ndarray, np.ndarray]:
        """
        Z-score based anomaly detection.
        
        Args:
            X: Input data
            fit: Whether to compute training statistics
            
        Returns:
            Tuple of (predictions, scores)
        """
        predictions = np.zeros(X.shape[0])
        scores = np.zeros(X.shape[0])
        
        for i in range(X.shape[1]):
            feature = X[:, i]
            
            if fit:
                mean = np.mean(feature)
                std = np.std(feature)
                self.training_stats_[f'zscore_mean_{i}'] = mean
                self.training_stats_[f'zscore_std_{i}'] = std
            else:
                mean = self.training_stats_.get(f'zscore_mean_{i}', np.mean(feature))
                std = self.training_stats_.get(f'zscore_std_{i}', np.std(feature))
            
            if std > 0:
                z_scores = np.abs((feature - mean) / std)
                feature_predictions = (z_scores > self.zscore_threshold).astype(int)
                predictions = np.maximum(predictions, feature_predictions)
                scores = np.maximum(scores, z_scores / self.zscore_threshold)
        
        return predictions, scores
    
    def _detect_modified_zscore(self, X: np.ndarray, fit: bool = False) -> Tuple[np.ndarray, np.ndarray]:
        """
        Modified Z-score using median absolute deviation.
        
        Args:
            X: Input data
            fit: Whether to compute training statistics
            
        Returns:
            Tuple of (predictions, scores)
        """
        predictions = np.zeros(X.shape[0])
        scores = np.zeros(X.shape[0])
        
        for i in range(X.shape[1]):
            feature = X[:, i]
            
            if fit:
                median = np.median(feature)
                mad = np.median(np.abs(feature - median))
                self.training_stats_[f'modified_zscore_median_{i}'] = median
                self.training_stats_[f'modified_zscore_mad_{i}'] = mad
            else:
                median = self.training_stats_.get(f'modified_zscore_median_{i}', np.median(feature))
                mad = self.training_stats_.get(f'modified_zscore_mad_{i}', np.median(np.abs(feature - median)))
            
            if mad > 0:
                modified_z_scores = 0.6745 * (feature - median) / mad
                modified_z_scores = np.abs(modified_z_scores)
                feature_predictions = (modified_z_scores > self.zscore_threshold).astype(int)
                predictions = np.maximum(predictions, feature_predictions)
                scores = np.maximum(scores, modified_z_scores / self.zscore_threshold)
        
        return predictions, scores
    
    def _detect_iqr(self, X: np.ndarray, fit: bool = False) -> Tuple[np.ndarray, np.ndarray]:
        """
        IQR-based anomaly detection.
        
        Args:
            X: Input data
            fit: Whether to compute training statistics
            
        Returns:
            Tuple of (predictions, scores)
        """
        predictions = np.zeros(X.shape[0])
        scores = np.zeros(X.shape[0])
        
        for i in range(X.shape[1]):
            feature = X[:, i]
            
            if fit:
                Q1 = np.percentile(feature, 25)
                Q3 = np.percentile(feature, 75)
                self.training_stats_[f'iqr_q1_{i}'] = Q1
                self.training_stats_[f'iqr_q3_{i}'] = Q3
            else:
                Q1 = self.training_stats_.get(f'iqr_q1_{i}', np.percentile(feature, 25))
                Q3 = self.training_stats_.get(f'iqr_q3_{i}', np.percentile(feature, 75))
            
            IQR = Q3 - Q1
            if IQR > 0:
                lower_bound = Q1 - self.iqr_multiplier * IQR
                upper_bound = Q3 + self.iqr_multiplier * IQR
                
                outliers = (feature < lower_bound) | (feature > upper_bound)
                feature_predictions = outliers.astype(int)
                
                # Calculate scores based on distance from bounds
                lower_scores = np.where(feature < lower_bound, 
                                       (lower_bound - feature) / IQR, 0)
                upper_scores = np.where(feature > upper_bound,
                                       (feature - upper_bound) / IQR, 0)
                feature_scores = np.maximum(lower_scores, upper_scores)
                
                predictions = np.maximum(predictions, feature_predictions)
                scores = np.maximum(scores, feature_scores)
        
        return predictions, scores
    
    def _detect_seasonal_decomposition(self, X: np.ndarray, fit: bool = False) -> Tuple[np.ndarray, np.ndarray]:
        """
        Seasonal decomposition-based anomaly detection.
        
        Args:
            X: Input data (assumes time-series format)
            fit: Whether to compute training statistics
            
        Returns:
            Tuple of (predictions, scores)
        """
        if self.seasonal_period is None or self.seasonal_period >= len(X):
            logger.warning("Seasonal period not set or too large, skipping seasonal decomposition")
            return np.zeros(X.shape[0]), np.zeros(X.shape[0])
        
        predictions = np.zeros(X.shape[0])
        scores = np.zeros(X.shape[0])
        
        for i in range(X.shape[1]):
            feature = X[:, i]
            
            try:
                # Perform seasonal decomposition
                decomposition = seasonal_decompose(
                    feature, 
                    model='additive',
                    period=self.seasonal_period,
                    extrapolate_trend='freq'
                )
                
                residuals = decomposition.resid
                residuals = residuals[~np.isnan(residuals)]
                
                if len(residuals) > 0:
                    if fit:
                        threshold = np.std(residuals) * self.zscore_threshold
                        self.training_stats_[f'seasonal_threshold_{i}'] = threshold
                    else:
                        threshold = self.training_stats_.get(f'seasonal_threshold_{i}', 
                                                           np.std(residuals) * self.zscore_threshold)
                    
                    # Detect anomalies in residuals
                    residual_abs = np.abs(decomposition.resid)
                    feature_predictions = (residual_abs > threshold).astype(int)
                    feature_scores = residual_abs / threshold if threshold > 0 else residual_abs
                    
                    # Handle NaN values from decomposition
                    feature_predictions = np.nan_to_num(feature_predictions)
                    feature_scores = np.nan_to_num(feature_scores)
                    
                    predictions = np.maximum(predictions, feature_predictions)
                    scores = np.maximum(scores, feature_scores)
            
            except Exception as e:
                logger.warning(f"Seasonal decomposition failed for feature {i}: {e}")
                continue
        
        return predictions, scores
    
    def _detect_moving_average(self, X: np.ndarray, fit: bool = False) -> Tuple[np.ndarray, np.ndarray]:
        """
        Moving average-based anomaly detection.
        
        Args:
            X: Input data
            fit: Whether to compute training statistics
            
        Returns:
            Tuple of (predictions, scores)
        """
        predictions = np.zeros(X.shape[0])
        scores = np.zeros(X.shape[0])
        
        for i in range(X.shape[1]):
            feature = X[:, i]
            
            # Calculate rolling mean and std
            df_temp = pd.DataFrame({'value': feature})
            rolling_mean = df_temp['value'].rolling(
                window=self.window_size, 
                min_periods=self.min_periods
            ).mean()
            rolling_std = df_temp['value'].rolling(
                window=self.window_size, 
                min_periods=self.min_periods
            ).std()
            
            # Calculate deviations
            deviations = np.abs(feature - rolling_mean)
            
            if fit:
                # Use training data to set threshold
                valid_stds = rolling_std[~np.isnan(rolling_std)]
                if len(valid_stds) > 0:
                    threshold_multiplier = np.median(valid_stds) * self.zscore_threshold
                    self.training_stats_[f'moving_avg_threshold_{i}'] = threshold_multiplier
                else:
                    self.training_stats_[f'moving_avg_threshold_{i}'] = self.zscore_threshold
            
            threshold_multiplier = self.training_stats_.get(f'moving_avg_threshold_{i}', self.zscore_threshold)
            
            # Detect anomalies
            thresholds = rolling_std * threshold_multiplier
            feature_predictions = (deviations > thresholds).astype(int)
            feature_scores = deviations / np.where(thresholds > 0, thresholds, 1)
            
            # Handle NaN values
            feature_predictions = np.nan_to_num(feature_predictions)
            feature_scores = np.nan_to_num(feature_scores)
            
            predictions = np.maximum(predictions, feature_predictions)
            scores = np.maximum(scores, feature_scores)
        
        return predictions, scores
    
    def _detect_macd(self, X: np.ndarray, fit: bool = False) -> Tuple[np.ndarray, np.ndarray]:
        """
        MACD-based anomaly detection for time-series data.
        
        Args:
            X: Input data (assumes time-series format)
            fit: Whether to compute training statistics
            
        Returns:
            Tuple of (predictions, scores)
        """
        predictions = np.zeros(X.shape[0])
        scores = np.zeros(X.shape[0])
        
        for i in range(X.shape[1]):
            feature = X[:, i]
            
            # Calculate MACD
            df_temp = pd.DataFrame({'price': feature})
            
            # Exponential moving averages
            ema_fast = df_temp['price'].ewm(span=self.macd_fast).mean()
            ema_slow = df_temp['price'].ewm(span=self.macd_slow).mean()
            
            # MACD line and signal line
            macd_line = ema_fast - ema_slow
            signal_line = macd_line.ewm(span=self.macd_signal).mean()
            macd_histogram = macd_line - signal_line
            
            # Use MACD histogram for anomaly detection
            if fit:
                histogram_std = np.std(macd_histogram[~np.isnan(macd_histogram)])
                threshold = histogram_std * self.zscore_threshold
                self.training_stats_[f'macd_threshold_{i}'] = threshold
            else:
                threshold = self.training_stats_.get(f'macd_threshold_{i}', 
                                                   np.std(macd_histogram[~np.isnan(macd_histogram)]) * self.zscore_threshold)
            
            # Detect anomalies in MACD histogram
            histogram_abs = np.abs(macd_histogram)
            feature_predictions = (histogram_abs > threshold).astype(int)
            feature_scores = histogram_abs / threshold if threshold > 0 else histogram_abs
            
            # Handle NaN values
            feature_predictions = np.nan_to_num(feature_predictions)
            feature_scores = np.nan_to_num(feature_scores)
            
            predictions = np.maximum(predictions, feature_predictions)
            scores = np.maximum(scores, feature_scores)
        
        return predictions, scores
    
    def _detect_bollinger_bands(self, X: np.ndarray, fit: bool = False) -> Tuple[np.ndarray, np.ndarray]:
        """
        Bollinger Bands-based anomaly detection.
        
        Args:
            X: Input data
            fit: Whether to compute training statistics
            
        Returns:
            Tuple of (predictions, scores)
        """
        predictions = np.zeros(X.shape[0])
        scores = np.zeros(X.shape[0])
        
        for i in range(X.shape[1]):
            feature = X[:, i]
            
            # Calculate Bollinger Bands
            df_temp = pd.DataFrame({'price': feature})
            
            rolling_mean = df_temp['price'].rolling(
                window=self.bollinger_window,
                min_periods=self.min_periods
            ).mean()
            
            rolling_std = df_temp['price'].rolling(
                window=self.bollinger_window,
                min_periods=self.min_periods
            ).std()
            
            upper_band = rolling_mean + (rolling_std * self.bollinger_std)
            lower_band = rolling_mean - (rolling_std * self.bollinger_std)
            
            # Detect anomalies outside bands
            above_upper = feature > upper_band
            below_lower = feature < lower_band
            feature_predictions = (above_upper | below_lower).astype(int)
            
            # Calculate scores based on distance from bands
            upper_scores = np.where(above_upper, (feature - upper_band) / rolling_std, 0)
            lower_scores = np.where(below_lower, (lower_band - feature) / rolling_std, 0)
            feature_scores = np.maximum(upper_scores, lower_scores)
            
            # Handle NaN values
            feature_predictions = np.nan_to_num(feature_predictions)
            feature_scores = np.nan_to_num(feature_scores)
            
            predictions = np.maximum(predictions, feature_predictions)
            scores = np.maximum(scores, feature_scores)
        
        return predictions, scores
    
    def _detect_hampel_filter(self, X: np.ndarray, fit: bool = False) -> Tuple[np.ndarray, np.ndarray]:
        """
        Hampel filter-based anomaly detection.
        
        Args:
            X: Input data
            fit: Whether to compute training statistics
            
        Returns:
            Tuple of (predictions, scores)
        """
        predictions = np.zeros(X.shape[0])
        scores = np.zeros(X.shape[0])
        
        for i in range(X.shape[1]):
            feature = X[:, i]
            
            # Apply Hampel filter
            df_temp = pd.DataFrame({'value': feature})
            
            rolling_median = df_temp['value'].rolling(
                window=self.hampel_window,
                center=True,
                min_periods=1
            ).median()
            
            # Calculate median absolute deviation
            mad = df_temp['value'].rolling(
                window=self.hampel_window,
                center=True,
                min_periods=1
            ).apply(lambda x: np.median(np.abs(x - np.median(x))))
            
            # Detect outliers
            deviations = np.abs(feature - rolling_median)
            threshold = self.hampel_threshold * mad
            
            feature_predictions = (deviations > threshold).astype(int)
            feature_scores = deviations / np.where(threshold > 0, threshold, 1)
            
            # Handle NaN values
            feature_predictions = np.nan_to_num(feature_predictions)
            feature_scores = np.nan_to_num(feature_scores)
            
            predictions = np.maximum(predictions, feature_predictions)
            scores = np.maximum(scores, feature_scores)
        
        return predictions, scores
    
    def fit(self, X: Union[np.ndarray, pd.DataFrame], y: Optional[np.ndarray] = None) -> 'StatisticalAnomalyDetector':
        """
        Fit the statistical anomaly detector.
        
        Args:
            X: Training features
            y: Ignored for statistical methods
            
        Returns:
            Self (for method chaining)
        """
        logger.info(f"Training {self.name} with methods: {[m.value for m in self.methods]}")
        
        X = self.validate_input(X)
        
        # Initialize method detectors
        method_map = {
            DetectionMethod.ZSCORE: self._detect_zscore,
            DetectionMethod.MODIFIED_ZSCORE: self._detect_modified_zscore,
            DetectionMethod.IQR: self._detect_iqr,
            DetectionMethod.SEASONAL_DECOMPOSITION: self._detect_seasonal_decomposition,
            DetectionMethod.MOVING_AVERAGE: self._detect_moving_average,
            DetectionMethod.MACD: self._detect_macd,
            DetectionMethod.BOLLINGER_BANDS: self._detect_bollinger_bands,
            DetectionMethod.HAMPEL_FILTER: self._detect_hampel_filter
        }
        
        # Fit each method
        for method in self.methods:
            if method in method_map:
                detector_func = method_map[method]
                try:
                    _, _ = detector_func(X, fit=True)
                    self.method_detectors_[method] = detector_func
                    logger.info(f"Successfully fitted {method.value} method")
                except Exception as e:
                    logger.warning(f"Failed to fit {method.value} method: {e}")
        
        self.is_fitted = True
        self.training_features = X.shape[1]
        
        logger.info(f"Statistical detector training completed with {len(self.method_detectors_)} methods")
        return self
    
    def predict(self, X: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        """
        Predict anomalies using statistical methods.
        
        Args:
            X: Input features for prediction
            
        Returns:
            Binary predictions (1 for anomaly, 0 for normal)
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")
        
        X = self.validate_input(X)
        
        method_predictions = []
        method_names = []
        
        # Get predictions from each fitted method
        for method, detector_func in self.method_detectors_.items():
            try:
                predictions, _ = detector_func(X, fit=False)
                method_predictions.append(predictions)
                method_names.append(method.value)
            except Exception as e:
                logger.warning(f"Failed to predict with {method.value}: {e}")
        
        if not method_predictions:
            logger.warning("No methods available for prediction")
            return np.zeros(X.shape[0])
        
        # Combine predictions
        return self._combine_predictions(method_predictions, method_names)
    
    def predict_proba(self, X: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        """
        Predict anomaly scores using statistical methods.
        
        Args:
            X: Input features for prediction
            
        Returns:
            Anomaly scores
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")
        
        X = self.validate_input(X)
        
        method_scores = []
        method_names = []
        
        # Get scores from each fitted method
        for method, detector_func in self.method_detectors_.items():
            try:
                _, scores = detector_func(X, fit=False)
                method_scores.append(scores)
                method_names.append(method.value)
            except Exception as e:
                logger.warning(f"Failed to get scores with {method.value}: {e}")
        
        if not method_scores:
            logger.warning("No methods available for scoring")
            return np.zeros(X.shape[0])
        
        # Combine scores
        return self._combine_scores(method_scores, method_names)
    
    def _combine_predictions(self, method_predictions: List[np.ndarray], method_names: List[str]) -> np.ndarray:
        """
        Combine predictions from multiple methods.
        
        Args:
            method_predictions: List of prediction arrays
            method_names: List of method names
            
        Returns:
            Combined predictions
        """
        predictions_array = np.array(method_predictions)
        
        if self.combine_method == 'vote':
            # Majority voting
            combined = np.mean(predictions_array, axis=0)
            return (combined > self.vote_threshold).astype(int)
        
        elif self.combine_method == 'weighted_average':
            # Weighted average
            weights = np.array([self.method_weights.get(name, 1.0) for name in method_names])
            weights = weights / np.sum(weights)  # Normalize weights
            
            weighted_predictions = np.average(predictions_array, axis=0, weights=weights)
            return (weighted_predictions > self.vote_threshold).astype(int)
        
        else:
            raise ValueError(f"Unknown combine method: {self.combine_method}")
    
    def _combine_scores(self, method_scores: List[np.ndarray], method_names: List[str]) -> np.ndarray:
        """
        Combine scores from multiple methods.
        
        Args:
            method_scores: List of score arrays
            method_names: List of method names
            
        Returns:
            Combined scores
        """
        scores_array = np.array(method_scores)
        
        if self.combine_method == 'vote':
            # Average scores
            return np.mean(scores_array, axis=0)
        
        elif self.combine_method == 'weighted_average':
            # Weighted average of scores
            weights = np.array([self.method_weights.get(name, 1.0) for name in method_names])
            weights = weights / np.sum(weights)  # Normalize weights
            
            return np.average(scores_array, axis=0, weights=weights)
        
        else:
            raise ValueError(f"Unknown combine method: {self.combine_method}")
    
    def get_method_contributions(self, X: Union[np.ndarray, pd.DataFrame]) -> Dict[str, Dict[str, np.ndarray]]:
        """
        Get individual method contributions.
        
        Args:
            X: Input features
            
        Returns:
            Dictionary with method contributions
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before getting contributions")
        
        X = self.validate_input(X)
        contributions = {}
        
        for method, detector_func in self.method_detectors_.items():
            try:
                predictions, scores = detector_func(X, fit=False)
                contributions[method.value] = {
                    'predictions': predictions,
                    'scores': scores
                }
            except Exception as e:
                logger.warning(f"Failed to get contributions for {method.value}: {e}")
        
        return contributions
    
    def get_model_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive model summary.
        
        Returns:
            Dictionary with model information
        """
        summary = self.get_model_info()
        
        if self.is_fitted:
            summary.update({
                'fitted_methods': list(self.method_detectors_.keys()),
                'method_count': len(self.method_detectors_),
                'training_stats_keys': list(self.training_stats_.keys()),
                'combine_method': self.combine_method,
                'vote_threshold': self.vote_threshold
            })
        
        return summary