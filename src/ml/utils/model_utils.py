"""
Model Utility Functions for QuantStream Analytics Platform

This module provides utility functions and helpers for ML model operations.
"""

import logging
import time
from typing import Any, Dict, List, Optional, Tuple, Union, Callable
from functools import wraps
import json

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split, TimeSeriesSplit
from sklearn.preprocessing import StandardScaler, RobustScaler, MinMaxScaler
from sklearn.metrics import classification_report, confusion_matrix

logger = logging.getLogger(__name__)


def timer(func: Callable) -> Callable:
    """Decorator to measure function execution time."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logger.info(f"{func.__name__} executed in {end_time - start_time:.4f} seconds")
        return result
    return wrapper


class ModelValidator:
    """Utility class for model validation and performance assessment."""
    
    @staticmethod
    def validate_data_format(X: Union[np.ndarray, pd.DataFrame], y: Optional[np.ndarray] = None) -> Tuple[np.ndarray, Optional[np.ndarray]]:
        """
        Validate and standardize data format.
        
        Args:
            X: Input features
            y: Optional target labels
            
        Returns:
            Validated (X, y) tuple
        """
        # Convert to numpy arrays
        if isinstance(X, pd.DataFrame):
            X = X.values
        if not isinstance(X, np.ndarray):
            X = np.array(X)
        
        if y is not None:
            if isinstance(y, pd.Series):
                y = y.values
            if not isinstance(y, np.ndarray):
                y = np.array(y)
        
        # Check dimensions
        if X.ndim == 1:
            X = X.reshape(1, -1)
        elif X.ndim != 2:
            raise ValueError(f"X must be 2D array, got shape {X.shape}")
        
        if y is not None:
            if y.ndim != 1 or len(y) != X.shape[0]:
                raise ValueError(f"y must be 1D array with length {X.shape[0]}, got shape {y.shape}")
        
        # Check for missing values
        if np.isnan(X).any():
            logger.warning("Input X contains NaN values")
        
        if y is not None and np.isnan(y).any():
            logger.warning("Target y contains NaN values")
        
        return X, y
    
    @staticmethod
    def check_data_quality(X: np.ndarray, min_samples: int = 100, max_nan_ratio: float = 0.1) -> Dict[str, Any]:
        """
        Check data quality and provide recommendations.
        
        Args:
            X: Input data
            min_samples: Minimum required samples
            max_nan_ratio: Maximum allowed NaN ratio
            
        Returns:
            Data quality report
        """
        n_samples, n_features = X.shape
        nan_ratio = np.isnan(X).sum() / (n_samples * n_features)
        
        quality_report = {
            'n_samples': n_samples,
            'n_features': n_features,
            'nan_ratio': nan_ratio,
            'warnings': [],
            'recommendations': []
        }
        
        # Check sample size
        if n_samples < min_samples:
            quality_report['warnings'].append(f"Low sample count: {n_samples} < {min_samples}")
            quality_report['recommendations'].append("Consider collecting more training data")
        
        # Check NaN ratio
        if nan_ratio > max_nan_ratio:
            quality_report['warnings'].append(f"High NaN ratio: {nan_ratio:.3f} > {max_nan_ratio}")
            quality_report['recommendations'].append("Consider data imputation or cleaning")
        
        # Check feature variance
        feature_vars = np.var(X, axis=0)
        zero_var_features = np.sum(feature_vars == 0)
        if zero_var_features > 0:
            quality_report['warnings'].append(f"{zero_var_features} features have zero variance")
            quality_report['recommendations'].append("Remove constant features")
        
        return quality_report


class DataSplitter:
    """Utility class for splitting time series data for ML training."""
    
    @staticmethod
    def time_series_split(
        X: Union[np.ndarray, pd.DataFrame],
        y: Optional[np.ndarray] = None,
        train_size: float = 0.7,
        validation_size: float = 0.15,
        test_size: float = 0.15,
        time_column: Optional[str] = None
    ) -> Dict[str, Union[np.ndarray, pd.DataFrame]]:
        """
        Split time series data maintaining temporal order.
        
        Args:
            X: Input features
            y: Optional target labels
            train_size: Proportion for training set
            validation_size: Proportion for validation set
            test_size: Proportion for test set
            time_column: Name of time column if X is DataFrame
            
        Returns:
            Dictionary containing train/validation/test splits
        """
        if not np.isclose(train_size + validation_size + test_size, 1.0):
            raise ValueError("Split sizes must sum to 1.0")
        
        n_samples = len(X)
        
        # Calculate split indices
        train_end = int(n_samples * train_size)
        val_end = int(n_samples * (train_size + validation_size))
        
        # Perform splits
        splits = {
            'X_train': X[:train_end],
            'X_val': X[train_end:val_end],
            'X_test': X[val_end:],
        }
        
        if y is not None:
            splits.update({
                'y_train': y[:train_end],
                'y_val': y[train_end:val_end],
                'y_test': y[val_end:],
            })
        
        logger.info(f"Data split: train={train_end}, val={val_end-train_end}, test={n_samples-val_end}")
        
        return splits
    
    @staticmethod
    def cross_validation_split(
        X: Union[np.ndarray, pd.DataFrame],
        y: Optional[np.ndarray] = None,
        n_splits: int = 5,
        test_size: float = 0.2
    ) -> List[Dict[str, Union[np.ndarray, pd.DataFrame]]]:
        """
        Create time series cross-validation splits.
        
        Args:
            X: Input features
            y: Optional target labels
            n_splits: Number of CV splits
            test_size: Size of test set in each split
            
        Returns:
            List of split dictionaries
        """
        tscv = TimeSeriesSplit(n_splits=n_splits, test_size=int(len(X) * test_size))
        
        splits = []
        for train_idx, test_idx in tscv.split(X):
            split = {
                'X_train': X[train_idx],
                'X_test': X[test_idx],
            }
            
            if y is not None:
                split.update({
                    'y_train': y[train_idx],
                    'y_test': y[test_idx],
                })
            
            splits.append(split)
        
        return splits


class FeatureScaler:
    """Utility class for feature scaling and normalization."""
    
    def __init__(self, method: str = 'robust'):
        """
        Initialize the feature scaler.
        
        Args:
            method: Scaling method ('standard', 'robust', 'minmax')
        """
        self.method = method
        self.scaler = self._create_scaler()
        self.is_fitted = False
    
    def _create_scaler(self):
        """Create the appropriate scaler instance."""
        if self.method == 'standard':
            return StandardScaler()
        elif self.method == 'robust':
            return RobustScaler()
        elif self.method == 'minmax':
            return MinMaxScaler()
        else:
            raise ValueError(f"Unknown scaling method: {self.method}")
    
    def fit(self, X: Union[np.ndarray, pd.DataFrame]) -> 'FeatureScaler':
        """
        Fit the scaler to training data.
        
        Args:
            X: Training data
            
        Returns:
            Self (for method chaining)
        """
        if isinstance(X, pd.DataFrame):
            X = X.values
        
        self.scaler.fit(X)
        self.is_fitted = True
        return self
    
    def transform(self, X: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        """
        Transform data using fitted scaler.
        
        Args:
            X: Data to transform
            
        Returns:
            Scaled data
        """
        if not self.is_fitted:
            raise ValueError("Scaler must be fitted before transform")
        
        if isinstance(X, pd.DataFrame):
            X = X.values
        
        return self.scaler.transform(X)
    
    def fit_transform(self, X: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        """
        Fit scaler and transform data in one step.
        
        Args:
            X: Training data
            
        Returns:
            Scaled data
        """
        return self.fit(X).transform(X)
    
    def inverse_transform(self, X: np.ndarray) -> np.ndarray:
        """
        Inverse transform scaled data back to original scale.
        
        Args:
            X: Scaled data
            
        Returns:
            Data in original scale
        """
        if not self.is_fitted:
            raise ValueError("Scaler must be fitted before inverse transform")
        
        return self.scaler.inverse_transform(X)


class PerformanceAnalyzer:
    """Utility class for analyzing model performance."""
    
    @staticmethod
    def generate_classification_report(
        y_true: np.ndarray,
        y_pred: np.ndarray,
        y_scores: Optional[np.ndarray] = None,
        target_names: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Generate comprehensive classification performance report.
        
        Args:
            y_true: True labels
            y_pred: Predicted labels
            y_scores: Prediction scores/probabilities
            target_names: Names of target classes
            
        Returns:
            Performance report dictionary
        """
        # Basic classification metrics
        report = classification_report(
            y_true, y_pred, 
            target_names=target_names, 
            output_dict=True
        )
        
        # Confusion matrix
        cm = confusion_matrix(y_true, y_pred)
        report['confusion_matrix'] = cm.tolist()
        
        # Additional anomaly detection metrics
        if len(np.unique(y_true)) == 2:  # Binary classification
            tn, fp, fn, tp = cm.ravel()
            
            # Anomaly detection specific metrics
            report['anomaly_metrics'] = {
                'true_positives': int(tp),
                'true_negatives': int(tn),
                'false_positives': int(fp),
                'false_negatives': int(fn),
                'false_positive_rate': fp / (fp + tn) if (fp + tn) > 0 else 0,
                'true_positive_rate': tp / (tp + fn) if (tp + fn) > 0 else 0,
                'detection_rate': tp / len(y_true),
                'miss_rate': fn / (tp + fn) if (tp + fn) > 0 else 0
            }
        
        return report
    
    @staticmethod
    def analyze_prediction_distribution(
        y_scores: np.ndarray,
        y_true: Optional[np.ndarray] = None,
        bins: int = 50
    ) -> Dict[str, Any]:
        """
        Analyze the distribution of prediction scores.
        
        Args:
            y_scores: Prediction scores
            y_true: Optional true labels
            bins: Number of histogram bins
            
        Returns:
            Distribution analysis results
        """
        analysis = {
            'score_statistics': {
                'mean': float(np.mean(y_scores)),
                'std': float(np.std(y_scores)),
                'min': float(np.min(y_scores)),
                'max': float(np.max(y_scores)),
                'median': float(np.median(y_scores)),
                'q25': float(np.percentile(y_scores, 25)),
                'q75': float(np.percentile(y_scores, 75))
            }
        }
        
        # Histogram
        hist, bin_edges = np.histogram(y_scores, bins=bins)
        analysis['histogram'] = {
            'counts': hist.tolist(),
            'bin_edges': bin_edges.tolist()
        }
        
        # Class-specific analysis if labels are available
        if y_true is not None:
            for class_label in np.unique(y_true):
                class_scores = y_scores[y_true == class_label]
                analysis[f'class_{class_label}_stats'] = {
                    'mean': float(np.mean(class_scores)),
                    'std': float(np.std(class_scores)),
                    'count': int(len(class_scores))
                }
        
        return analysis


def create_anomaly_threshold_analyzer(contamination_rates: List[float] = None) -> Callable:
    """
    Create a function to analyze optimal anomaly detection thresholds.
    
    Args:
        contamination_rates: List of contamination rates to test
        
    Returns:
        Threshold analysis function
    """
    if contamination_rates is None:
        contamination_rates = [0.01, 0.05, 0.1, 0.15, 0.2]
    
    def analyze_thresholds(
        y_scores: np.ndarray,
        y_true: Optional[np.ndarray] = None
    ) -> Dict[str, Any]:
        """
        Analyze different threshold values for anomaly detection.
        
        Args:
            y_scores: Anomaly scores
            y_true: True labels (if available)
            
        Returns:
            Threshold analysis results
        """
        results = {}
        
        for contamination in contamination_rates:
            threshold = np.percentile(y_scores, (1 - contamination) * 100)
            y_pred = (y_scores > threshold).astype(int)
            
            threshold_result = {
                'contamination_rate': contamination,
                'threshold': float(threshold),
                'predicted_anomalies': int(np.sum(y_pred)),
                'anomaly_percentage': float(np.mean(y_pred))
            }
            
            if y_true is not None:
                # Calculate performance metrics
                from sklearn.metrics import precision_score, recall_score, f1_score
                
                threshold_result.update({
                    'precision': float(precision_score(y_true, y_pred, zero_division=0)),
                    'recall': float(recall_score(y_true, y_pred, zero_division=0)),
                    'f1_score': float(f1_score(y_true, y_pred, zero_division=0))
                })
            
            results[f'contamination_{contamination}'] = threshold_result
        
        # Find best threshold if labels are available
        if y_true is not None:
            best_f1 = 0
            best_contamination = contamination_rates[0]
            
            for contamination in contamination_rates:
                f1 = results[f'contamination_{contamination}']['f1_score']
                if f1 > best_f1:
                    best_f1 = f1
                    best_contamination = contamination
            
            results['best_threshold'] = {
                'contamination_rate': best_contamination,
                'f1_score': best_f1,
                'threshold': results[f'contamination_{best_contamination}']['threshold']
            }
        
        return results
    
    return analyze_thresholds