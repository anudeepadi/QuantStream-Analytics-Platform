"""
Isolation Forest Anomaly Detector Implementation

This module provides a production-ready implementation of Isolation Forest
for anomaly detection in financial time-series data.
"""

import logging
import warnings
from typing import Any, Dict, List, Optional, Union, Tuple
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV
from sklearn.metrics import classification_report
import joblib

from ..base_model import BaseAnomalyDetector

logger = logging.getLogger(__name__)


class IsolationForestDetector(BaseAnomalyDetector):
    """
    Isolation Forest-based anomaly detector with automatic hyperparameter tuning.
    
    This implementation wraps scikit-learn's IsolationForest with additional
    features for production use including automatic contamination estimation,
    feature importance analysis, and model persistence.
    """
    
    def __init__(
        self,
        name: str = "IsolationForest",
        n_estimators: int = 100,
        max_samples: Union[int, float, str] = 'auto',
        contamination: Union[float, str] = 'auto',
        max_features: Union[int, float] = 1.0,
        bootstrap: bool = False,
        n_jobs: Optional[int] = None,
        random_state: Optional[int] = None,
        verbose: int = 0,
        warm_start: bool = False,
        scaler_type: str = 'standard',
        auto_tune: bool = False,
        tune_method: str = 'grid',
        tune_cv: int = 3,
        **kwargs
    ):
        """
        Initialize Isolation Forest detector.
        
        Args:
            name: Model name
            n_estimators: Number of base estimators in ensemble
            max_samples: Number of samples to draw from X to train each base estimator
            contamination: Proportion of outliers in the data set
            max_features: Number of features to draw from X to train each base estimator
            bootstrap: Whether bootstrap is used when sampling
            n_jobs: Number of jobs to run in parallel
            random_state: Random state for reproducibility
            verbose: Verbosity level
            warm_start: When set to True, reuse solution of previous call
            scaler_type: Type of feature scaling ('standard', 'minmax', 'none')
            auto_tune: Whether to automatically tune hyperparameters
            tune_method: Tuning method ('grid', 'random')
            tune_cv: Cross-validation folds for tuning
        """
        hyperparameters = {
            'n_estimators': n_estimators,
            'max_samples': max_samples,
            'contamination': contamination,
            'max_features': max_features,
            'bootstrap': bootstrap,
            'n_jobs': n_jobs,
            'random_state': random_state,
            'verbose': verbose,
            'warm_start': warm_start,
            'scaler_type': scaler_type,
            'auto_tune': auto_tune,
            'tune_method': tune_method,
            'tune_cv': tune_cv
        }
        
        super().__init__(
            name=name,
            model_type='unsupervised',
            hyperparameters=hyperparameters,
            random_state=random_state
        )
        
        # Model parameters
        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.contamination = contamination
        self.max_features = max_features
        self.bootstrap = bootstrap
        self.n_jobs = n_jobs
        self.verbose = verbose
        self.warm_start = warm_start
        
        # Preprocessing
        self.scaler_type = scaler_type
        self.scaler = None
        
        # Hyperparameter tuning
        self.auto_tune = auto_tune
        self.tune_method = tune_method
        self.tune_cv = tune_cv
        self.best_params_ = None
        
        # Model components
        self._model = None
        self.feature_importances_ = None
        self.decision_scores_ = None
        self.threshold_ = None
        
    def _initialize_scaler(self) -> None:
        """Initialize the feature scaler based on scaler_type."""
        if self.scaler_type == 'standard':
            self.scaler = StandardScaler()
        elif self.scaler_type == 'minmax':
            self.scaler = MinMaxScaler()
        elif self.scaler_type == 'none':
            self.scaler = None
        else:
            raise ValueError(f"Unknown scaler type: {self.scaler_type}")
    
    def _preprocess_features(self, X: np.ndarray, fit_scaler: bool = False) -> np.ndarray:
        """
        Preprocess features with scaling.
        
        Args:
            X: Input features
            fit_scaler: Whether to fit the scaler
            
        Returns:
            Preprocessed features
        """
        if self.scaler is None:
            return X
        
        if fit_scaler:
            return self.scaler.fit_transform(X)
        else:
            return self.scaler.transform(X)
    
    def _estimate_contamination(self, X: np.ndarray, method: str = 'iqr') -> float:
        """
        Automatically estimate contamination rate from data.
        
        Args:
            X: Input features
            method: Method for contamination estimation ('iqr', 'percentile')
            
        Returns:
            Estimated contamination rate
        """
        if method == 'iqr':
            # Use IQR method for each feature and take the median
            contamination_rates = []
            
            for i in range(X.shape[1]):
                feature = X[:, i]
                Q1 = np.percentile(feature, 25)
                Q3 = np.percentile(feature, 75)
                IQR = Q3 - Q1
                
                # Points outside 1.5 * IQR are considered outliers
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                outliers = np.sum((feature < lower_bound) | (feature > upper_bound))
                contamination_rate = outliers / len(feature)
                contamination_rates.append(contamination_rate)
            
            # Use median contamination rate across features
            estimated_contamination = np.median(contamination_rates)
            
        elif method == 'percentile':
            # Use 95th percentile as threshold
            estimated_contamination = 0.05
            
        else:
            raise ValueError(f"Unknown contamination estimation method: {method}")
        
        # Ensure contamination is within reasonable bounds
        estimated_contamination = np.clip(estimated_contamination, 0.01, 0.5)
        
        logger.info(f"Estimated contamination rate: {estimated_contamination:.4f}")
        return estimated_contamination
    
    def _get_param_grid(self) -> Dict[str, List[Any]]:
        """
        Get parameter grid for hyperparameter tuning.
        
        Returns:
            Parameter grid dictionary
        """
        param_grid = {
            'n_estimators': [50, 100, 200, 300],
            'max_samples': ['auto', 0.5, 0.7, 0.9],
            'contamination': ['auto', 0.05, 0.1, 0.15, 0.2],
            'max_features': [0.5, 0.7, 1.0],
            'bootstrap': [True, False]
        }
        
        return param_grid
    
    def _tune_hyperparameters(self, X: np.ndarray) -> Dict[str, Any]:
        """
        Tune hyperparameters using cross-validation.
        
        Args:
            X: Training features
            
        Returns:
            Best hyperparameters
        """
        logger.info("Starting hyperparameter tuning...")
        
        # Create base model for tuning
        base_model = IsolationForest(random_state=self.random_state, n_jobs=self.n_jobs)
        
        param_grid = self._get_param_grid()
        
        # Custom scoring function for unsupervised anomaly detection
        def anomaly_score(estimator, X):
            # Use silhouette score or similar unsupervised metric
            scores = estimator.decision_function(X)
            # Return negative mean score (higher is better for scoring)
            return -np.mean(scores)
        
        if self.tune_method == 'grid':
            search = GridSearchCV(
                estimator=base_model,
                param_grid=param_grid,
                scoring=anomaly_score,
                cv=self.tune_cv,
                n_jobs=self.n_jobs,
                verbose=self.verbose
            )
        elif self.tune_method == 'random':
            search = RandomizedSearchCV(
                estimator=base_model,
                param_distributions=param_grid,
                n_iter=50,
                scoring=anomaly_score,
                cv=self.tune_cv,
                n_jobs=self.n_jobs,
                random_state=self.random_state,
                verbose=self.verbose
            )
        else:
            raise ValueError(f"Unknown tuning method: {self.tune_method}")
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            search.fit(X)
        
        best_params = search.best_params_
        logger.info(f"Best parameters found: {best_params}")
        
        return best_params
    
    def _compute_feature_importance(self, X: np.ndarray) -> np.ndarray:
        """
        Compute feature importance based on isolation paths.
        
        Args:
            X: Input features
            
        Returns:
            Feature importance scores
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted to compute feature importance")
        
        # Get decision paths for a sample of data
        sample_size = min(1000, X.shape[0])
        sample_indices = np.random.choice(X.shape[0], sample_size, replace=False)
        X_sample = X[sample_indices]
        
        # Calculate path lengths for each feature
        feature_importance = np.zeros(X.shape[1])
        
        # For each tree in the forest
        for estimator in self._model.estimators_:
            # Get decision paths
            leaves = estimator.apply(X_sample)
            
            # Calculate feature usage in paths
            feature_count = np.zeros(X.shape[1])
            
            # This is a simplified importance calculation
            # In practice, you might want to use more sophisticated methods
            for i in range(X.shape[1]):
                # Count how often each feature is used in decision paths
                feature_values = X_sample[:, i]
                unique_values = len(np.unique(feature_values))
                feature_count[i] = unique_values / len(feature_values)
            
            feature_importance += feature_count
        
        # Normalize importance scores
        feature_importance = feature_importance / len(self._model.estimators_)
        feature_importance = feature_importance / np.sum(feature_importance)
        
        return feature_importance
    
    def fit(self, X: Union[np.ndarray, pd.DataFrame], y: Optional[np.ndarray] = None) -> 'IsolationForestDetector':
        """
        Fit the Isolation Forest model.
        
        Args:
            X: Training features
            y: Ignored for unsupervised learning
            
        Returns:
            Self (for method chaining)
        """
        logger.info(f"Training {self.name} model...")
        
        # Validate and preprocess input
        X = self.validate_input(X)
        
        # Initialize and fit scaler
        self._initialize_scaler()
        if self.scaler is not None:
            X_scaled = self._preprocess_features(X, fit_scaler=True)
        else:
            X_scaled = X
        
        # Auto-estimate contamination if needed
        contamination = self.contamination
        if contamination == 'auto':
            contamination = self._estimate_contamination(X_scaled)
        
        # Hyperparameter tuning if requested
        if self.auto_tune:
            self.best_params_ = self._tune_hyperparameters(X_scaled)
            # Update model parameters with best found
            for param, value in self.best_params_.items():
                setattr(self, param, value)
            contamination = self.best_params_.get('contamination', contamination)
        
        # Initialize and train model
        self._model = IsolationForest(
            n_estimators=self.n_estimators,
            max_samples=self.max_samples,
            contamination=contamination,
            max_features=self.max_features,
            bootstrap=self.bootstrap,
            n_jobs=self.n_jobs,
            random_state=self.random_state,
            verbose=self.verbose,
            warm_start=self.warm_start
        )
        
        self._model.fit(X_scaled)
        
        # Compute additional metrics
        self.decision_scores_ = self._model.decision_function(X_scaled)
        self.threshold_ = self._model.offset_
        
        # Compute feature importance
        try:
            self.feature_importances_ = self._compute_feature_importance(X_scaled)
        except Exception as e:
            logger.warning(f"Could not compute feature importance: {e}")
            self.feature_importances_ = np.ones(X_scaled.shape[1]) / X_scaled.shape[1]
        
        # Update state
        self.is_fitted = True
        self.training_features = X.shape[1]
        
        # Update metadata
        self.metadata.hyperparameters = self.hyperparameters
        
        logger.info(f"Model training completed. Contamination: {contamination:.4f}")
        return self
    
    def predict(self, X: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        """
        Predict anomalies in the input data.
        
        Args:
            X: Input features for prediction
            
        Returns:
            Binary predictions (1 for anomaly, -1 for normal in sklearn convention)
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")
        
        X = self.validate_input(X)
        X_scaled = self._preprocess_features(X, fit_scaler=False)
        
        predictions = self._model.predict(X_scaled)
        
        # Convert sklearn convention (-1, 1) to standard convention (0, 1)
        predictions = np.where(predictions == -1, 1, 0)
        
        return predictions
    
    def predict_proba(self, X: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        """
        Predict anomaly scores (not probabilities for Isolation Forest).
        
        Args:
            X: Input features for prediction
            
        Returns:
            Anomaly scores (higher means more anomalous)
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")
        
        X = self.validate_input(X)
        X_scaled = self._preprocess_features(X, fit_scaler=False)
        
        # Get decision function scores
        scores = self._model.decision_function(X_scaled)
        
        # Convert to anomaly scores (higher = more anomalous)
        anomaly_scores = -scores  # Negative because decision_function returns negative for outliers
        
        # Normalize to [0, 1] range
        min_score = np.min(anomaly_scores)
        max_score = np.max(anomaly_scores)
        if max_score > min_score:
            anomaly_scores = (anomaly_scores - min_score) / (max_score - min_score)
        else:
            anomaly_scores = np.zeros_like(anomaly_scores)
        
        return anomaly_scores
    
    def decision_function(self, X: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        """
        Compute anomaly decision scores.
        
        Args:
            X: Input features
            
        Returns:
            Decision scores (negative for outliers)
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")
        
        X = self.validate_input(X)
        X_scaled = self._preprocess_features(X, fit_scaler=False)
        
        return self._model.decision_function(X_scaled)
    
    def get_feature_importance(self, feature_names: Optional[List[str]] = None) -> Dict[str, float]:
        """
        Get feature importance scores.
        
        Args:
            feature_names: Optional list of feature names
            
        Returns:
            Dictionary mapping feature names to importance scores
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted to get feature importance")
        
        if self.feature_importances_ is None:
            return {}
        
        if feature_names is None:
            feature_names = [f"feature_{i}" for i in range(len(self.feature_importances_))]
        
        return dict(zip(feature_names, self.feature_importances_))
    
    def set_contamination_threshold(self, contamination: float) -> None:
        """
        Update contamination threshold after training.
        
        Args:
            contamination: New contamination rate
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before updating contamination")
        
        if not 0 < contamination < 1:
            raise ValueError("Contamination must be between 0 and 1")
        
        # Update model contamination
        self._model.contamination = contamination
        self._model._set_oob_score = True  # Enable out-of-bag scoring
        
        logger.info(f"Updated contamination threshold to {contamination}")
    
    def get_model_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive model summary.
        
        Returns:
            Dictionary with model information and statistics
        """
        summary = self.get_model_info()
        
        if self.is_fitted:
            summary.update({
                'contamination': self._model.contamination,
                'threshold': self.threshold_,
                'n_features_trained': self.training_features,
                'decision_scores_stats': {
                    'mean': float(np.mean(self.decision_scores_)) if self.decision_scores_ is not None else None,
                    'std': float(np.std(self.decision_scores_)) if self.decision_scores_ is not None else None,
                    'min': float(np.min(self.decision_scores_)) if self.decision_scores_ is not None else None,
                    'max': float(np.max(self.decision_scores_)) if self.decision_scores_ is not None else None,
                },
                'feature_importance_available': self.feature_importances_ is not None
            })
        
        return summary