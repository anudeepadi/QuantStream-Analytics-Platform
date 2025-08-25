"""
Base Model Classes for QuantStream Analytics Platform

This module provides abstract base classes that define the interface
for all anomaly detection models in the system.
"""

import abc
import logging
import pickle
import json
from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator
from sklearn.metrics import precision_score, recall_score, f1_score, roc_auc_score


logger = logging.getLogger(__name__)


class ModelMetadata:
    """Container for model metadata and versioning information."""
    
    def __init__(
        self,
        name: str,
        version: str,
        model_type: str,
        creation_date: datetime,
        hyperparameters: Dict[str, Any],
        performance_metrics: Optional[Dict[str, float]] = None
    ):
        self.name = name
        self.version = version
        self.model_type = model_type
        self.creation_date = creation_date
        self.hyperparameters = hyperparameters
        self.performance_metrics = performance_metrics or {}
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary format."""
        return {
            'name': self.name,
            'version': self.version,
            'model_type': self.model_type,
            'creation_date': self.creation_date.isoformat(),
            'hyperparameters': self.hyperparameters,
            'performance_metrics': self.performance_metrics
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ModelMetadata':
        """Create metadata from dictionary."""
        return cls(
            name=data['name'],
            version=data['version'],
            model_type=data['model_type'],
            creation_date=datetime.fromisoformat(data['creation_date']),
            hyperparameters=data['hyperparameters'],
            performance_metrics=data.get('performance_metrics', {})
        )


class BaseAnomalyDetector(abc.ABC):
    """
    Abstract base class for all anomaly detection models.
    
    This class defines the standard interface that all anomaly detection
    models must implement, ensuring consistency across different algorithms.
    """
    
    def __init__(
        self,
        name: str,
        model_type: str,
        hyperparameters: Optional[Dict[str, Any]] = None,
        random_state: Optional[int] = None
    ):
        """
        Initialize the base anomaly detector.
        
        Args:
            name: Human-readable name for the model
            model_type: Type category (e.g., 'unsupervised', 'statistical')
            hyperparameters: Model hyperparameters
            random_state: Random state for reproducibility
        """
        self.name = name
        self.model_type = model_type
        self.hyperparameters = hyperparameters or {}
        self.random_state = random_state
        
        # Model state
        self.is_fitted = False
        self.training_features = None
        self._model = None
        
        # Metadata
        self.metadata = ModelMetadata(
            name=name,
            version="1.0.0",
            model_type=model_type,
            creation_date=datetime.now(),
            hyperparameters=self.hyperparameters
        )
        
        # Performance tracking
        self.training_history = []
        self.validation_scores = {}
        
    @abc.abstractmethod
    def fit(self, X: Union[np.ndarray, pd.DataFrame], y: Optional[np.ndarray] = None) -> 'BaseAnomalyDetector':
        """
        Train the anomaly detection model.
        
        Args:
            X: Training features
            y: Optional labels for supervised/semi-supervised models
            
        Returns:
            Self (for method chaining)
        """
        pass
    
    @abc.abstractmethod
    def predict(self, X: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        """
        Predict anomalies in the input data.
        
        Args:
            X: Input features for prediction
            
        Returns:
            Binary predictions (1 for anomaly, 0 for normal)
        """
        pass
    
    @abc.abstractmethod
    def predict_proba(self, X: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        """
        Predict anomaly probabilities/scores.
        
        Args:
            X: Input features for prediction
            
        Returns:
            Anomaly scores or probabilities
        """
        pass
    
    def decision_function(self, X: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        """
        Compute anomaly decision scores.
        
        Default implementation uses predict_proba, but can be overridden
        for models that have a specific decision function.
        
        Args:
            X: Input features
            
        Returns:
            Decision scores (higher means more anomalous)
        """
        return self.predict_proba(X)
    
    def validate_input(self, X: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        """
        Validate and prepare input data.
        
        Args:
            X: Input data to validate
            
        Returns:
            Validated numpy array
        """
        if isinstance(X, pd.DataFrame):
            X = X.values
        
        if not isinstance(X, np.ndarray):
            X = np.array(X)
        
        if X.ndim == 1:
            X = X.reshape(1, -1)
        
        # Check for NaN values
        if np.isnan(X).any():
            logger.warning("Input contains NaN values, this may affect model performance")
        
        # Check feature consistency during inference
        if self.is_fitted and self.training_features is not None:
            if X.shape[1] != self.training_features:
                raise ValueError(
                    f"Input has {X.shape[1]} features, but model was trained "
                    f"with {self.training_features} features"
                )
        
        return X
    
    def evaluate(
        self,
        X: Union[np.ndarray, pd.DataFrame],
        y_true: np.ndarray,
        metrics: Optional[List[str]] = None
    ) -> Dict[str, float]:
        """
        Evaluate model performance on labeled data.
        
        Args:
            X: Input features
            y_true: True anomaly labels (1 for anomaly, 0 for normal)
            metrics: List of metrics to compute
            
        Returns:
            Dictionary of metric scores
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before evaluation")
        
        if metrics is None:
            metrics = ['precision', 'recall', 'f1_score', 'roc_auc']
        
        y_pred = self.predict(X)
        y_scores = self.predict_proba(X)
        
        results = {}
        
        if 'precision' in metrics:
            results['precision'] = precision_score(y_true, y_pred)
        
        if 'recall' in metrics:
            results['recall'] = recall_score(y_true, y_pred)
        
        if 'f1_score' in metrics:
            results['f1_score'] = f1_score(y_true, y_pred)
        
        if 'roc_auc' in metrics:
            try:
                results['roc_auc'] = roc_auc_score(y_true, y_scores)
            except ValueError as e:
                logger.warning(f"Could not compute ROC AUC: {e}")
                results['roc_auc'] = np.nan
        
        # Update metadata with performance metrics
        self.metadata.performance_metrics.update(results)
        
        return results
    
    def save_model(self, filepath: Union[str, Path]) -> None:
        """
        Save the trained model to disk.
        
        Args:
            filepath: Path where to save the model
        """
        if not self.is_fitted:
            raise ValueError("Cannot save model that hasn't been fitted")
        
        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        model_data = {
            'model': self._model,
            'metadata': self.metadata.to_dict(),
            'hyperparameters': self.hyperparameters,
            'training_features': self.training_features,
            'validation_scores': self.validation_scores,
            'training_history': self.training_history
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
        
        logger.info(f"Model saved to {filepath}")
    
    @classmethod
    def load_model(cls, filepath: Union[str, Path]) -> 'BaseAnomalyDetector':
        """
        Load a trained model from disk.
        
        Args:
            filepath: Path to the saved model
            
        Returns:
            Loaded model instance
        """
        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)
        
        # Create instance
        metadata = ModelMetadata.from_dict(model_data['metadata'])
        instance = cls(
            name=metadata.name,
            model_type=metadata.model_type,
            hyperparameters=model_data['hyperparameters']
        )
        
        # Restore state
        instance._model = model_data['model']
        instance.metadata = metadata
        instance.training_features = model_data['training_features']
        instance.validation_scores = model_data['validation_scores']
        instance.training_history = model_data.get('training_history', [])
        instance.is_fitted = True
        
        logger.info(f"Model loaded from {filepath}")
        return instance
    
    def get_model_info(self) -> Dict[str, Any]:
        """
        Get comprehensive model information.
        
        Returns:
            Dictionary containing model metadata and status
        """
        return {
            'name': self.name,
            'model_type': self.model_type,
            'is_fitted': self.is_fitted,
            'training_features': self.training_features,
            'hyperparameters': self.hyperparameters,
            'metadata': self.metadata.to_dict(),
            'validation_scores': self.validation_scores
        }
    
    def __repr__(self) -> str:
        """String representation of the model."""
        status = "fitted" if self.is_fitted else "not fitted"
        return f"{self.__class__.__name__}(name='{self.name}', status='{status}')"


class BaseEnsembleDetector(BaseAnomalyDetector):
    """
    Abstract base class for ensemble anomaly detection models.
    
    This class extends BaseAnomalyDetector to support ensemble methods
    that combine multiple base models.
    """
    
    def __init__(
        self,
        name: str,
        base_models: List[BaseAnomalyDetector],
        aggregation_strategy: str = 'average',
        model_weights: Optional[Dict[str, float]] = None,
        **kwargs
    ):
        """
        Initialize the ensemble detector.
        
        Args:
            name: Name of the ensemble model
            base_models: List of base models to combine
            aggregation_strategy: How to aggregate predictions ('average', 'majority_vote', 'weighted')
            model_weights: Weights for weighted aggregation
            **kwargs: Additional arguments for base class
        """
        super().__init__(name=name, model_type='ensemble', **kwargs)
        
        self.base_models = base_models
        self.aggregation_strategy = aggregation_strategy
        self.model_weights = model_weights or {}
        
        # Validate inputs
        if not base_models:
            raise ValueError("Base models list cannot be empty")
        
        if aggregation_strategy == 'weighted' and not model_weights:
            raise ValueError("Model weights must be provided for weighted aggregation")
    
    def fit(self, X: Union[np.ndarray, pd.DataFrame], y: Optional[np.ndarray] = None) -> 'BaseEnsembleDetector':
        """
        Train all base models in the ensemble.
        
        Args:
            X: Training features
            y: Optional labels
            
        Returns:
            Self (for method chaining)
        """
        X = self.validate_input(X)
        
        logger.info(f"Training ensemble with {len(self.base_models)} base models")
        
        for i, model in enumerate(self.base_models):
            logger.info(f"Training base model {i+1}/{len(self.base_models)}: {model.name}")
            model.fit(X, y)
        
        self.is_fitted = True
        self.training_features = X.shape[1]
        
        logger.info("Ensemble training completed")
        return self
    
    def predict(self, X: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        """
        Predict anomalies using ensemble aggregation.
        
        Args:
            X: Input features
            
        Returns:
            Ensemble predictions
        """
        if not self.is_fitted:
            raise ValueError("Ensemble must be fitted before prediction")
        
        X = self.validate_input(X)
        
        # Get predictions from all base models
        predictions = np.array([model.predict(X) for model in self.base_models])
        
        return self._aggregate_predictions(predictions)
    
    def predict_proba(self, X: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        """
        Predict anomaly probabilities using ensemble aggregation.
        
        Args:
            X: Input features
            
        Returns:
            Ensemble probability scores
        """
        if not self.is_fitted:
            raise ValueError("Ensemble must be fitted before prediction")
        
        X = self.validate_input(X)
        
        # Get probability scores from all base models
        scores = np.array([model.predict_proba(X) for model in self.base_models])
        
        return self._aggregate_scores(scores)
    
    def _aggregate_predictions(self, predictions: np.ndarray) -> np.ndarray:
        """
        Aggregate binary predictions from base models.
        
        Args:
            predictions: Array of shape (n_models, n_samples)
            
        Returns:
            Aggregated predictions
        """
        if self.aggregation_strategy == 'majority_vote':
            return (predictions.mean(axis=0) > 0.5).astype(int)
        
        elif self.aggregation_strategy == 'average':
            return (predictions.mean(axis=0) > 0.5).astype(int)
        
        elif self.aggregation_strategy == 'weighted':
            weights = np.array([
                self.model_weights.get(model.name, 1.0) 
                for model in self.base_models
            ])
            weights = weights / weights.sum()  # Normalize weights
            weighted_preds = (predictions * weights[:, np.newaxis]).sum(axis=0)
            return (weighted_preds > 0.5).astype(int)
        
        else:
            raise ValueError(f"Unknown aggregation strategy: {self.aggregation_strategy}")
    
    def _aggregate_scores(self, scores: np.ndarray) -> np.ndarray:
        """
        Aggregate probability scores from base models.
        
        Args:
            scores: Array of shape (n_models, n_samples)
            
        Returns:
            Aggregated scores
        """
        if self.aggregation_strategy in ['majority_vote', 'average']:
            return scores.mean(axis=0)
        
        elif self.aggregation_strategy == 'weighted':
            weights = np.array([
                self.model_weights.get(model.name, 1.0) 
                for model in self.base_models
            ])
            weights = weights / weights.sum()  # Normalize weights
            return (scores * weights[:, np.newaxis]).sum(axis=0)
        
        else:
            raise ValueError(f"Unknown aggregation strategy: {self.aggregation_strategy}")
    
    def get_model_contributions(self, X: Union[np.ndarray, pd.DataFrame]) -> Dict[str, np.ndarray]:
        """
        Get individual model contributions to ensemble predictions.
        
        Args:
            X: Input features
            
        Returns:
            Dictionary mapping model names to their predictions
        """
        if not self.is_fitted:
            raise ValueError("Ensemble must be fitted before getting contributions")
        
        X = self.validate_input(X)
        
        contributions = {}
        for model in self.base_models:
            contributions[model.name] = {
                'predictions': model.predict(X),
                'scores': model.predict_proba(X)
            }
        
        return contributions