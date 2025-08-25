"""
Ensemble Anomaly Detector Implementation

This module provides an ensemble approach that combines multiple anomaly detection
methods for improved accuracy and robustness.
"""

import logging
import warnings
from typing import Any, Dict, List, Optional, Union, Tuple, Callable
import numpy as np
import pandas as pd
from sklearn.ensemble import VotingClassifier
from sklearn.model_selection import cross_val_score
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

from ..base_model import BaseEnsembleDetector, BaseAnomalyDetector
from .isolation_forest import IsolationForestDetector
from .statistical_detector import StatisticalAnomalyDetector, DetectionMethod

# Try to import LSTM Autoencoder
try:
    from .lstm_autoencoder import LSTMAutoencoderDetector
    HAS_LSTM = True
except ImportError:
    HAS_LSTM = False
    logger = logging.getLogger(__name__)
    logger.warning("LSTM Autoencoder not available for ensemble")

logger = logging.getLogger(__name__)


class EnsembleAnomalyDetector(BaseEnsembleDetector):
    """
    Ensemble anomaly detector combining multiple detection methods.
    
    This detector combines Isolation Forest, Statistical methods, and optionally
    LSTM Autoencoder for robust anomaly detection with improved accuracy.
    """
    
    def __init__(
        self,
        name: str = "EnsembleDetector",
        use_isolation_forest: bool = True,
        use_statistical: bool = True,
        use_lstm_autoencoder: bool = True,
        isolation_forest_params: Optional[Dict[str, Any]] = None,
        statistical_params: Optional[Dict[str, Any]] = None,
        lstm_params: Optional[Dict[str, Any]] = None,
        aggregation_strategy: str = 'weighted',
        model_weights: Optional[Dict[str, float]] = None,
        auto_weight: bool = True,
        voting_threshold: float = 0.5,
        meta_learning: bool = False,
        dynamic_selection: bool = False,
        min_models: int = 2,
        **kwargs
    ):
        """
        Initialize Ensemble Anomaly Detector.
        
        Args:
            name: Model name
            use_isolation_forest: Whether to include Isolation Forest
            use_statistical: Whether to include Statistical methods
            use_lstm_autoencoder: Whether to include LSTM Autoencoder
            isolation_forest_params: Parameters for Isolation Forest
            statistical_params: Parameters for Statistical detector
            lstm_params: Parameters for LSTM Autoencoder
            aggregation_strategy: How to combine predictions
            model_weights: Manual weights for models
            auto_weight: Whether to automatically determine weights
            voting_threshold: Threshold for voting-based aggregation
            meta_learning: Whether to use meta-learning for combination
            dynamic_selection: Whether to dynamically select best models
            min_models: Minimum number of models required
        """
        # Initialize base models
        base_models = []
        
        # Isolation Forest
        if use_isolation_forest:
            if_params = isolation_forest_params or {}
            if_model = IsolationForestDetector(
                name="IsolationForest_Ensemble",
                **if_params
            )
            base_models.append(if_model)
        
        # Statistical Detector
        if use_statistical:
            stat_params = statistical_params or {
                'methods': [DetectionMethod.ZSCORE, DetectionMethod.IQR, DetectionMethod.MOVING_AVERAGE]
            }
            stat_model = StatisticalAnomalyDetector(
                name="Statistical_Ensemble",
                **stat_params
            )
            base_models.append(stat_model)
        
        # LSTM Autoencoder
        if use_lstm_autoencoder and HAS_LSTM:
            lstm_params = lstm_params or {}
            lstm_model = LSTMAutoencoderDetector(
                name="LSTM_Ensemble",
                **lstm_params
            )
            base_models.append(lstm_model)
        elif use_lstm_autoencoder and not HAS_LSTM:
            logger.warning("LSTM Autoencoder requested but not available")
        
        if len(base_models) < min_models:
            raise ValueError(f"At least {min_models} models required for ensemble, got {len(base_models)}")
        
        # Initialize model weights
        if model_weights is None:
            model_weights = {model.name: 1.0 for model in base_models}
        
        super().__init__(
            name=name,
            base_models=base_models,
            aggregation_strategy=aggregation_strategy,
            model_weights=model_weights,
            **kwargs
        )
        
        # Ensemble-specific parameters
        self.auto_weight = auto_weight
        self.voting_threshold = voting_threshold
        self.meta_learning = meta_learning
        self.dynamic_selection = dynamic_selection
        self.min_models = min_models
        
        # Model performance tracking
        self.model_performances_ = {}
        self.optimal_weights_ = {}
        self.selection_criteria_ = {}
        self.meta_model_ = None
        
    def _calculate_model_weights(self, X: np.ndarray, y: Optional[np.ndarray] = None) -> Dict[str, float]:
        """
        Automatically calculate optimal model weights based on performance.
        
        Args:
            X: Training data
            y: Optional labels for supervised weight calculation
            
        Returns:
            Dictionary of optimal weights
        """
        if not self.auto_weight:
            return self.model_weights
        
        logger.info("Calculating optimal model weights...")
        weights = {}
        
        if y is not None:
            # Supervised weight calculation using cross-validation
            for model in self.base_models:
                try:
                    # Wrap model for sklearn compatibility
                    class ModelWrapper:
                        def __init__(self, model):
                            self.model = model
                        
                        def fit(self, X, y=None):
                            self.model.fit(X, y)
                            return self
                        
                        def predict(self, X):
                            return self.model.predict(X)
                    
                    wrapper = ModelWrapper(model)
                    cv_scores = cross_val_score(wrapper, X, y, cv=3, scoring='f1')
                    weight = np.mean(cv_scores)
                    weights[model.name] = max(weight, 0.1)  # Minimum weight
                    
                    logger.info(f"{model.name} CV F1 score: {weight:.4f}")
                    
                except Exception as e:
                    logger.warning(f"Could not calculate weight for {model.name}: {e}")
                    weights[model.name] = 1.0
        
        else:
            # Unsupervised weight calculation using internal metrics
            for model in self.base_models:
                try:
                    model.fit(X)
                    
                    # Use model-specific metrics for weighting
                    if hasattr(model, 'get_model_summary'):
                        summary = model.get_model_summary()
                        
                        # Different weighting strategies for different model types
                        if model.model_type == 'unsupervised':
                            # For Isolation Forest, use score consistency
                            scores = model.predict_proba(X)
                            consistency = 1.0 / (np.std(scores) + 1e-6)
                            weight = min(consistency, 10.0)
                        
                        elif model.model_type == 'statistical':
                            # For statistical methods, use method coverage
                            weight = len(getattr(model, 'method_detectors_', {}))
                        
                        elif model.model_type == 'deep_learning':
                            # For LSTM, use reconstruction quality
                            if hasattr(model, 'reconstruction_errors_'):
                                errors = model.reconstruction_errors_
                                weight = 1.0 / (np.mean(errors) + 1e-6)
                            else:
                                weight = 1.0
                        
                        else:
                            weight = 1.0
                    
                    else:
                        weight = 1.0
                    
                    weights[model.name] = max(weight, 0.1)
                    logger.info(f"{model.name} calculated weight: {weight:.4f}")
                
                except Exception as e:
                    logger.warning(f"Could not calculate weight for {model.name}: {e}")
                    weights[model.name] = 1.0
        
        # Normalize weights
        total_weight = sum(weights.values())
        if total_weight > 0:
            weights = {k: v / total_weight for k, v in weights.items()}
        
        self.optimal_weights_ = weights
        logger.info(f"Optimal weights calculated: {weights}")
        
        return weights
    
    def _dynamic_model_selection(self, X: np.ndarray) -> List[BaseAnomalyDetector]:
        """
        Dynamically select best performing models for current data.
        
        Args:
            X: Input data
            
        Returns:
            List of selected models
        """
        if not self.dynamic_selection or not self.is_fitted:
            return self.base_models
        
        selected_models = []
        
        # Calculate data characteristics
        data_stats = {
            'variance': np.var(X, axis=0).mean(),
            'skewness': pd.DataFrame(X).skew().mean(),
            'kurtosis': pd.DataFrame(X).kurtosis().mean(),
            'trend': self._detect_trend(X),
            'seasonality': self._detect_seasonality(X)
        }
        
        # Select models based on data characteristics
        for model in self.base_models:
            should_select = True
            
            # Rules for model selection
            if model.model_type == 'deep_learning':
                # LSTM works better with trending data
                if data_stats['trend'] < 0.1:
                    should_select = False
            
            elif model.model_type == 'statistical':
                # Statistical methods work better with stationary data
                if data_stats['variance'] > 10.0:  # High variance
                    should_select = False
            
            elif model.model_type == 'unsupervised':
                # Isolation Forest works well in most cases
                pass
            
            if should_select:
                selected_models.append(model)
        
        # Ensure minimum number of models
        if len(selected_models) < self.min_models:
            selected_models = self.base_models[:self.min_models]
        
        logger.info(f"Dynamic selection: {len(selected_models)}/{len(self.base_models)} models selected")
        return selected_models
    
    def _detect_trend(self, X: np.ndarray) -> float:
        """Detect trend in data (simplified)."""
        try:
            # Use first feature for trend detection
            feature = X[:, 0]
            x = np.arange(len(feature))
            correlation = np.corrcoef(x, feature)[0, 1]
            return abs(correlation)
        except:
            return 0.0
    
    def _detect_seasonality(self, X: np.ndarray) -> float:
        """Detect seasonality in data (simplified)."""
        try:
            from scipy.fft import fft
            # Use FFT to detect periodic patterns
            feature = X[:, 0]
            fft_vals = np.abs(fft(feature))
            # Find strongest frequency component
            max_freq_power = np.max(fft_vals[1:len(fft_vals)//2])
            total_power = np.sum(fft_vals)
            return max_freq_power / total_power if total_power > 0 else 0.0
        except:
            return 0.0
    
    def _train_meta_model(self, X: np.ndarray, base_predictions: List[np.ndarray], y: Optional[np.ndarray] = None) -> None:
        """
        Train a meta-model to combine base model predictions.
        
        Args:
            X: Training data
            base_predictions: Predictions from base models
            y: Optional labels for supervised meta-learning
        """
        if not self.meta_learning:
            return
        
        logger.info("Training meta-model for prediction combination...")
        
        try:
            from sklearn.linear_model import LogisticRegression
            from sklearn.ensemble import RandomForestClassifier
            
            # Prepare meta-features
            meta_features = np.column_stack(base_predictions)
            
            if y is not None:
                # Supervised meta-learning
                self.meta_model_ = LogisticRegression(random_state=self.random_state)
                self.meta_model_.fit(meta_features, y)
                
            else:
                # Unsupervised meta-learning using consensus
                # Create pseudo-labels based on majority vote
                consensus = np.mean(meta_features, axis=1)
                pseudo_labels = (consensus > 0.5).astype(int)
                
                self.meta_model_ = LogisticRegression(random_state=self.random_state)
                self.meta_model_.fit(meta_features, pseudo_labels)
            
            logger.info("Meta-model training completed")
            
        except Exception as e:
            logger.warning(f"Meta-model training failed: {e}")
            self.meta_model_ = None
    
    def fit(self, X: Union[np.ndarray, pd.DataFrame], y: Optional[np.ndarray] = None) -> 'EnsembleAnomalyDetector':
        """
        Fit the ensemble anomaly detector.
        
        Args:
            X: Training features
            y: Optional labels for supervised learning
            
        Returns:
            Self (for method chaining)
        """
        logger.info(f"Training {self.name} with {len(self.base_models)} base models...")
        
        X = self.validate_input(X)
        
        # Calculate optimal weights if auto-weighting is enabled
        if self.auto_weight:
            self.model_weights = self._calculate_model_weights(X, y)
        
        # Train base models (parent class handles this)
        super().fit(X, y)
        
        # Get base model predictions for meta-learning
        if self.meta_learning:
            base_predictions = []
            for model in self.base_models:
                try:
                    preds = model.predict(X)
                    base_predictions.append(preds)
                except Exception as e:
                    logger.warning(f"Could not get predictions from {model.name}: {e}")
            
            if base_predictions:
                self._train_meta_model(X, base_predictions, y)
        
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
        
        # Dynamic model selection if enabled
        active_models = self._dynamic_model_selection(X)
        
        # Get predictions from active models
        predictions = []
        model_names = []
        
        for model in active_models:
            if model in self.base_models:  # Ensure model is trained
                try:
                    pred = model.predict(X)
                    predictions.append(pred)
                    model_names.append(model.name)
                except Exception as e:
                    logger.warning(f"Prediction failed for {model.name}: {e}")
        
        if not predictions:
            logger.warning("No models available for prediction")
            return np.zeros(X.shape[0])
        
        predictions_array = np.array(predictions)
        
        # Use meta-model if available
        if self.meta_model_ is not None:
            try:
                meta_features = predictions_array.T
                ensemble_predictions = self.meta_model_.predict(meta_features)
                return ensemble_predictions
            except Exception as e:
                logger.warning(f"Meta-model prediction failed: {e}")
        
        # Fallback to traditional aggregation
        return self._aggregate_predictions(predictions_array)
    
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
        
        # Dynamic model selection if enabled
        active_models = self._dynamic_model_selection(X)
        
        # Get scores from active models
        scores = []
        model_names = []
        
        for model in active_models:
            if model in self.base_models:  # Ensure model is trained
                try:
                    score = model.predict_proba(X)
                    scores.append(score)
                    model_names.append(model.name)
                except Exception as e:
                    logger.warning(f"Score prediction failed for {model.name}: {e}")
        
        if not scores:
            logger.warning("No models available for scoring")
            return np.zeros(X.shape[0])
        
        scores_array = np.array(scores)
        
        # Use meta-model for probability if available
        if self.meta_model_ is not None and hasattr(self.meta_model_, 'predict_proba'):
            try:
                # Get binary predictions for meta-features
                predictions = []
                for model in active_models:
                    if model in self.base_models:
                        try:
                            pred = model.predict(X)
                            predictions.append(pred)
                        except:
                            pass
                
                if predictions:
                    meta_features = np.array(predictions).T
                    proba = self.meta_model_.predict_proba(meta_features)
                    return proba[:, 1] if proba.shape[1] > 1 else proba.flatten()
            except Exception as e:
                logger.warning(f"Meta-model probability prediction failed: {e}")
        
        # Fallback to traditional score aggregation
        return self._aggregate_scores(scores_array)
    
    def get_model_contributions(self, X: Union[np.ndarray, pd.DataFrame]) -> Dict[str, Dict[str, np.ndarray]]:
        """
        Get individual model contributions to ensemble predictions.
        
        Args:
            X: Input features
            
        Returns:
            Dictionary mapping model names to their predictions and scores
        """
        contributions = super().get_model_contributions(X)
        
        # Add ensemble-specific information
        if self.optimal_weights_:
            for model_name in contributions:
                if model_name in self.optimal_weights_:
                    contributions[model_name]['weight'] = self.optimal_weights_[model_name]
        
        # Add meta-model predictions if available
        if self.meta_model_ is not None:
            try:
                meta_predictions = self.predict(X)
                contributions['meta_model'] = {
                    'predictions': meta_predictions,
                    'scores': self.predict_proba(X)
                }
            except Exception as e:
                logger.warning(f"Could not get meta-model contributions: {e}")
        
        return contributions
    
    def get_ensemble_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive ensemble summary.
        
        Returns:
            Dictionary with ensemble information
        """
        summary = self.get_model_info()
        
        if self.is_fitted:
            summary.update({
                'base_models': [model.name for model in self.base_models],
                'model_count': len(self.base_models),
                'aggregation_strategy': self.aggregation_strategy,
                'auto_weight': self.auto_weight,
                'optimal_weights': self.optimal_weights_,
                'meta_learning': self.meta_learning,
                'has_meta_model': self.meta_model_ is not None,
                'dynamic_selection': self.dynamic_selection,
                'model_types': [model.model_type for model in self.base_models]
            })
        
        return summary
    
    def update_weights(self, new_weights: Dict[str, float]) -> None:
        """
        Update model weights manually.
        
        Args:
            new_weights: Dictionary of new weights
        """
        # Validate weights
        for model_name in new_weights:
            if not any(model.name == model_name for model in self.base_models):
                raise ValueError(f"Model '{model_name}' not found in ensemble")
        
        # Normalize weights
        total_weight = sum(new_weights.values())
        if total_weight > 0:
            normalized_weights = {k: v / total_weight for k, v in new_weights.items()}
        else:
            normalized_weights = new_weights
        
        self.model_weights.update(normalized_weights)
        logger.info(f"Model weights updated: {normalized_weights}")
    
    def remove_model(self, model_name: str) -> None:
        """
        Remove a model from the ensemble.
        
        Args:
            model_name: Name of model to remove
        """
        models_to_keep = [model for model in self.base_models if model.name != model_name]
        
        if len(models_to_keep) < self.min_models:
            raise ValueError(f"Cannot remove model: would have less than {self.min_models} models")
        
        self.base_models = models_to_keep
        
        # Remove from weights
        if model_name in self.model_weights:
            del self.model_weights[model_name]
        
        logger.info(f"Removed model '{model_name}' from ensemble")
    
    def add_model(self, model: BaseAnomalyDetector, weight: float = 1.0) -> None:
        """
        Add a new model to the ensemble.
        
        Args:
            model: Model to add
            weight: Weight for the new model
        """
        if any(existing.name == model.name for existing in self.base_models):
            raise ValueError(f"Model with name '{model.name}' already exists in ensemble")
        
        self.base_models.append(model)
        self.model_weights[model.name] = weight
        
        # If ensemble is already fitted, fit the new model
        if self.is_fitted and hasattr(self, '_last_training_data'):
            try:
                model.fit(self._last_training_data)
                logger.info(f"Added and fitted model '{model.name}' to ensemble")
            except Exception as e:
                logger.warning(f"Could not fit new model '{model.name}': {e}")
        else:
            logger.info(f"Added model '{model.name}' to ensemble (requires retraining)")
        
        # Store training data for future model additions
        # Note: This would need to be implemented in the fit method