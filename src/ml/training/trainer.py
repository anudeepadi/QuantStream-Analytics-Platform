"""
Training Pipeline for Anomaly Detection Models

This module provides a comprehensive training pipeline with hyperparameter tuning,
cross-validation, performance monitoring, and MLflow integration.
"""

import logging
import warnings
from typing import Any, Dict, List, Optional, Union, Tuple, Callable
from dataclasses import dataclass
from datetime import datetime
import numpy as np
import pandas as pd
from sklearn.model_selection import (
    train_test_split, 
    TimeSeriesSplit, 
    StratifiedKFold,
    ParameterGrid,
    RandomizedSearchCV,
    GridSearchCV
)
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
from sklearn.preprocessing import StandardScaler, LabelEncoder

# Hyperparameter optimization
try:
    from hyperopt import hp, fmin, tpe, Trials, STATUS_OK
    from hyperopt.early_stop import no_progress_loss
    HAS_HYPEROPT = True
except ImportError:
    HAS_HYPEROPT = False

# MLflow integration
try:
    from ..utils.mlflow_utils import MLflowTracker, MLflowModelRegistry, HAS_MLFLOW
except ImportError:
    HAS_MLFLOW = False
    MLflowTracker = None
    MLflowModelRegistry = None

from ..models.base_model import BaseAnomalyDetector
from ..models.anomaly_detectors import IsolationForestDetector, StatisticalAnomalyDetector, EnsembleAnomalyDetector

try:
    from ..models.anomaly_detectors import LSTMAutoencoderDetector
    HAS_LSTM = True
except ImportError:
    HAS_LSTM = False

logger = logging.getLogger(__name__)


@dataclass
class TrainingConfig:
    """Configuration for model training."""
    model_type: str
    hyperparameters: Dict[str, Any]
    validation_strategy: str = "time_series"  # "time_series", "stratified", "holdout"
    test_size: float = 0.2
    validation_size: float = 0.2
    cv_folds: int = 5
    scoring_metric: str = "f1_score"
    early_stopping: bool = True
    patience: int = 10
    min_delta: float = 1e-4
    max_epochs: int = 100
    use_gpu: bool = True
    random_state: Optional[int] = 42


@dataclass
class HyperparameterTuningConfig:
    """Configuration for hyperparameter tuning."""
    method: str = "hyperopt"  # "hyperopt", "grid_search", "random_search"
    n_trials: int = 100
    timeout: Optional[int] = None
    early_stopping_rounds: Optional[int] = 20
    objective_direction: str = "maximize"  # "maximize" or "minimize"
    cv_folds: int = 3
    scoring_metric: str = "f1_score"
    param_space: Optional[Dict[str, Any]] = None


class AnomalyDetectionTrainer:
    """
    Comprehensive trainer for anomaly detection models with hyperparameter tuning
    and MLflow integration.
    """
    
    def __init__(
        self,
        mlflow_tracking_uri: Optional[str] = None,
        mlflow_experiment_name: str = "anomaly_detection_training",
        model_registry_uri: Optional[str] = None,
        enable_mlflow: bool = True
    ):
        """
        Initialize the trainer.
        
        Args:
            mlflow_tracking_uri: MLflow tracking server URI
            mlflow_experiment_name: MLflow experiment name
            model_registry_uri: MLflow model registry URI
            enable_mlflow: Whether to enable MLflow tracking
        """
        self.enable_mlflow = enable_mlflow and HAS_MLFLOW
        
        if self.enable_mlflow:
            try:
                self.mlflow_tracker = MLflowTracker(
                    tracking_uri=mlflow_tracking_uri,
                    experiment_name=mlflow_experiment_name,
                    registry_uri=model_registry_uri
                )
                self.model_registry = MLflowModelRegistry(registry_uri=model_registry_uri)
                logger.info("MLflow tracking initialized")
            except Exception as e:
                logger.warning(f"MLflow initialization failed: {e}")
                self.enable_mlflow = False
        
        self.trained_models = {}
        self.training_history = {}
        self.best_models = {}
        
    def create_model(self, model_type: str, hyperparameters: Dict[str, Any]) -> BaseAnomalyDetector:
        """
        Create anomaly detection model based on type and hyperparameters.
        
        Args:
            model_type: Type of model to create
            hyperparameters: Model hyperparameters
            
        Returns:
            Initialized model
        """
        model_map = {
            "isolation_forest": IsolationForestDetector,
            "statistical": StatisticalAnomalyDetector,
            "ensemble": EnsembleAnomalyDetector
        }
        
        if HAS_LSTM:
            model_map["lstm_autoencoder"] = LSTMAutoencoderDetector
        
        if model_type not in model_map:
            raise ValueError(f"Unknown model type: {model_type}")
        
        model_class = model_map[model_type]
        return model_class(**hyperparameters)
    
    def prepare_data(
        self,
        X: Union[np.ndarray, pd.DataFrame],
        y: Optional[np.ndarray] = None,
        config: TrainingConfig = None
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        Prepare data for training with proper train/validation split.
        
        Args:
            X: Input features
            y: Target labels (optional)
            config: Training configuration
            
        Returns:
            Tuple of (X_train, X_val, y_train, y_val)
        """
        if config is None:
            config = TrainingConfig(model_type="isolation_forest", hyperparameters={})
        
        # Convert to numpy arrays
        if isinstance(X, pd.DataFrame):
            X = X.values
        
        # Handle missing labels for unsupervised learning
        if y is None:
            y = np.zeros(X.shape[0])  # Dummy labels
        
        # Split data based on validation strategy
        if config.validation_strategy == "time_series":
            # For time series data, maintain temporal order
            split_idx = int(X.shape[0] * (1 - config.validation_size))
            X_train, X_val = X[:split_idx], X[split_idx:]
            y_train, y_val = y[:split_idx], y[split_idx:]
        
        elif config.validation_strategy == "stratified":
            # Stratified split for balanced classes
            X_train, X_val, y_train, y_val = train_test_split(
                X, y,
                test_size=config.validation_size,
                stratify=y,
                random_state=config.random_state
            )
        
        else:  # holdout
            # Random holdout split
            X_train, X_val, y_train, y_val = train_test_split(
                X, y,
                test_size=config.validation_size,
                random_state=config.random_state
            )
        
        logger.info(f"Data split: Train={X_train.shape[0]}, Validation={X_val.shape[0]}")
        return X_train, X_val, y_train, y_val
    
    def evaluate_model(
        self,
        model: BaseAnomalyDetector,
        X_test: np.ndarray,
        y_test: np.ndarray,
        return_predictions: bool = False
    ) -> Dict[str, float]:
        """
        Evaluate model performance on test data.
        
        Args:
            model: Trained model
            X_test: Test features
            y_test: Test labels
            return_predictions: Whether to return predictions
            
        Returns:
            Dictionary of evaluation metrics
        """
        try:
            # Get predictions
            y_pred = model.predict(X_test)
            y_scores = model.predict_proba(X_test)
            
            # Calculate metrics
            metrics = model.evaluate(X_test, y_test)
            
            # Add additional metrics
            try:
                auc_score = roc_auc_score(y_test, y_scores)
                metrics['roc_auc'] = auc_score
            except ValueError:
                # ROC AUC not available for this data
                pass
            
            # Confusion matrix
            cm = confusion_matrix(y_test, y_pred)
            if cm.shape == (2, 2):  # Binary classification
                tn, fp, fn, tp = cm.ravel()
                metrics.update({
                    'true_negatives': tn,
                    'false_positives': fp,
                    'false_negatives': fn,
                    'true_positives': tp,
                    'specificity': tn / (tn + fp) if (tn + fp) > 0 else 0,
                    'sensitivity': tp / (tp + fn) if (tp + fn) > 0 else 0
                })
            
            if return_predictions:
                return metrics, y_pred, y_scores
            
            return metrics
        
        except Exception as e:
            logger.error(f"Model evaluation failed: {e}")
            return {'error': str(e)}
    
    def train_single_model(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: np.ndarray,
        y_val: np.ndarray,
        config: TrainingConfig,
        run_name: Optional[str] = None
    ) -> Tuple[BaseAnomalyDetector, Dict[str, float]]:
        """
        Train a single model with given configuration.
        
        Args:
            X_train: Training features
            y_train: Training labels
            X_val: Validation features
            y_val: Validation labels
            config: Training configuration
            run_name: Optional MLflow run name
            
        Returns:
            Tuple of (trained_model, validation_metrics)
        """
        # Start MLflow run
        if self.enable_mlflow:
            run_id = self.mlflow_tracker.start_run(run_name=run_name)
        
        try:
            # Create model
            model = self.create_model(config.model_type, config.hyperparameters)
            
            # Log configuration
            if self.enable_mlflow:
                self.mlflow_tracker.log_params({
                    "model_type": config.model_type,
                    **config.hyperparameters,
                    "validation_strategy": config.validation_strategy,
                    "cv_folds": config.cv_folds,
                    "scoring_metric": config.scoring_metric
                })
            
            # Train model
            logger.info(f"Training {config.model_type} model...")
            start_time = datetime.now()
            
            model.fit(X_train, y_train)
            
            training_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"Training completed in {training_time:.2f} seconds")
            
            # Evaluate on validation set
            val_metrics = self.evaluate_model(model, X_val, y_val)
            
            # Log metrics
            if self.enable_mlflow:
                metrics_to_log = {f"val_{k}": v for k, v in val_metrics.items() if isinstance(v, (int, float))}
                metrics_to_log["training_time_seconds"] = training_time
                self.mlflow_tracker.log_metrics(metrics_to_log)
                
                # Log model
                input_example = X_train[:5] if len(X_train) >= 5 else X_train
                self.mlflow_tracker.log_model(
                    model=model,
                    artifact_path="model",
                    input_example=input_example
                )
            
            logger.info(f"Validation metrics: {val_metrics}")
            return model, val_metrics
        
        except Exception as e:
            logger.error(f"Training failed: {e}")
            if self.enable_mlflow:
                self.mlflow_tracker.end_run(status="FAILED")
            raise
        
        finally:
            if self.enable_mlflow:
                self.mlflow_tracker.end_run()
    
    def hyperparameter_tuning(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: np.ndarray,
        y_val: np.ndarray,
        model_type: str,
        tuning_config: HyperparameterTuningConfig
    ) -> Tuple[Dict[str, Any], Dict[str, float]]:
        """
        Perform hyperparameter tuning for a model.
        
        Args:
            X_train: Training features
            y_train: Training labels
            X_val: Validation features
            y_val: Validation labels
            model_type: Type of model to tune
            tuning_config: Tuning configuration
            
        Returns:
            Tuple of (best_params, best_metrics)
        """
        logger.info(f"Starting hyperparameter tuning for {model_type}")
        
        if tuning_config.method == "hyperopt" and not HAS_HYPEROPT:
            logger.warning("Hyperopt not available, falling back to grid search")
            tuning_config.method = "grid_search"
        
        # Define parameter spaces
        param_spaces = self._get_parameter_spaces()
        
        if model_type not in param_spaces:
            raise ValueError(f"No parameter space defined for {model_type}")
        
        param_space = tuning_config.param_space or param_spaces[model_type]
        
        if tuning_config.method == "hyperopt":
            return self._hyperopt_tuning(
                X_train, y_train, X_val, y_val,
                model_type, param_space, tuning_config
            )
        
        elif tuning_config.method == "grid_search":
            return self._grid_search_tuning(
                X_train, y_train, X_val, y_val,
                model_type, param_space, tuning_config
            )
        
        elif tuning_config.method == "random_search":
            return self._random_search_tuning(
                X_train, y_train, X_val, y_val,
                model_type, param_space, tuning_config
            )
        
        else:
            raise ValueError(f"Unknown tuning method: {tuning_config.method}")
    
    def _get_parameter_spaces(self) -> Dict[str, Dict[str, Any]]:
        """Get parameter spaces for different model types."""
        spaces = {
            "isolation_forest": {
                "n_estimators": [50, 100, 200, 300],
                "max_samples": ["auto", 0.5, 0.7, 0.9],
                "contamination": ["auto", 0.05, 0.1, 0.15, 0.2],
                "max_features": [0.5, 0.7, 1.0],
                "bootstrap": [True, False]
            },
            
            "statistical": {
                "methods": [["zscore", "iqr"], ["zscore", "iqr", "moving_average"], ["all"]],
                "zscore_threshold": [2.0, 2.5, 3.0, 3.5],
                "iqr_multiplier": [1.0, 1.5, 2.0],
                "window_size": [10, 20, 30, 50],
                "combine_method": ["vote", "weighted_average"]
            },
            
            "lstm_autoencoder": {
                "sequence_length": [10, 20, 30, 50],
                "encoder_layers": [[32, 16], [64, 32, 16], [128, 64, 32]],
                "dropout_rate": [0.1, 0.2, 0.3],
                "learning_rate": [0.001, 0.01, 0.1],
                "batch_size": [16, 32, 64],
                "epochs": [50, 100, 150]
            },
            
            "ensemble": {
                "aggregation_strategy": ["average", "weighted", "majority_vote"],
                "auto_weight": [True, False],
                "voting_threshold": [0.3, 0.5, 0.7],
                "dynamic_selection": [True, False]
            }
        }
        
        return spaces
    
    def _hyperopt_tuning(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: np.ndarray,
        y_val: np.ndarray,
        model_type: str,
        param_space: Dict[str, Any],
        config: HyperparameterTuningConfig
    ) -> Tuple[Dict[str, Any], Dict[str, float]]:
        """Hyperparameter tuning using Hyperopt."""
        
        # Convert parameter space to Hyperopt format
        hp_space = {}
        for param, values in param_space.items():
            if isinstance(values, list):
                if all(isinstance(v, (int, float)) for v in values):
                    hp_space[param] = hp.choice(param, values)
                else:
                    hp_space[param] = hp.choice(param, values)
            else:
                hp_space[param] = values
        
        # Objective function
        def objective(params):
            try:
                if self.enable_mlflow:
                    run_name = f"hyperopt_trial_{model_type}"
                    run_id = self.mlflow_tracker.start_run(run_name=run_name, nested=True)
                
                # Create and train model
                model = self.create_model(model_type, params)
                model.fit(X_train, y_train)
                
                # Evaluate
                metrics = self.evaluate_model(model, X_val, y_val)
                score = metrics.get(config.scoring_metric, 0.0)
                
                # Log to MLflow
                if self.enable_mlflow:
                    self.mlflow_tracker.log_params(params)
                    self.mlflow_tracker.log_metrics({f"val_{k}": v for k, v in metrics.items() if isinstance(v, (int, float))})
                    self.mlflow_tracker.end_run()
                
                # Return loss (negative for maximization)
                loss = -score if config.objective_direction == "maximize" else score
                return {"loss": loss, "status": STATUS_OK, "eval_time": datetime.now()}
            
            except Exception as e:
                logger.error(f"Hyperopt trial failed: {e}")
                if self.enable_mlflow:
                    self.mlflow_tracker.end_run(status="FAILED")
                return {"loss": float('inf'), "status": STATUS_OK}
        
        # Run optimization
        trials = Trials()
        
        early_stop_fn = None
        if config.early_stopping_rounds:
            early_stop_fn = no_progress_loss(config.early_stopping_rounds)
        
        best = fmin(
            fn=objective,
            space=hp_space,
            algo=tpe.suggest,
            max_evals=config.n_trials,
            trials=trials,
            early_stop_fn=early_stop_fn,
            timeout=config.timeout
        )
        
        # Get best parameters and metrics
        best_trial = min(trials.trials, key=lambda x: x['result']['loss'])
        best_params = best_trial['misc']['vals']
        
        # Convert back from Hyperopt format
        for param, values in param_space.items():
            if isinstance(values, list) and param in best_params:
                idx = best_params[param][0]
                best_params[param] = values[idx]
        
        # Train final model with best parameters
        final_model = self.create_model(model_type, best_params)
        final_model.fit(X_train, y_train)
        best_metrics = self.evaluate_model(final_model, X_val, y_val)
        
        logger.info(f"Hyperopt tuning completed. Best {config.scoring_metric}: {best_metrics.get(config.scoring_metric, 'N/A')}")
        return best_params, best_metrics
    
    def _grid_search_tuning(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: np.ndarray,
        y_val: np.ndarray,
        model_type: str,
        param_space: Dict[str, Any],
        config: HyperparameterTuningConfig
    ) -> Tuple[Dict[str, Any], Dict[str, float]]:
        """Grid search hyperparameter tuning."""
        
        param_grid = list(ParameterGrid(param_space))
        best_score = -float('inf') if config.objective_direction == "maximize" else float('inf')
        best_params = None
        best_metrics = None
        
        logger.info(f"Grid search with {len(param_grid)} parameter combinations")
        
        for i, params in enumerate(param_grid):
            try:
                if self.enable_mlflow:
                    run_name = f"grid_search_{model_type}_{i+1}"
                    run_id = self.mlflow_tracker.start_run(run_name=run_name, nested=True)
                
                # Train model
                model = self.create_model(model_type, params)
                model.fit(X_train, y_train)
                
                # Evaluate
                metrics = self.evaluate_model(model, X_val, y_val)
                score = metrics.get(config.scoring_metric, 0.0)
                
                # Log to MLflow
                if self.enable_mlflow:
                    self.mlflow_tracker.log_params(params)
                    self.mlflow_tracker.log_metrics({f"val_{k}": v for k, v in metrics.items() if isinstance(v, (int, float))})
                    self.mlflow_tracker.end_run()
                
                # Update best
                is_better = (score > best_score) if config.objective_direction == "maximize" else (score < best_score)
                if is_better:
                    best_score = score
                    best_params = params
                    best_metrics = metrics
                
                logger.info(f"Grid search {i+1}/{len(param_grid)}: {config.scoring_metric}={score:.4f}")
            
            except Exception as e:
                logger.error(f"Grid search iteration {i+1} failed: {e}")
                if self.enable_mlflow:
                    self.mlflow_tracker.end_run(status="FAILED")
        
        logger.info(f"Grid search completed. Best {config.scoring_metric}: {best_score:.4f}")
        return best_params, best_metrics
    
    def _random_search_tuning(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: np.ndarray,
        y_val: np.ndarray,
        model_type: str,
        param_space: Dict[str, Any],
        config: HyperparameterTuningConfig
    ) -> Tuple[Dict[str, Any], Dict[str, float]]:
        """Random search hyperparameter tuning."""
        
        best_score = -float('inf') if config.objective_direction == "maximize" else float('inf')
        best_params = None
        best_metrics = None
        
        logger.info(f"Random search with {config.n_trials} trials")
        
        for i in range(config.n_trials):
            try:
                # Sample random parameters
                params = {}
                for param, values in param_space.items():
                    if isinstance(values, list):
                        params[param] = np.random.choice(values)
                    elif isinstance(values, tuple) and len(values) == 2:
                        # Assume range (min, max)
                        params[param] = np.random.uniform(values[0], values[1])
                
                if self.enable_mlflow:
                    run_name = f"random_search_{model_type}_{i+1}"
                    run_id = self.mlflow_tracker.start_run(run_name=run_name, nested=True)
                
                # Train model
                model = self.create_model(model_type, params)
                model.fit(X_train, y_train)
                
                # Evaluate
                metrics = self.evaluate_model(model, X_val, y_val)
                score = metrics.get(config.scoring_metric, 0.0)
                
                # Log to MLflow
                if self.enable_mlflow:
                    self.mlflow_tracker.log_params(params)
                    self.mlflow_tracker.log_metrics({f"val_{k}": v for k, v in metrics.items() if isinstance(v, (int, float))})
                    self.mlflow_tracker.end_run()
                
                # Update best
                is_better = (score > best_score) if config.objective_direction == "maximize" else (score < best_score)
                if is_better:
                    best_score = score
                    best_params = params
                    best_metrics = metrics
                
                logger.info(f"Random search {i+1}/{config.n_trials}: {config.scoring_metric}={score:.4f}")
            
            except Exception as e:
                logger.error(f"Random search iteration {i+1} failed: {e}")
                if self.enable_mlflow:
                    self.mlflow_tracker.end_run(status="FAILED")
        
        logger.info(f"Random search completed. Best {config.scoring_metric}: {best_score:.4f}")
        return best_params, best_metrics
    
    def train_with_cross_validation(
        self,
        X: np.ndarray,
        y: np.ndarray,
        model_type: str,
        hyperparameters: Dict[str, Any],
        cv_folds: int = 5,
        validation_strategy: str = "time_series"
    ) -> Dict[str, Any]:
        """
        Train model with cross-validation.
        
        Args:
            X: Input features
            y: Target labels
            model_type: Type of model
            hyperparameters: Model hyperparameters
            cv_folds: Number of CV folds
            validation_strategy: CV strategy
            
        Returns:
            Cross-validation results
        """
        logger.info(f"Cross-validation with {cv_folds} folds")
        
        # Choose CV strategy
        if validation_strategy == "time_series":
            cv = TimeSeriesSplit(n_splits=cv_folds)
        else:
            cv = StratifiedKFold(n_splits=cv_folds, shuffle=True, random_state=42)
        
        cv_scores = []
        cv_metrics = []
        
        for fold, (train_idx, val_idx) in enumerate(cv.split(X, y)):
            logger.info(f"Training fold {fold + 1}/{cv_folds}")
            
            X_train, X_val = X[train_idx], X[val_idx]
            y_train, y_val = y[train_idx], y[val_idx]
            
            # Train model
            model = self.create_model(model_type, hyperparameters)
            model.fit(X_train, y_train)
            
            # Evaluate
            metrics = self.evaluate_model(model, X_val, y_val)
            cv_metrics.append(metrics)
            
            # Store primary score
            score = metrics.get('f1_score', 0.0)
            cv_scores.append(score)
            
            logger.info(f"Fold {fold + 1} F1 score: {score:.4f}")
        
        # Aggregate results
        cv_results = {
            'cv_scores': cv_scores,
            'mean_score': np.mean(cv_scores),
            'std_score': np.std(cv_scores),
            'cv_metrics': cv_metrics
        }
        
        # Calculate mean metrics
        mean_metrics = {}
        for key in cv_metrics[0].keys():
            if isinstance(cv_metrics[0][key], (int, float)):
                values = [m[key] for m in cv_metrics if key in m]
                mean_metrics[f"mean_{key}"] = np.mean(values)
                mean_metrics[f"std_{key}"] = np.std(values)
        
        cv_results['mean_metrics'] = mean_metrics
        
        logger.info(f"Cross-validation completed. Mean F1: {cv_results['mean_score']:.4f} ± {cv_results['std_score']:.4f}")
        return cv_results
    
    def get_best_model(
        self,
        metric: str = "f1_score",
        ascending: bool = False
    ) -> Optional[BaseAnomalyDetector]:
        """Get best trained model based on metric."""
        if not self.best_models:
            return None
        
        best_model_name = None
        best_score = float('inf') if ascending else -float('inf')
        
        for model_name, model_info in self.best_models.items():
            score = model_info['metrics'].get(metric)
            if score is None:
                continue
            
            if (ascending and score < best_score) or (not ascending and score > best_score):
                best_score = score
                best_model_name = model_name
        
        return self.best_models.get(best_model_name, {}).get('model')