"""
MLflow Utilities for QuantStream Analytics Platform

This module provides comprehensive utilities for MLflow integration including
experiment management, model registration, artifact handling, and deployment utilities.
"""

import logging
import os
import json
import pickle
from typing import Any, Dict, List, Optional, Union, Tuple
from pathlib import Path
from datetime import datetime
import hashlib
import tempfile

# MLflow imports
try:
    import mlflow
    import mlflow.sklearn
    import mlflow.tensorflow
    import mlflow.keras
    from mlflow.tracking import MlflowClient
    from mlflow.entities import Run, Experiment
    from mlflow.models.signature import ModelSignature
    from mlflow.types.schema import Schema, ColSpec
    from mlflow.exceptions import MlflowException
    HAS_MLFLOW = True
except ImportError:
    HAS_MLFLOW = False

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator

from ..models.base_model import BaseAnomalyDetector

logger = logging.getLogger(__name__)


class MLflowTracker:
    """
    MLflow experiment tracking and management utilities.
    
    This class provides a high-level interface for MLflow experiment tracking,
    model logging, and artifact management specifically for anomaly detection models.
    """
    
    def __init__(
        self,
        tracking_uri: Optional[str] = None,
        experiment_name: str = "quantstream_anomaly_detection",
        registry_uri: Optional[str] = None,
        artifact_location: Optional[str] = None
    ):
        """
        Initialize MLflow tracker.
        
        Args:
            tracking_uri: MLflow tracking server URI
            experiment_name: Name of the MLflow experiment
            registry_uri: MLflow model registry URI
            artifact_location: Custom artifact storage location
        """
        if not HAS_MLFLOW:
            raise ImportError("MLflow is required for model tracking")
        
        self.experiment_name = experiment_name
        self.client = MlflowClient(tracking_uri=tracking_uri, registry_uri=registry_uri)
        
        # Set MLflow configuration
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
        if registry_uri:
            mlflow.set_registry_uri(registry_uri)
        
        # Create or get experiment
        self.experiment = self._get_or_create_experiment(artifact_location)
        
        # Current run context
        self.current_run = None
        self.current_run_id = None
        
    def _get_or_create_experiment(self, artifact_location: Optional[str] = None) -> Experiment:
        """Get or create MLflow experiment."""
        try:
            experiment = self.client.get_experiment_by_name(self.experiment_name)
            if experiment is None:
                experiment_id = self.client.create_experiment(
                    self.experiment_name,
                    artifact_location=artifact_location
                )
                experiment = self.client.get_experiment(experiment_id)
            
            logger.info(f"Using experiment: {self.experiment_name} (ID: {experiment.experiment_id})")
            return experiment
        
        except Exception as e:
            logger.error(f"Failed to create/get experiment: {e}")
            raise
    
    def start_run(
        self,
        run_name: Optional[str] = None,
        nested: bool = False,
        tags: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Start a new MLflow run.
        
        Args:
            run_name: Name for the run
            nested: Whether this is a nested run
            tags: Additional tags for the run
            
        Returns:
            Run ID
        """
        if self.current_run is not None and not nested:
            logger.warning("Ending current run before starting new one")
            self.end_run()
        
        try:
            self.current_run = mlflow.start_run(
                experiment_id=self.experiment.experiment_id,
                run_name=run_name,
                nested=nested,
                tags=tags
            )
            self.current_run_id = self.current_run.info.run_id
            
            logger.info(f"Started run: {self.current_run_id}")
            return self.current_run_id
        
        except Exception as e:
            logger.error(f"Failed to start run: {e}")
            raise
    
    def end_run(self, status: str = "FINISHED") -> None:
        """End the current MLflow run."""
        if self.current_run is not None:
            try:
                mlflow.end_run(status=status)
                logger.info(f"Ended run: {self.current_run_id}")
                self.current_run = None
                self.current_run_id = None
            except Exception as e:
                logger.error(f"Failed to end run: {e}")
    
    def log_params(self, params: Dict[str, Any]) -> None:
        """Log parameters to current run."""
        if self.current_run is None:
            raise ValueError("No active run. Call start_run() first")
        
        try:
            # Convert all values to strings for MLflow
            str_params = {k: str(v) for k, v in params.items()}
            mlflow.log_params(str_params)
            logger.debug(f"Logged {len(params)} parameters")
        except Exception as e:
            logger.error(f"Failed to log parameters: {e}")
    
    def log_metrics(self, metrics: Dict[str, float], step: Optional[int] = None) -> None:
        """Log metrics to current run."""
        if self.current_run is None:
            raise ValueError("No active run. Call start_run() first")
        
        try:
            if step is not None:
                for key, value in metrics.items():
                    mlflow.log_metric(key, value, step=step)
            else:
                mlflow.log_metrics(metrics)
            logger.debug(f"Logged {len(metrics)} metrics")
        except Exception as e:
            logger.error(f"Failed to log metrics: {e}")
    
    def log_artifact(self, local_path: str, artifact_path: Optional[str] = None) -> None:
        """Log artifact to current run."""
        if self.current_run is None:
            raise ValueError("No active run. Call start_run() first")
        
        try:
            mlflow.log_artifact(local_path, artifact_path)
            logger.debug(f"Logged artifact: {local_path}")
        except Exception as e:
            logger.error(f"Failed to log artifact: {e}")
    
    def log_dict(self, dictionary: Dict[str, Any], filename: str) -> None:
        """Log dictionary as JSON artifact."""
        if self.current_run is None:
            raise ValueError("No active run. Call start_run() first")
        
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump(dictionary, f, indent=2, default=str)
                temp_path = f.name
            
            mlflow.log_artifact(temp_path, filename)
            os.unlink(temp_path)
            logger.debug(f"Logged dictionary as {filename}")
        except Exception as e:
            logger.error(f"Failed to log dictionary: {e}")
    
    def log_model(
        self,
        model: BaseAnomalyDetector,
        artifact_path: str = "model",
        signature: Optional[ModelSignature] = None,
        input_example: Optional[np.ndarray] = None,
        registered_model_name: Optional[str] = None,
        await_registration_for: int = 300
    ) -> str:
        """
        Log anomaly detection model to MLflow.
        
        Args:
            model: Trained anomaly detection model
            artifact_path: Path within run artifacts to save model
            signature: Model signature
            input_example: Example input for model
            registered_model_name: Name for model registry
            await_registration_for: Time to wait for registration
            
        Returns:
            Model URI
        """
        if self.current_run is None:
            raise ValueError("No active run. Call start_run() first")
        
        try:
            # Create model signature if not provided
            if signature is None and input_example is not None:
                signature = self._infer_signature(model, input_example)
            
            # Log model based on type
            if hasattr(model, '_model') and isinstance(model._model, BaseEstimator):
                # Scikit-learn based model (e.g., Isolation Forest)
                model_info = mlflow.sklearn.log_model(
                    sk_model=model._model,
                    artifact_path=artifact_path,
                    signature=signature,
                    input_example=input_example,
                    registered_model_name=registered_model_name,
                    await_registration_for=await_registration_for
                )
            
            elif hasattr(model, '_model') and hasattr(model._model, 'save'):
                # TensorFlow/Keras model (e.g., LSTM Autoencoder)
                model_info = mlflow.tensorflow.log_model(
                    tf_saved_model_dir=self._save_tf_model_temp(model._model),
                    tf_meta_graph_tags=None,
                    tf_signature_def_key=None,
                    artifact_path=artifact_path,
                    signature=signature,
                    input_example=input_example,
                    registered_model_name=registered_model_name,
                    await_registration_for=await_registration_for
                )
            
            else:
                # Custom model - save using pickle
                with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
                    pickle.dump(model, f)
                    temp_path = f.name
                
                mlflow.log_artifact(temp_path, f"{artifact_path}/model.pkl")
                os.unlink(temp_path)
                
                # Create model info manually
                model_info = type('ModelInfo', (), {
                    'model_uri': f"runs:/{self.current_run_id}/{artifact_path}"
                })()
            
            # Log model metadata
            model_metadata = {
                'model_name': model.name,
                'model_type': model.model_type,
                'is_fitted': model.is_fitted,
                'training_features': model.training_features,
                'hyperparameters': model.hyperparameters
            }
            
            self.log_dict(model_metadata, f"{artifact_path}/metadata.json")
            
            logger.info(f"Logged model to {model_info.model_uri}")
            return model_info.model_uri
        
        except Exception as e:
            logger.error(f"Failed to log model: {e}")
            raise
    
    def _infer_signature(self, model: BaseAnomalyDetector, input_example: np.ndarray) -> ModelSignature:
        """Infer model signature from input/output examples."""
        try:
            # Get predictions for signature
            predictions = model.predict(input_example)
            probabilities = model.predict_proba(input_example)
            
            # Create input schema
            input_schema = Schema([
                ColSpec("double", name=f"feature_{i}") 
                for i in range(input_example.shape[1])
            ])
            
            # Create output schema
            output_schema = Schema([
                ColSpec("long", name="prediction"),
                ColSpec("double", name="probability")
            ])
            
            signature = ModelSignature(inputs=input_schema, outputs=output_schema)
            return signature
        
        except Exception as e:
            logger.warning(f"Failed to infer model signature: {e}")
            return None
    
    def _save_tf_model_temp(self, tf_model) -> str:
        """Save TensorFlow model to temporary directory."""
        temp_dir = tempfile.mkdtemp()
        tf_model.save(temp_dir)
        return temp_dir
    
    def load_model(self, model_uri: str) -> Any:
        """Load model from MLflow."""
        try:
            if "sklearn" in model_uri or "artifact_path=model" in model_uri:
                return mlflow.sklearn.load_model(model_uri)
            elif "tensorflow" in model_uri:
                return mlflow.tensorflow.load_model(model_uri)
            else:
                # Try to load as generic Python model
                return mlflow.pyfunc.load_model(model_uri)
        
        except Exception as e:
            logger.error(f"Failed to load model from {model_uri}: {e}")
            raise
    
    def search_runs(
        self,
        filter_string: Optional[str] = None,
        run_view_type: str = "ACTIVE_ONLY",
        max_results: int = 1000,
        order_by: Optional[List[str]] = None
    ) -> List[Run]:
        """Search runs in current experiment."""
        try:
            runs = self.client.search_runs(
                experiment_ids=[self.experiment.experiment_id],
                filter_string=filter_string,
                run_view_type=run_view_type,
                max_results=max_results,
                order_by=order_by
            )
            return runs
        
        except Exception as e:
            logger.error(f"Failed to search runs: {e}")
            return []
    
    def get_best_run(
        self,
        metric_name: str,
        ascending: bool = False,
        filter_string: Optional[str] = None
    ) -> Optional[Run]:
        """Get best run based on metric."""
        try:
            order_by = [f"metrics.{metric_name} {'ASC' if ascending else 'DESC'}"]
            runs = self.search_runs(
                filter_string=filter_string,
                order_by=order_by,
                max_results=1
            )
            return runs[0] if runs else None
        
        except Exception as e:
            logger.error(f"Failed to get best run: {e}")
            return None
    
    def compare_runs(
        self,
        run_ids: List[str],
        metrics: List[str]
    ) -> pd.DataFrame:
        """Compare multiple runs across specified metrics."""
        try:
            data = []
            
            for run_id in run_ids:
                run = self.client.get_run(run_id)
                row = {'run_id': run_id, 'run_name': run.data.tags.get('mlflow.runName', 'N/A')}
                
                for metric in metrics:
                    row[metric] = run.data.metrics.get(metric, np.nan)
                
                data.append(row)
            
            return pd.DataFrame(data)
        
        except Exception as e:
            logger.error(f"Failed to compare runs: {e}")
            return pd.DataFrame()


class MLflowModelRegistry:
    """
    MLflow Model Registry utilities for model versioning and lifecycle management.
    """
    
    def __init__(self, registry_uri: Optional[str] = None):
        """Initialize model registry client."""
        if not HAS_MLFLOW:
            raise ImportError("MLflow is required for model registry")
        
        self.client = MlflowClient(registry_uri=registry_uri)
        if registry_uri:
            mlflow.set_registry_uri(registry_uri)
    
    def create_registered_model(
        self,
        name: str,
        tags: Optional[Dict[str, str]] = None,
        description: Optional[str] = None
    ) -> None:
        """Create a new registered model."""
        try:
            self.client.create_registered_model(
                name=name,
                tags=tags,
                description=description
            )
            logger.info(f"Created registered model: {name}")
        
        except MlflowException as e:
            if "already exists" in str(e).lower():
                logger.info(f"Registered model {name} already exists")
            else:
                logger.error(f"Failed to create registered model: {e}")
                raise
    
    def register_model(
        self,
        model_uri: str,
        name: str,
        tags: Optional[Dict[str, str]] = None,
        description: Optional[str] = None
    ) -> str:
        """Register a model version."""
        try:
            # Create registered model if it doesn't exist
            try:
                self.client.get_registered_model(name)
            except MlflowException:
                self.create_registered_model(name, description=description)
            
            # Register model version
            model_version = self.client.create_model_version(
                name=name,
                source=model_uri,
                tags=tags,
                description=description,
                run_id=model_uri.split('/')[-2] if 'runs:' in model_uri else None
            )
            
            logger.info(f"Registered model version: {name} v{model_version.version}")
            return model_version.version
        
        except Exception as e:
            logger.error(f"Failed to register model: {e}")
            raise
    
    def transition_model_version_stage(
        self,
        name: str,
        version: str,
        stage: str,
        archive_existing_versions: bool = False
    ) -> None:
        """Transition model version to a new stage."""
        try:
            self.client.transition_model_version_stage(
                name=name,
                version=version,
                stage=stage,
                archive_existing_versions=archive_existing_versions
            )
            logger.info(f"Transitioned {name} v{version} to {stage}")
        
        except Exception as e:
            logger.error(f"Failed to transition model stage: {e}")
            raise
    
    def get_model_version(self, name: str, version: str):
        """Get specific model version."""
        try:
            return self.client.get_model_version(name, version)
        except Exception as e:
            logger.error(f"Failed to get model version: {e}")
            raise
    
    def get_latest_version(self, name: str, stage: Optional[str] = None):
        """Get latest model version, optionally filtered by stage."""
        try:
            versions = self.client.get_latest_versions(name, stages=[stage] if stage else None)
            return versions[0] if versions else None
        except Exception as e:
            logger.error(f"Failed to get latest version: {e}")
            return None
    
    def list_model_versions(self, name: str) -> List:
        """List all versions of a registered model."""
        try:
            return self.client.search_model_versions(f"name='{name}'")
        except Exception as e:
            logger.error(f"Failed to list model versions: {e}")
            return []
    
    def delete_model_version(self, name: str, version: str) -> None:
        """Delete a model version."""
        try:
            self.client.delete_model_version(name, version)
            logger.info(f"Deleted model version: {name} v{version}")
        except Exception as e:
            logger.error(f"Failed to delete model version: {e}")
            raise
    
    def update_model_version(
        self,
        name: str,
        version: str,
        description: Optional[str] = None
    ) -> None:
        """Update model version description."""
        try:
            self.client.update_model_version(
                name=name,
                version=version,
                description=description
            )
            logger.info(f"Updated model version: {name} v{version}")
        except Exception as e:
            logger.error(f"Failed to update model version: {e}")
            raise


def setup_mlflow_tracking(
    tracking_uri: str = "http://localhost:5000",
    experiment_name: str = "quantstream_anomaly_detection",
    model_registry_uri: Optional[str] = None
) -> Tuple[MLflowTracker, MLflowModelRegistry]:
    """
    Setup MLflow tracking and model registry.
    
    Args:
        tracking_uri: MLflow tracking server URI
        experiment_name: Experiment name
        model_registry_uri: Model registry URI
        
    Returns:
        Tuple of (tracker, registry)
    """
    if not HAS_MLFLOW:
        raise ImportError("MLflow is required for tracking setup")
    
    # Initialize tracker and registry
    tracker = MLflowTracker(
        tracking_uri=tracking_uri,
        experiment_name=experiment_name,
        registry_uri=model_registry_uri
    )
    
    registry = MLflowModelRegistry(registry_uri=model_registry_uri)
    
    logger.info("MLflow tracking and registry setup completed")
    return tracker, registry


def generate_model_signature(
    model: BaseAnomalyDetector,
    input_example: np.ndarray
) -> Optional[ModelSignature]:
    """
    Generate MLflow model signature for anomaly detection model.
    
    Args:
        model: Trained model
        input_example: Example input data
        
    Returns:
        Model signature or None if generation fails
    """
    try:
        if not HAS_MLFLOW:
            logger.warning("MLflow not available for signature generation")
            return None
        
        # Get predictions
        predictions = model.predict(input_example)
        probabilities = model.predict_proba(input_example)
        
        # Infer signature from input/output
        signature = mlflow.models.infer_signature(
            input_example,
            {"predictions": predictions, "probabilities": probabilities}
        )
        
        return signature
    
    except Exception as e:
        logger.warning(f"Failed to generate model signature: {e}")
        return None


def create_model_deployment_config(
    model_name: str,
    model_version: str,
    deployment_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Create deployment configuration for MLflow model serving.
    
    Args:
        model_name: Registered model name
        model_version: Model version
        deployment_config: Deployment configuration
        
    Returns:
        Complete deployment configuration
    """
    config = {
        "model_name": model_name,
        "model_version": model_version,
        "model_uri": f"models:/{model_name}/{model_version}",
        "created_at": datetime.now().isoformat(),
        **deployment_config
    }
    
    return config