"""
ML Model Integration

Provides integration between the feature store and ML models for
training data preparation and inference serving.
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
import pandas as pd
import numpy as np
from dataclasses import dataclass
from enum import Enum

from ..store.feature_store import FeatureStore


logger = logging.getLogger(__name__)


class DataSplitType(str, Enum):
    """Types of data splits for ML."""
    TRAIN = "train"
    VALIDATION = "validation" 
    TEST = "test"


@dataclass
class TrainingConfig:
    """Configuration for training data preparation."""
    
    feature_ids: List[str]
    target_column: str
    entity_column: str = "entity_id"
    timestamp_column: str = "timestamp"
    
    # Time-based splitting
    train_start: Optional[datetime] = None
    train_end: Optional[datetime] = None
    validation_start: Optional[datetime] = None
    validation_end: Optional[datetime] = None
    test_start: Optional[datetime] = None
    test_end: Optional[datetime] = None
    
    # Data preparation options
    fill_missing: bool = True
    normalize_features: bool = True
    remove_outliers: bool = False
    min_samples: int = 100
    
    # Target engineering
    target_transformation: Optional[str] = None  # "log", "diff", "returns"
    prediction_horizon: int = 1  # Periods ahead to predict


@dataclass 
class InferenceConfig:
    """Configuration for inference serving."""
    
    feature_ids: List[str]
    model_id: str
    entity_column: str = "entity_id"
    
    # Real-time options
    use_cache: bool = True
    max_staleness_minutes: int = 60
    fallback_to_default: bool = True
    default_values: Optional[Dict[str, float]] = None
    
    # Batch options  
    batch_size: int = 1000
    parallel_processing: bool = True


class TrainingDataBuilder:
    """
    Builds training datasets from the feature store with proper
    time-based splitting and preprocessing for ML models.
    """
    
    def __init__(self, feature_store: FeatureStore):
        self.feature_store = feature_store
    
    async def build_training_data(
        self,
        config: TrainingConfig,
        entities: Optional[List[str]] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Build training, validation, and test datasets.
        
        Args:
            config: Training configuration
            entities: List of entities to include (all if None)
            
        Returns:
            Dictionary with train/validation/test DataFrames
        """
        try:
            logger.info("Building training data...")
            
            # Get date ranges
            date_ranges = self._get_date_ranges(config)
            
            datasets = {}
            
            for split_type, (start_date, end_date) in date_ranges.items():
                if start_date and end_date:
                    logger.info(f"Building {split_type} dataset: {start_date} to {end_date}")
                    
                    # Get feature data for the date range
                    split_data = await self._get_features_for_daterange(
                        config, entities, start_date, end_date
                    )
                    
                    if split_data is not None and not split_data.empty:
                        # Preprocess data
                        processed_data = await self._preprocess_data(config, split_data)
                        datasets[split_type] = processed_data
                        
                        logger.info(f"{split_type} dataset: {len(processed_data)} records")
                    else:
                        logger.warning(f"No data found for {split_type} split")
            
            # Validate datasets
            await self._validate_datasets(datasets, config)
            
            return datasets
            
        except Exception as e:
            logger.error(f"Error building training data: {e}")
            raise
    
    async def create_feature_matrix(
        self,
        feature_ids: List[str],
        entities: List[str],
        start_date: datetime,
        end_date: datetime,
        entity_column: str = "entity_id"
    ) -> pd.DataFrame:
        """
        Create feature matrix for specified entities and date range.
        
        Args:
            feature_ids: Features to include
            entities: Entity identifiers
            start_date: Start date
            end_date: End date  
            entity_column: Entity column name
            
        Returns:
            Feature matrix DataFrame
        """
        try:
            # Get features data
            features_data = {}
            
            # Sample data points within date range (simplified approach)
            # In a real implementation, you'd query by date range
            for feature_id in feature_ids:
                feature_df = await self.feature_store.get_features(
                    feature_ids=[feature_id],
                    entities=entities,
                    entity_column=entity_column
                ).get(feature_id)
                
                if feature_df is not None and not feature_df.empty:
                    features_data[feature_id] = feature_df
            
            if not features_data:
                return pd.DataFrame()
            
            # Merge all features into single DataFrame
            merged_df = None
            
            for feature_id, feature_df in features_data.items():
                if merged_df is None:
                    merged_df = feature_df.copy()
                else:
                    merge_columns = ['timestamp', entity_column]
                    merge_columns = [col for col in merge_columns if col in feature_df.columns]
                    
                    if merge_columns:
                        merged_df = merged_df.merge(
                            feature_df,
                            on=merge_columns,
                            how='outer',
                            suffixes=('', f'_{feature_id}')
                        )
            
            return merged_df if merged_df is not None else pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error creating feature matrix: {e}")
            return pd.DataFrame()
    
    def _get_date_ranges(self, config: TrainingConfig) -> Dict[str, Tuple[datetime, datetime]]:
        """Get date ranges for train/validation/test splits."""
        ranges = {}
        
        if config.train_start and config.train_end:
            ranges[DataSplitType.TRAIN] = (config.train_start, config.train_end)
        
        if config.validation_start and config.validation_end:
            ranges[DataSplitType.VALIDATION] = (config.validation_start, config.validation_end)
        
        if config.test_start and config.test_end:
            ranges[DataSplitType.TEST] = (config.test_start, config.test_end)
        
        # If no explicit ranges, create them based on available data
        if not ranges:
            now = datetime.now(timezone.utc)
            ranges = {
                DataSplitType.TRAIN: (now - timedelta(days=365), now - timedelta(days=90)),
                DataSplitType.VALIDATION: (now - timedelta(days=90), now - timedelta(days=30)),
                DataSplitType.TEST: (now - timedelta(days=30), now)
            }
        
        return ranges
    
    async def _get_features_for_daterange(
        self,
        config: TrainingConfig,
        entities: Optional[List[str]],
        start_date: datetime,
        end_date: datetime
    ) -> Optional[pd.DataFrame]:
        """Get features for a specific date range."""
        try:
            if not entities:
                # In a real implementation, you'd get all entities from the feature store
                entities = ["DEFAULT"]  # Placeholder
            
            # Create feature matrix
            feature_matrix = await self.create_feature_matrix(
                feature_ids=config.feature_ids,
                entities=entities,
                start_date=start_date,
                end_date=end_date,
                entity_column=config.entity_column
            )
            
            return feature_matrix
            
        except Exception as e:
            logger.error(f"Error getting features for date range: {e}")
            return None
    
    async def _preprocess_data(self, config: TrainingConfig, data: pd.DataFrame) -> pd.DataFrame:
        """Preprocess training data."""
        try:
            processed_data = data.copy()
            
            # Handle missing values
            if config.fill_missing:
                # Forward fill then backward fill
                processed_data = processed_data.fillna(method='ffill').fillna(method='bfill')
                
                # Fill remaining with mean for numeric columns
                numeric_cols = processed_data.select_dtypes(include=[np.number]).columns
                processed_data[numeric_cols] = processed_data[numeric_cols].fillna(
                    processed_data[numeric_cols].mean()
                )
            
            # Remove outliers (simple IQR method)
            if config.remove_outliers:
                numeric_cols = processed_data.select_dtypes(include=[np.number]).columns
                for col in numeric_cols:
                    if col not in [config.entity_column, config.timestamp_column]:
                        Q1 = processed_data[col].quantile(0.25)
                        Q3 = processed_data[col].quantile(0.75)
                        IQR = Q3 - Q1
                        lower_bound = Q1 - 1.5 * IQR
                        upper_bound = Q3 + 1.5 * IQR
                        
                        processed_data = processed_data[
                            (processed_data[col] >= lower_bound) & 
                            (processed_data[col] <= upper_bound)
                        ]
            
            # Feature normalization
            if config.normalize_features:
                feature_cols = [col for col in processed_data.columns 
                              if col not in [config.entity_column, config.timestamp_column, config.target_column]]
                
                for col in feature_cols:
                    if processed_data[col].dtype in ['float64', 'int64']:
                        mean = processed_data[col].mean()
                        std = processed_data[col].std()
                        if std > 0:
                            processed_data[col] = (processed_data[col] - mean) / std
            
            # Target transformation
            if config.target_transformation and config.target_column in processed_data.columns:
                if config.target_transformation == "log":
                    processed_data[config.target_column] = np.log1p(processed_data[config.target_column])
                elif config.target_transformation == "diff":
                    processed_data[config.target_column] = processed_data[config.target_column].diff()
                elif config.target_transformation == "returns":
                    processed_data[config.target_column] = processed_data[config.target_column].pct_change()
            
            # Remove rows with insufficient data
            min_samples = max(config.min_samples, 10)
            if len(processed_data) < min_samples:
                logger.warning(f"Insufficient data: {len(processed_data)} < {min_samples}")
            
            return processed_data.dropna()
            
        except Exception as e:
            logger.error(f"Error preprocessing data: {e}")
            return data
    
    async def _validate_datasets(self, datasets: Dict[str, pd.DataFrame], config: TrainingConfig) -> None:
        """Validate training datasets."""
        try:
            for split_name, dataset in datasets.items():
                # Check minimum samples
                if len(dataset) < config.min_samples:
                    logger.warning(f"{split_name} dataset has only {len(dataset)} samples")
                
                # Check for required columns
                required_cols = config.feature_ids + [config.target_column]
                missing_cols = [col for col in required_cols if col not in dataset.columns]
                if missing_cols:
                    logger.error(f"{split_name} missing columns: {missing_cols}")
                
                # Check data quality
                null_counts = dataset.isnull().sum()
                if null_counts.sum() > 0:
                    logger.warning(f"{split_name} has null values: {null_counts[null_counts > 0].to_dict()}")
                
        except Exception as e:
            logger.error(f"Error validating datasets: {e}")


class InferenceEngine:
    """
    Provides real-time and batch inference capabilities using
    features from the feature store.
    """
    
    def __init__(self, feature_store: FeatureStore):
        self.feature_store = feature_store
        self.models: Dict[str, Any] = {}  # Model registry
    
    def register_model(self, model_id: str, model: Any, config: InferenceConfig) -> None:
        """Register a model for inference."""
        self.models[model_id] = {
            'model': model,
            'config': config,
            'registered_at': datetime.now(timezone.utc)
        }
        logger.info(f"Registered model: {model_id}")
    
    async def predict_single(
        self,
        model_id: str,
        entity_id: str,
        timestamp: Optional[datetime] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Make prediction for single entity.
        
        Args:
            model_id: Model identifier
            entity_id: Entity identifier
            timestamp: Point-in-time timestamp
            
        Returns:
            Prediction result with features and prediction
        """
        try:
            if model_id not in self.models:
                raise ValueError(f"Model {model_id} not registered")
            
            model_info = self.models[model_id]
            model = model_info['model']
            config = model_info['config']
            
            # Get feature vector
            feature_vector = await self.feature_store.get_feature_vector(
                feature_ids=config.feature_ids,
                entity_id=entity_id,
                timestamp=timestamp,
                use_cache=config.use_cache
            )
            
            if not feature_vector:
                if config.fallback_to_default and config.default_values:
                    feature_vector = config.default_values.copy()
                else:
                    logger.warning(f"No features found for entity {entity_id}")
                    return None
            
            # Prepare features for model
            feature_array = self._prepare_features(feature_vector, config.feature_ids)
            
            # Make prediction
            if hasattr(model, 'predict'):
                prediction = model.predict([feature_array])[0]
            elif hasattr(model, '__call__'):
                prediction = model(feature_array)
            else:
                raise ValueError(f"Model {model_id} doesn't have predict method or callable interface")
            
            # Track lineage
            if self.feature_store.lineage_tracker:
                await self.feature_store.lineage_tracker.track_model_inference(
                    model_id=model_id,
                    feature_ids=config.feature_ids,
                    prediction_count=1
                )
            
            return {
                'entity_id': entity_id,
                'model_id': model_id,
                'prediction': prediction,
                'features_used': feature_vector,
                'timestamp': timestamp or datetime.now(timezone.utc),
                'model_version': model_info.get('version', '1.0.0')
            }
            
        except Exception as e:
            logger.error(f"Error in single prediction: {e}")
            return None
    
    async def predict_batch(
        self,
        model_id: str,
        entity_ids: List[str],
        timestamp: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Make predictions for multiple entities in batch.
        
        Args:
            model_id: Model identifier
            entity_ids: List of entity identifiers
            timestamp: Point-in-time timestamp
            
        Returns:
            List of prediction results
        """
        try:
            if model_id not in self.models:
                raise ValueError(f"Model {model_id} not registered")
            
            model_info = self.models[model_id]
            model = model_info['model']
            config = model_info['config']
            
            # Get features for all entities
            features_data = await self.feature_store.get_features(
                feature_ids=config.feature_ids,
                entities=entity_ids,
                timestamp=timestamp,
                use_cache=config.use_cache
            )
            
            predictions = []
            
            # Process in batches
            for i in range(0, len(entity_ids), config.batch_size):
                batch_entities = entity_ids[i:i + config.batch_size]
                batch_features = []
                valid_entities = []
                
                for entity_id in batch_entities:
                    entity_features = {}
                    
                    # Collect features for this entity
                    for feature_id, feature_df in features_data.items():
                        if not feature_df.empty:
                            entity_data = feature_df[
                                feature_df[config.entity_column] == entity_id
                            ]
                            if not entity_data.empty:
                                # Get feature values
                                value_cols = [col for col in entity_data.columns 
                                            if not col.startswith('_') and 
                                            col not in [config.entity_column, 'timestamp']]
                                if len(value_cols) == 1:
                                    entity_features[feature_id] = entity_data.iloc[0][value_cols[0]]
                    
                    if entity_features:
                        feature_array = self._prepare_features(entity_features, config.feature_ids)
                        batch_features.append(feature_array)
                        valid_entities.append(entity_id)
                    elif config.fallback_to_default and config.default_values:
                        feature_array = self._prepare_features(config.default_values, config.feature_ids)
                        batch_features.append(feature_array)
                        valid_entities.append(entity_id)
                
                if batch_features:
                    # Make batch prediction
                    batch_predictions = model.predict(batch_features)
                    
                    # Create results
                    for entity_id, prediction in zip(valid_entities, batch_predictions):
                        predictions.append({
                            'entity_id': entity_id,
                            'model_id': model_id,
                            'prediction': prediction,
                            'timestamp': timestamp or datetime.now(timezone.utc)
                        })
            
            # Track lineage
            if self.feature_store.lineage_tracker:
                await self.feature_store.lineage_tracker.track_model_inference(
                    model_id=model_id,
                    feature_ids=config.feature_ids,
                    prediction_count=len(predictions)
                )
            
            return predictions
            
        except Exception as e:
            logger.error(f"Error in batch prediction: {e}")
            return []
    
    def _prepare_features(self, feature_vector: Dict[str, Any], feature_ids: List[str]) -> np.ndarray:
        """Prepare features for model input."""
        try:
            feature_values = []
            
            for feature_id in feature_ids:
                if feature_id in feature_vector:
                    value = feature_vector[feature_id]
                    if isinstance(value, dict):
                        # Multi-value feature - take first value or flatten
                        feature_values.extend(list(value.values()))
                    else:
                        feature_values.append(value)
                else:
                    feature_values.append(0.0)  # Default value for missing features
            
            return np.array(feature_values, dtype=np.float32)
            
        except Exception as e:
            logger.error(f"Error preparing features: {e}")
            return np.array([0.0] * len(feature_ids), dtype=np.float32)


class ModelIntegration:
    """
    Main integration class providing complete ML model lifecycle
    support with the feature store.
    """
    
    def __init__(self, feature_store: FeatureStore):
        self.feature_store = feature_store
        self.training_builder = TrainingDataBuilder(feature_store)
        self.inference_engine = InferenceEngine(feature_store)
    
    async def prepare_training_data(self, config: TrainingConfig) -> Dict[str, pd.DataFrame]:
        """Prepare training data with proper preprocessing."""
        return await self.training_builder.build_training_data(config)
    
    async def register_model_for_inference(
        self, 
        model_id: str, 
        model: Any, 
        config: InferenceConfig
    ) -> None:
        """Register model for inference serving."""
        self.inference_engine.register_model(model_id, model, config)
        
        # Track model registration in lineage
        if self.feature_store.lineage_tracker:
            await self.feature_store.lineage_tracker.track_model_training(
                model_id=model_id,
                feature_ids=config.feature_ids,
                training_records=0,  # Not available at registration
                parameters={"registered_at": datetime.now(timezone.utc).isoformat()}
            )
    
    async def serve_prediction(
        self,
        model_id: str,
        entity_id: str,
        timestamp: Optional[datetime] = None
    ) -> Optional[Dict[str, Any]]:
        """Serve single prediction."""
        return await self.inference_engine.predict_single(model_id, entity_id, timestamp)
    
    async def serve_batch_predictions(
        self,
        model_id: str,
        entity_ids: List[str],
        timestamp: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Serve batch predictions."""
        return await self.inference_engine.predict_batch(model_id, entity_ids, timestamp)
    
    async def get_model_performance_metrics(self, model_id: str) -> Dict[str, Any]:
        """Get performance metrics for a model."""
        if self.feature_store.lineage_tracker:
            # Get model lineage to see usage patterns
            lineage = await self.feature_store.lineage_tracker.get_feature_lineage(model_id)
            if lineage:
                return {
                    'model_id': model_id,
                    'total_predictions': lineage.total_accesses,
                    'last_prediction': lineage.last_accessed,
                    'features_used': lineage.upstream_features
                }
        
        return {'model_id': model_id, 'metrics_not_available': True}