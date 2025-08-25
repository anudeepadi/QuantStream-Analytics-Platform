"""
Real-time Inference Engine for Anomaly Detection

This module provides a comprehensive real-time inference engine with stream processing
integration, batch prediction capabilities, and performance optimization.
"""

import logging
import asyncio
import threading
import queue
import time
from typing import Any, Dict, List, Optional, Union, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import pandas as pd
import json
from pathlib import Path

# Redis for caching
try:
    import redis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

# Kafka for streaming
try:
    from kafka import KafkaConsumer, KafkaProducer
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False

from ..models.base_model import BaseAnomalyDetector
from ..utils.mlflow_utils import MLflowTracker, MLflowModelRegistry, HAS_MLFLOW

logger = logging.getLogger(__name__)


@dataclass
class InferenceConfig:
    """Configuration for inference engine."""
    batch_size: int = 32
    max_batch_wait_time: float = 1.0  # seconds
    max_queue_size: int = 10000
    num_workers: int = 4
    enable_caching: bool = True
    cache_ttl: int = 300  # seconds
    feature_cache_size: int = 1000
    prediction_cache_size: int = 5000
    performance_monitoring: bool = True
    alert_thresholds: Dict[str, float] = field(default_factory=lambda: {
        'anomaly_score': 0.8,
        'batch_latency': 100.0,  # ms
        'queue_size': 1000
    })


@dataclass
class PredictionRequest:
    """Individual prediction request."""
    request_id: str
    features: np.ndarray
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = None
    callback: Optional[Callable] = None


@dataclass 
class PredictionResponse:
    """Prediction response."""
    request_id: str
    prediction: int
    anomaly_score: float
    confidence: float
    timestamp: datetime
    latency_ms: float
    model_name: str
    metadata: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class ModelCache:
    """In-memory model cache with LRU eviction."""
    
    def __init__(self, max_size: int = 10):
        """Initialize model cache."""
        self.max_size = max_size
        self.cache = {}
        self.access_times = {}
        
    def get(self, model_key: str) -> Optional[BaseAnomalyDetector]:
        """Get model from cache."""
        if model_key in self.cache:
            self.access_times[model_key] = time.time()
            return self.cache[model_key]
        return None
    
    def put(self, model_key: str, model: BaseAnomalyDetector) -> None:
        """Add model to cache."""
        # Evict least recently used if at capacity
        if len(self.cache) >= self.max_size:
            lru_key = min(self.access_times, key=self.access_times.get)
            del self.cache[lru_key]
            del self.access_times[lru_key]
        
        self.cache[model_key] = model
        self.access_times[model_key] = time.time()
    
    def remove(self, model_key: str) -> None:
        """Remove model from cache."""
        if model_key in self.cache:
            del self.cache[model_key]
            del self.access_times[model_key]
    
    def clear(self) -> None:
        """Clear all models from cache."""
        self.cache.clear()
        self.access_times.clear()


class FeatureCache:
    """Redis-backed feature cache for performance optimization."""
    
    def __init__(
        self, 
        redis_client: Optional[redis.Redis] = None,
        ttl: int = 300,
        max_local_cache: int = 1000
    ):
        """Initialize feature cache."""
        self.redis_client = redis_client
        self.ttl = ttl
        self.local_cache = {}
        self.local_access_times = {}
        self.max_local_cache = max_local_cache
        
    def _serialize_features(self, features: np.ndarray) -> str:
        """Serialize features for caching."""
        return json.dumps(features.tolist())
    
    def _deserialize_features(self, serialized: str) -> np.ndarray:
        """Deserialize features from cache."""
        return np.array(json.loads(serialized))
    
    def _generate_cache_key(self, features: np.ndarray) -> str:
        """Generate cache key for features."""
        # Simple hash of features
        feature_hash = hash(features.tobytes())
        return f"features:{feature_hash}"
    
    def get_processed_features(self, raw_features: np.ndarray) -> Optional[np.ndarray]:
        """Get processed features from cache."""
        cache_key = self._generate_cache_key(raw_features)
        
        # Try local cache first
        if cache_key in self.local_cache:
            self.local_access_times[cache_key] = time.time()
            return self.local_cache[cache_key]
        
        # Try Redis cache
        if self.redis_client:
            try:
                cached_data = self.redis_client.get(cache_key)
                if cached_data:
                    features = self._deserialize_features(cached_data.decode())
                    # Store in local cache
                    self._put_local(cache_key, features)
                    return features
            except Exception as e:
                logger.warning(f"Redis cache get failed: {e}")
        
        return None
    
    def put_processed_features(self, raw_features: np.ndarray, processed_features: np.ndarray) -> None:
        """Store processed features in cache."""
        cache_key = self._generate_cache_key(raw_features)
        
        # Store in local cache
        self._put_local(cache_key, processed_features)
        
        # Store in Redis cache
        if self.redis_client:
            try:
                serialized = self._serialize_features(processed_features)
                self.redis_client.setex(cache_key, self.ttl, serialized)
            except Exception as e:
                logger.warning(f"Redis cache put failed: {e}")
    
    def _put_local(self, cache_key: str, features: np.ndarray) -> None:
        """Store in local cache with LRU eviction."""
        # Evict LRU if at capacity
        if len(self.local_cache) >= self.max_local_cache:
            lru_key = min(self.local_access_times, key=self.local_access_times.get)
            del self.local_cache[lru_key]
            del self.local_access_times[lru_key]
        
        self.local_cache[cache_key] = features
        self.local_access_times[cache_key] = time.time()


class PredictionCache:
    """Cache for prediction results."""
    
    def __init__(
        self, 
        redis_client: Optional[redis.Redis] = None,
        ttl: int = 300,
        max_local_cache: int = 5000
    ):
        """Initialize prediction cache."""
        self.redis_client = redis_client
        self.ttl = ttl
        self.local_cache = {}
        self.local_access_times = {}
        self.max_local_cache = max_local_cache
    
    def _generate_cache_key(self, features: np.ndarray, model_name: str) -> str:
        """Generate cache key for prediction."""
        feature_hash = hash(features.tobytes())
        return f"prediction:{model_name}:{feature_hash}"
    
    def get_prediction(self, features: np.ndarray, model_name: str) -> Optional[Tuple[int, float]]:
        """Get cached prediction."""
        cache_key = self._generate_cache_key(features, model_name)
        
        # Try local cache first
        if cache_key in self.local_cache:
            self.local_access_times[cache_key] = time.time()
            return self.local_cache[cache_key]
        
        # Try Redis cache
        if self.redis_client:
            try:
                cached_data = self.redis_client.get(cache_key)
                if cached_data:
                    prediction, score = json.loads(cached_data.decode())
                    result = (int(prediction), float(score))
                    # Store in local cache
                    self._put_local(cache_key, result)
                    return result
            except Exception as e:
                logger.warning(f"Redis prediction cache get failed: {e}")
        
        return None
    
    def put_prediction(self, features: np.ndarray, model_name: str, prediction: int, score: float) -> None:
        """Cache prediction result."""
        cache_key = self._generate_cache_key(features, model_name)
        result = (prediction, score)
        
        # Store in local cache
        self._put_local(cache_key, result)
        
        # Store in Redis cache
        if self.redis_client:
            try:
                serialized = json.dumps([prediction, score])
                self.redis_client.setex(cache_key, self.ttl, serialized)
            except Exception as e:
                logger.warning(f"Redis prediction cache put failed: {e}")
    
    def _put_local(self, cache_key: str, result: Tuple[int, float]) -> None:
        """Store in local cache with LRU eviction."""
        # Evict LRU if at capacity
        if len(self.local_cache) >= self.max_local_cache:
            lru_key = min(self.local_access_times, key=self.local_access_times.get)
            del self.local_cache[lru_key]
            del self.local_access_times[lru_key]
        
        self.local_cache[cache_key] = result
        self.local_access_times[cache_key] = time.time()


class PerformanceMonitor:
    """Monitor inference engine performance and health."""
    
    def __init__(self, window_size: int = 1000):
        """Initialize performance monitor."""
        self.window_size = window_size
        self.request_times = []
        self.latencies = []
        self.errors = []
        self.queue_sizes = []
        self.cache_hits = 0
        self.cache_misses = 0
        
    def record_request(self, latency_ms: float, queue_size: int, error: bool = False) -> None:
        """Record a request for monitoring."""
        current_time = time.time()
        
        # Maintain sliding window
        if len(self.request_times) >= self.window_size:
            self.request_times.pop(0)
            self.latencies.pop(0)
            self.errors.pop(0)
            self.queue_sizes.pop(0)
        
        self.request_times.append(current_time)
        self.latencies.append(latency_ms)
        self.errors.append(error)
        self.queue_sizes.append(queue_size)
    
    def record_cache_hit(self) -> None:
        """Record cache hit."""
        self.cache_hits += 1
    
    def record_cache_miss(self) -> None:
        """Record cache miss."""
        self.cache_misses += 1
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get current performance statistics."""
        if not self.latencies:
            return {}
        
        current_time = time.time()
        recent_requests = sum(1 for t in self.request_times if current_time - t <= 60)  # Last minute
        
        stats = {
            'requests_per_minute': recent_requests,
            'avg_latency_ms': np.mean(self.latencies),
            'p95_latency_ms': np.percentile(self.latencies, 95),
            'p99_latency_ms': np.percentile(self.latencies, 99),
            'error_rate': np.mean(self.errors) if self.errors else 0.0,
            'avg_queue_size': np.mean(self.queue_sizes),
            'max_queue_size': np.max(self.queue_sizes),
            'cache_hit_rate': self.cache_hits / (self.cache_hits + self.cache_misses) if (self.cache_hits + self.cache_misses) > 0 else 0.0,
            'total_requests': len(self.request_times)
        }
        
        return stats


class RealTimeInferenceEngine:
    """
    Real-time inference engine for anomaly detection models.
    """
    
    def __init__(
        self,
        config: Optional[InferenceConfig] = None,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0
    ):
        """
        Initialize the inference engine.
        
        Args:
            config: Inference configuration
            redis_host: Redis server host
            redis_port: Redis server port
            redis_db: Redis database number
        """
        self.config = config or InferenceConfig()
        self.model_cache = ModelCache()
        
        # Initialize Redis client
        redis_client = None
        if HAS_REDIS and self.config.enable_caching:
            try:
                redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
                redis_client.ping()  # Test connection
                logger.info("Redis connection established")
            except Exception as e:
                logger.warning(f"Redis connection failed: {e}")
                redis_client = None
        
        # Initialize caches
        self.feature_cache = FeatureCache(
            redis_client=redis_client,
            ttl=self.config.cache_ttl,
            max_local_cache=self.config.feature_cache_size
        )
        
        self.prediction_cache = PredictionCache(
            redis_client=redis_client,
            ttl=self.config.cache_ttl,
            max_local_cache=self.config.prediction_cache_size
        )
        
        # Performance monitoring
        self.performance_monitor = PerformanceMonitor()
        
        # Request processing
        self.request_queue = queue.Queue(maxsize=self.config.max_queue_size)
        self.response_callbacks = {}
        self.executor = ThreadPoolExecutor(max_workers=self.config.num_workers)
        
        # Batch processing
        self.batch_processor = None
        self.processing_thread = None
        self.shutdown_event = threading.Event()
        
        # Default model
        self.default_model = None
        self.default_model_name = None
        
        # Alert system
        self.alert_callbacks = []
        
    def load_model(
        self, 
        model_or_path: Union[BaseAnomalyDetector, str, Path],
        model_name: Optional[str] = None
    ) -> str:
        """
        Load a model for inference.
        
        Args:
            model_or_path: Model instance or path to saved model
            model_name: Optional name for the model
            
        Returns:
            Model key for future reference
        """
        if isinstance(model_or_path, BaseAnomalyDetector):
            model = model_or_path
            model_key = model_name or model.name
        else:
            # Load from path
            model_path = Path(model_or_path)
            if not model_path.exists():
                raise FileNotFoundError(f"Model file not found: {model_path}")
            
            # Try to load using different methods
            try:
                if HAS_MLFLOW and "mlflow" in str(model_path).lower():
                    import mlflow
                    model = mlflow.pyfunc.load_model(str(model_path))
                else:
                    # Try pickle loading
                    import pickle
                    with open(model_path, 'rb') as f:
                        model = pickle.load(f)
            except Exception as e:
                logger.error(f"Failed to load model from {model_path}: {e}")
                raise
            
            model_key = model_name or model_path.stem
        
        # Validate model
        if not hasattr(model, 'predict') or not hasattr(model, 'predict_proba'):
            raise ValueError("Model must have predict and predict_proba methods")
        
        # Cache model
        self.model_cache.put(model_key, model)
        
        # Set as default if first model
        if self.default_model is None:
            self.default_model = model
            self.default_model_name = model_key
        
        logger.info(f"Model loaded: {model_key}")
        return model_key
    
    def set_default_model(self, model_key: str) -> None:
        """Set default model for inference."""
        model = self.model_cache.get(model_key)
        if model is None:
            raise ValueError(f"Model not found: {model_key}")
        
        self.default_model = model
        self.default_model_name = model_key
        logger.info(f"Default model set to: {model_key}")
    
    def start(self) -> None:
        """Start the inference engine."""
        if self.processing_thread is not None:
            logger.warning("Inference engine already started")
            return
        
        logger.info("Starting inference engine...")
        self.shutdown_event.clear()
        
        # Start batch processing thread
        self.processing_thread = threading.Thread(target=self._batch_processing_loop, daemon=True)
        self.processing_thread.start()
        
        logger.info("Inference engine started")
    
    def stop(self, timeout: float = 30.0) -> None:
        """Stop the inference engine."""
        if self.processing_thread is None:
            logger.warning("Inference engine not running")
            return
        
        logger.info("Stopping inference engine...")
        self.shutdown_event.set()
        
        # Wait for processing thread to finish
        self.processing_thread.join(timeout=timeout)
        
        # Shutdown executor
        self.executor.shutdown(wait=True, timeout=timeout)
        
        self.processing_thread = None
        logger.info("Inference engine stopped")
    
    def predict(
        self,
        features: Union[np.ndarray, List[List[float]]],
        model_name: Optional[str] = None,
        request_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> PredictionResponse:
        """
        Synchronous prediction.
        
        Args:
            features: Input features
            model_name: Model to use (default if None)
            request_id: Optional request identifier
            metadata: Optional metadata
            
        Returns:
            Prediction response
        """
        # Convert to numpy array
        if not isinstance(features, np.ndarray):
            features = np.array(features)
        
        if features.ndim == 1:
            features = features.reshape(1, -1)
        
        # Generate request ID
        if request_id is None:
            request_id = f"sync_{int(time.time() * 1000000)}"
        
        # Use default model if none specified
        if model_name is None:
            model_name = self.default_model_name
            model = self.default_model
        else:
            model = self.model_cache.get(model_name)
            if model is None:
                return PredictionResponse(
                    request_id=request_id,
                    prediction=-1,
                    anomaly_score=0.0,
                    confidence=0.0,
                    timestamp=datetime.now(),
                    latency_ms=0.0,
                    model_name=model_name,
                    metadata=metadata,
                    error=f"Model not found: {model_name}"
                )
        
        start_time = time.time()
        
        try:
            # Check prediction cache
            cached_result = None
            if self.config.enable_caching:
                cached_result = self.prediction_cache.get_prediction(features[0], model_name)
                if cached_result is not None:
                    self.performance_monitor.record_cache_hit()
                    prediction, anomaly_score = cached_result
                    latency_ms = (time.time() - start_time) * 1000
                    
                    return PredictionResponse(
                        request_id=request_id,
                        prediction=prediction,
                        anomaly_score=anomaly_score,
                        confidence=min(anomaly_score, 1.0),  # Simple confidence estimate
                        timestamp=datetime.now(),
                        latency_ms=latency_ms,
                        model_name=model_name,
                        metadata=metadata
                    )
            
            self.performance_monitor.record_cache_miss()
            
            # Make prediction
            predictions = model.predict(features)
            scores = model.predict_proba(features)
            
            prediction = int(predictions[0])
            anomaly_score = float(scores[0])
            confidence = min(anomaly_score, 1.0)
            
            # Cache result
            if self.config.enable_caching:
                self.prediction_cache.put_prediction(features[0], model_name, prediction, anomaly_score)
            
            latency_ms = (time.time() - start_time) * 1000
            
            # Record performance
            self.performance_monitor.record_request(latency_ms, self.request_queue.qsize())
            
            # Check for alerts
            self._check_alerts(anomaly_score, latency_ms)
            
            return PredictionResponse(
                request_id=request_id,
                prediction=prediction,
                anomaly_score=anomaly_score,
                confidence=confidence,
                timestamp=datetime.now(),
                latency_ms=latency_ms,
                model_name=model_name,
                metadata=metadata
            )
        
        except Exception as e:
            error_msg = f"Prediction failed: {str(e)}"
            logger.error(error_msg)
            
            latency_ms = (time.time() - start_time) * 1000
            self.performance_monitor.record_request(latency_ms, self.request_queue.qsize(), error=True)
            
            return PredictionResponse(
                request_id=request_id,
                prediction=-1,
                anomaly_score=0.0,
                confidence=0.0,
                timestamp=datetime.now(),
                latency_ms=latency_ms,
                model_name=model_name,
                metadata=metadata,
                error=error_msg
            )
    
    def predict_batch(
        self,
        features_batch: Union[np.ndarray, List[List[List[float]]]],
        model_name: Optional[str] = None,
        request_ids: Optional[List[str]] = None,
        metadata_batch: Optional[List[Dict[str, Any]]] = None
    ) -> List[PredictionResponse]:
        """
        Batch prediction for improved throughput.
        
        Args:
            features_batch: Batch of input features
            model_name: Model to use
            request_ids: Optional request identifiers
            metadata_batch: Optional metadata for each request
            
        Returns:
            List of prediction responses
        """
        # Convert to numpy array
        if not isinstance(features_batch, np.ndarray):
            features_batch = np.array(features_batch)
        
        batch_size = len(features_batch)
        
        # Generate request IDs if not provided
        if request_ids is None:
            base_time = int(time.time() * 1000000)
            request_ids = [f"batch_{base_time}_{i}" for i in range(batch_size)]
        
        if metadata_batch is None:
            metadata_batch = [None] * batch_size
        
        # Use default model if none specified
        if model_name is None:
            model_name = self.default_model_name
            model = self.default_model
        else:
            model = self.model_cache.get(model_name)
            if model is None:
                return [
                    PredictionResponse(
                        request_id=request_ids[i],
                        prediction=-1,
                        anomaly_score=0.0,
                        confidence=0.0,
                        timestamp=datetime.now(),
                        latency_ms=0.0,
                        model_name=model_name,
                        metadata=metadata_batch[i],
                        error=f"Model not found: {model_name}"
                    )
                    for i in range(batch_size)
                ]
        
        start_time = time.time()
        
        try:
            # Check cache for each sample
            cache_hits = {}
            features_to_predict = []
            indices_to_predict = []
            
            if self.config.enable_caching:
                for i, features in enumerate(features_batch):
                    cached_result = self.prediction_cache.get_prediction(features, model_name)
                    if cached_result is not None:
                        cache_hits[i] = cached_result
                        self.performance_monitor.record_cache_hit()
                    else:
                        features_to_predict.append(features)
                        indices_to_predict.append(i)
                        self.performance_monitor.record_cache_miss()
            else:
                features_to_predict = features_batch
                indices_to_predict = list(range(batch_size))
            
            # Predict for non-cached samples
            predictions_dict = {}
            scores_dict = {}
            
            if features_to_predict:
                features_array = np.array(features_to_predict)
                predictions = model.predict(features_array)
                scores = model.predict_proba(features_array)
                
                for i, idx in enumerate(indices_to_predict):
                    predictions_dict[idx] = int(predictions[i])
                    scores_dict[idx] = float(scores[i])
                    
                    # Cache results
                    if self.config.enable_caching:
                        self.prediction_cache.put_prediction(
                            features_array[i], model_name, predictions[i], scores[i]
                        )
            
            # Create responses
            responses = []
            latency_ms = (time.time() - start_time) * 1000
            
            for i in range(batch_size):
                if i in cache_hits:
                    prediction, anomaly_score = cache_hits[i]
                else:
                    prediction = predictions_dict[i]
                    anomaly_score = scores_dict[i]
                
                confidence = min(anomaly_score, 1.0)
                
                response = PredictionResponse(
                    request_id=request_ids[i],
                    prediction=prediction,
                    anomaly_score=anomaly_score,
                    confidence=confidence,
                    timestamp=datetime.now(),
                    latency_ms=latency_ms / batch_size,  # Amortized latency
                    model_name=model_name,
                    metadata=metadata_batch[i]
                )
                
                responses.append(response)
                
                # Check for alerts
                self._check_alerts(anomaly_score, latency_ms / batch_size)
            
            # Record performance
            self.performance_monitor.record_request(latency_ms, self.request_queue.qsize())
            
            logger.debug(f"Batch prediction completed: {batch_size} samples in {latency_ms:.2f}ms")
            return responses
        
        except Exception as e:
            error_msg = f"Batch prediction failed: {str(e)}"
            logger.error(error_msg)
            
            latency_ms = (time.time() - start_time) * 1000
            self.performance_monitor.record_request(latency_ms, self.request_queue.qsize(), error=True)
            
            return [
                PredictionResponse(
                    request_id=request_ids[i],
                    prediction=-1,
                    anomaly_score=0.0,
                    confidence=0.0,
                    timestamp=datetime.now(),
                    latency_ms=latency_ms / batch_size,
                    model_name=model_name,
                    metadata=metadata_batch[i],
                    error=error_msg
                )
                for i in range(batch_size)
            ]
    
    def _batch_processing_loop(self) -> None:
        """Main batch processing loop."""
        pending_requests = []
        
        while not self.shutdown_event.is_set():
            try:
                # Collect requests for batch processing
                batch_start_time = time.time()
                
                while (len(pending_requests) < self.config.batch_size and 
                       (time.time() - batch_start_time) < self.config.max_batch_wait_time):
                    
                    try:
                        request = self.request_queue.get(timeout=0.1)
                        pending_requests.append(request)
                    except queue.Empty:
                        continue
                
                if pending_requests:
                    # Process batch
                    self._process_batch(pending_requests)
                    pending_requests.clear()
                
            except Exception as e:
                logger.error(f"Batch processing error: {e}")
                time.sleep(0.1)
    
    def _process_batch(self, requests: List[PredictionRequest]) -> None:
        """Process a batch of requests."""
        if not requests:
            return
        
        # Group requests by model
        model_groups = {}
        for request in requests:
            model_name = getattr(request, 'model_name', self.default_model_name)
            if model_name not in model_groups:
                model_groups[model_name] = []
            model_groups[model_name].append(request)
        
        # Process each model group
        for model_name, model_requests in model_groups.items():
            try:
                features_batch = [req.features for req in model_requests]
                request_ids = [req.request_id for req in model_requests]
                metadata_batch = [req.metadata for req in model_requests]
                
                responses = self.predict_batch(features_batch, model_name, request_ids, metadata_batch)
                
                # Call callbacks
                for i, request in enumerate(model_requests):
                    if request.callback:
                        try:
                            request.callback(responses[i])
                        except Exception as e:
                            logger.error(f"Callback failed for request {request.request_id}: {e}")
            
            except Exception as e:
                logger.error(f"Batch processing failed for model {model_name}: {e}")
    
    def _check_alerts(self, anomaly_score: float, latency_ms: float) -> None:
        """Check for alert conditions and trigger callbacks."""
        alerts = []
        
        # Check anomaly score threshold
        if anomaly_score > self.config.alert_thresholds.get('anomaly_score', 1.0):
            alerts.append({
                'type': 'high_anomaly_score',
                'value': anomaly_score,
                'threshold': self.config.alert_thresholds['anomaly_score']
            })
        
        # Check latency threshold
        if latency_ms > self.config.alert_thresholds.get('batch_latency', float('inf')):
            alerts.append({
                'type': 'high_latency',
                'value': latency_ms,
                'threshold': self.config.alert_thresholds['batch_latency']
            })
        
        # Check queue size threshold
        queue_size = self.request_queue.qsize()
        if queue_size > self.config.alert_thresholds.get('queue_size', float('inf')):
            alerts.append({
                'type': 'high_queue_size',
                'value': queue_size,
                'threshold': self.config.alert_thresholds['queue_size']
            })
        
        # Trigger alert callbacks
        for alert in alerts:
            for callback in self.alert_callbacks:
                try:
                    callback(alert)
                except Exception as e:
                    logger.error(f"Alert callback failed: {e}")
    
    def add_alert_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Add alert callback function."""
        self.alert_callbacks.append(callback)
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get current performance statistics."""
        stats = self.performance_monitor.get_performance_stats()
        stats['queue_size'] = self.request_queue.qsize()
        stats['loaded_models'] = list(self.model_cache.cache.keys())
        stats['default_model'] = self.default_model_name
        return stats
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check and return status."""
        health = {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'issues': []
        }
        
        # Check if default model is available
        if self.default_model is None:
            health['issues'].append('No default model loaded')
            health['status'] = 'unhealthy'
        
        # Check queue size
        queue_size = self.request_queue.qsize()
        if queue_size > self.config.max_queue_size * 0.8:
            health['issues'].append(f'Queue size high: {queue_size}')
            health['status'] = 'degraded'
        
        # Check recent error rate
        stats = self.performance_monitor.get_performance_stats()
        error_rate = stats.get('error_rate', 0.0)
        if error_rate > 0.1:  # 10% error rate threshold
            health['issues'].append(f'High error rate: {error_rate:.2%}')
            health['status'] = 'degraded'
        
        # Check processing thread
        if self.processing_thread is None or not self.processing_thread.is_alive():
            health['issues'].append('Batch processing thread not running')
            health['status'] = 'unhealthy'
        
        health.update(stats)
        return health