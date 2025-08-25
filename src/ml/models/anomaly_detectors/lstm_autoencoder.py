"""
LSTM Autoencoder Anomaly Detector Implementation

This module provides a deep learning-based anomaly detector using LSTM Autoencoders
for time-series anomaly detection in financial data.
"""

import logging
import warnings
from typing import Any, Dict, List, Optional, Union, Tuple, Callable
import numpy as np
import pandas as pd

# TensorFlow imports
try:
    import tensorflow as tf
    from tensorflow import keras
    from tensorflow.keras import layers, Model, callbacks
    from tensorflow.keras.optimizers import Adam
    from tensorflow.keras.losses import MeanSquaredError
    from tensorflow.keras.metrics import MeanAbsoluteError
    HAS_TENSORFLOW = True
except ImportError:
    HAS_TENSORFLOW = False
    logger = logging.getLogger(__name__)
    logger.warning("TensorFlow not available. LSTM Autoencoder will not function.")

from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.model_selection import train_test_split

from ..base_model import BaseAnomalyDetector

logger = logging.getLogger(__name__)


class LSTMAutoencoderDetector(BaseAnomalyDetector):
    """
    LSTM Autoencoder-based anomaly detector for time-series data.
    
    This detector uses a deep learning approach with LSTM layers to learn
    normal patterns in time-series data and detect anomalies based on
    reconstruction error.
    """
    
    def __init__(
        self,
        name: str = "LSTMAutoencoder",
        sequence_length: int = 30,
        n_features: Optional[int] = None,
        encoder_layers: List[int] = None,
        decoder_layers: List[int] = None,
        dropout_rate: float = 0.2,
        activation: str = 'tanh',
        learning_rate: float = 0.001,
        batch_size: int = 32,
        epochs: int = 100,
        validation_split: float = 0.2,
        patience: int = 10,
        min_delta: float = 1e-4,
        threshold_percentile: float = 95,
        scaler_type: str = 'standard',
        use_gpu: bool = True,
        verbose: int = 1,
        random_state: Optional[int] = None,
        **kwargs
    ):
        """
        Initialize LSTM Autoencoder detector.
        
        Args:
            name: Model name
            sequence_length: Length of input sequences
            n_features: Number of input features (auto-detected if None)
            encoder_layers: List of LSTM units for encoder layers
            decoder_layers: List of LSTM units for decoder layers
            dropout_rate: Dropout rate for regularization
            activation: Activation function for LSTM layers
            learning_rate: Learning rate for optimizer
            batch_size: Training batch size
            epochs: Maximum training epochs
            validation_split: Fraction of data for validation
            patience: Early stopping patience
            min_delta: Minimum change for early stopping
            threshold_percentile: Percentile for anomaly threshold
            scaler_type: Type of feature scaling
            use_gpu: Whether to use GPU if available
            verbose: Verbosity level
            random_state: Random state for reproducibility
        """
        if not HAS_TENSORFLOW:
            raise ImportError("TensorFlow is required for LSTM Autoencoder detector")
        
        if encoder_layers is None:
            encoder_layers = [64, 32, 16]
        if decoder_layers is None:
            decoder_layers = [16, 32, 64]
        
        hyperparameters = {
            'sequence_length': sequence_length,
            'n_features': n_features,
            'encoder_layers': encoder_layers,
            'decoder_layers': decoder_layers,
            'dropout_rate': dropout_rate,
            'activation': activation,
            'learning_rate': learning_rate,
            'batch_size': batch_size,
            'epochs': epochs,
            'validation_split': validation_split,
            'patience': patience,
            'min_delta': min_delta,
            'threshold_percentile': threshold_percentile,
            'scaler_type': scaler_type,
            'use_gpu': use_gpu
        }
        
        super().__init__(
            name=name,
            model_type='deep_learning',
            hyperparameters=hyperparameters,
            random_state=random_state,
            **kwargs
        )
        
        # Model architecture parameters
        self.sequence_length = sequence_length
        self.n_features = n_features
        self.encoder_layers = encoder_layers
        self.decoder_layers = decoder_layers
        self.dropout_rate = dropout_rate
        self.activation = activation
        
        # Training parameters
        self.learning_rate = learning_rate
        self.batch_size = batch_size
        self.epochs = epochs
        self.validation_split = validation_split
        self.patience = patience
        self.min_delta = min_delta
        self.verbose = verbose
        
        # Anomaly detection parameters
        self.threshold_percentile = threshold_percentile
        self.threshold_ = None
        
        # Preprocessing
        self.scaler_type = scaler_type
        self.scaler = None
        
        # GPU configuration
        self.use_gpu = use_gpu
        self._configure_gpu()
        
        # Model components
        self._model = None
        self.encoder = None
        self.decoder = None
        self.training_history_ = None
        self.reconstruction_errors_ = None
        
    def _configure_gpu(self) -> None:
        """Configure GPU settings for TensorFlow."""
        if self.use_gpu:
            gpus = tf.config.experimental.list_physical_devices('GPU')
            if gpus:
                try:
                    # Enable memory growth
                    for gpu in gpus:
                        tf.config.experimental.set_memory_growth(gpu, True)
                    logger.info(f"Configured {len(gpus)} GPU(s) for training")
                except RuntimeError as e:
                    logger.warning(f"GPU configuration failed: {e}")
            else:
                logger.info("No GPUs available, using CPU")
        else:
            # Disable GPU usage
            tf.config.set_visible_devices([], 'GPU')
            logger.info("GPU usage disabled, using CPU")
    
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
    
    def _create_sequences(self, data: np.ndarray) -> np.ndarray:
        """
        Create sequences for LSTM input.
        
        Args:
            data: Input data of shape (n_samples, n_features)
            
        Returns:
            Sequences of shape (n_sequences, sequence_length, n_features)
        """
        sequences = []
        
        for i in range(len(data) - self.sequence_length + 1):
            sequence = data[i:i + self.sequence_length]
            sequences.append(sequence)
        
        return np.array(sequences)
    
    def _build_autoencoder(self) -> Model:
        """
        Build LSTM Autoencoder model.
        
        Returns:
            Compiled Keras model
        """
        # Set random seeds for reproducibility
        if self.random_state is not None:
            tf.random.set_seed(self.random_state)
        
        # Input layer
        input_layer = keras.Input(shape=(self.sequence_length, self.n_features))
        
        # Encoder
        encoded = input_layer
        for i, units in enumerate(self.encoder_layers):
            return_sequences = i < len(self.encoder_layers) - 1
            encoded = layers.LSTM(
                units,
                activation=self.activation,
                return_sequences=return_sequences,
                dropout=self.dropout_rate,
                recurrent_dropout=self.dropout_rate,
                name=f'encoder_lstm_{i+1}'
            )(encoded)
        
        # Store encoder model
        self.encoder = Model(input_layer, encoded, name='encoder')
        
        # Decoder
        # Repeat the encoded vector to create sequences
        decoded = layers.RepeatVector(self.sequence_length)(encoded)
        
        for i, units in enumerate(self.decoder_layers):
            return_sequences = True  # Decoder always returns sequences
            decoded = layers.LSTM(
                units,
                activation=self.activation,
                return_sequences=return_sequences,
                dropout=self.dropout_rate,
                recurrent_dropout=self.dropout_rate,
                name=f'decoder_lstm_{i+1}'
            )(decoded)
        
        # Output layer - reconstruct original features
        output_layer = layers.TimeDistributed(
            layers.Dense(self.n_features, activation='linear'),
            name='reconstruction'
        )(decoded)
        
        # Create autoencoder model
        autoencoder = Model(input_layer, output_layer, name='lstm_autoencoder')
        
        # Compile model
        optimizer = Adam(learning_rate=self.learning_rate)
        autoencoder.compile(
            optimizer=optimizer,
            loss=MeanSquaredError(),
            metrics=[MeanAbsoluteError()]
        )
        
        # Store decoder model
        decoder_input = keras.Input(shape=(units,))
        decoder_repeated = layers.RepeatVector(self.sequence_length)(decoder_input)
        for i, layer in enumerate(autoencoder.layers):
            if layer.name.startswith('decoder_lstm') or layer.name == 'reconstruction':
                if i == len(autoencoder.layers) - 1:  # Last layer
                    decoder_output = layer(decoder_repeated)
                else:
                    decoder_repeated = layer(decoder_repeated)
        
        self.decoder = Model(decoder_input, decoder_output, name='decoder')
        
        if self.verbose > 0:
            autoencoder.summary()
        
        return autoencoder
    
    def _calculate_reconstruction_error(self, X_true: np.ndarray, X_pred: np.ndarray) -> np.ndarray:
        """
        Calculate reconstruction error for anomaly detection.
        
        Args:
            X_true: Original sequences
            X_pred: Reconstructed sequences
            
        Returns:
            Reconstruction errors for each sequence
        """
        # Calculate MSE for each sequence
        mse = np.mean(np.square(X_true - X_pred), axis=(1, 2))
        return mse
    
    def _determine_threshold(self, reconstruction_errors: np.ndarray) -> float:
        """
        Determine anomaly threshold based on reconstruction errors.
        
        Args:
            reconstruction_errors: Training reconstruction errors
            
        Returns:
            Anomaly threshold
        """
        threshold = np.percentile(reconstruction_errors, self.threshold_percentile)
        logger.info(f"Anomaly threshold set to {threshold:.6f} ({self.threshold_percentile}th percentile)")
        return threshold
    
    def fit(self, X: Union[np.ndarray, pd.DataFrame], y: Optional[np.ndarray] = None) -> 'LSTMAutoencoderDetector':
        """
        Fit the LSTM Autoencoder model.
        
        Args:
            X: Training features
            y: Ignored for unsupervised learning
            
        Returns:
            Self (for method chaining)
        """
        logger.info(f"Training {self.name} model...")
        
        # Validate and preprocess input
        X = self.validate_input(X)
        
        # Determine number of features if not specified
        if self.n_features is None:
            self.n_features = X.shape[1]
        
        # Initialize and fit scaler
        self._initialize_scaler()
        if self.scaler is not None:
            X_scaled = self.scaler.fit_transform(X)
        else:
            X_scaled = X.copy()
        
        # Create sequences
        X_sequences = self._create_sequences(X_scaled)
        logger.info(f"Created {len(X_sequences)} sequences of length {self.sequence_length}")
        
        if len(X_sequences) < self.batch_size:
            raise ValueError(f"Not enough sequences ({len(X_sequences)}) for batch size ({self.batch_size})")
        
        # Build model
        self._model = self._build_autoencoder()
        
        # Prepare callbacks
        callback_list = []
        
        # Early stopping
        early_stopping = callbacks.EarlyStopping(
            monitor='val_loss',
            patience=self.patience,
            min_delta=self.min_delta,
            restore_best_weights=True,
            verbose=self.verbose
        )
        callback_list.append(early_stopping)
        
        # Reduce learning rate on plateau
        reduce_lr = callbacks.ReduceLROnPlateau(
            monitor='val_loss',
            factor=0.5,
            patience=self.patience // 2,
            min_lr=self.learning_rate * 0.01,
            verbose=self.verbose
        )
        callback_list.append(reduce_lr)
        
        # Train model
        logger.info("Starting model training...")
        
        history = self._model.fit(
            X_sequences,
            X_sequences,  # Autoencoder trains to reconstruct input
            batch_size=self.batch_size,
            epochs=self.epochs,
            validation_split=self.validation_split,
            callbacks=callback_list,
            verbose=self.verbose,
            shuffle=True
        )
        
        self.training_history_ = history.history
        
        # Calculate reconstruction errors on training data for threshold
        X_reconstructed = self._model.predict(X_sequences, verbose=0)
        self.reconstruction_errors_ = self._calculate_reconstruction_error(X_sequences, X_reconstructed)
        
        # Determine anomaly threshold
        self.threshold_ = self._determine_threshold(self.reconstruction_errors_)
        
        # Update state
        self.is_fitted = True
        self.training_features = X.shape[1]
        
        # Update metadata
        self.metadata.hyperparameters = self.hyperparameters
        
        logger.info(f"Model training completed. Final loss: {history.history['loss'][-1]:.6f}")
        return self
    
    def predict(self, X: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        """
        Predict anomalies in the input data.
        
        Args:
            X: Input features for prediction
            
        Returns:
            Binary predictions (1 for anomaly, 0 for normal)
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")
        
        scores = self.predict_proba(X)
        return (scores > self.threshold_).astype(int)
    
    def predict_proba(self, X: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        """
        Predict anomaly scores based on reconstruction error.
        
        Args:
            X: Input features for prediction
            
        Returns:
            Reconstruction error scores
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")
        
        X = self.validate_input(X)
        
        # Scale features
        if self.scaler is not None:
            X_scaled = self.scaler.transform(X)
        else:
            X_scaled = X.copy()
        
        # Create sequences
        X_sequences = self._create_sequences(X_scaled)
        
        if len(X_sequences) == 0:
            logger.warning("No sequences could be created from input data")
            return np.zeros(X.shape[0])
        
        # Get reconstructions
        X_reconstructed = self._model.predict(X_sequences, verbose=0)
        
        # Calculate reconstruction errors
        reconstruction_errors = self._calculate_reconstruction_error(X_sequences, X_reconstructed)
        
        # Map sequence-level errors back to original data points
        # For simplicity, we'll assign the error to the last point of each sequence
        scores = np.zeros(X.shape[0])
        
        for i, error in enumerate(reconstruction_errors):
            end_idx = i + self.sequence_length
            if end_idx <= len(scores):
                scores[end_idx - 1] = error
        
        # For points at the beginning that don't have sequence-level scores,
        # use the first available score
        first_score_idx = self.sequence_length - 1
        if first_score_idx < len(scores) and scores[first_score_idx] > 0:
            scores[:first_score_idx] = scores[first_score_idx]
        
        return scores
    
    def encode(self, X: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        """
        Encode input sequences to latent representation.
        
        Args:
            X: Input features
            
        Returns:
            Encoded representations
        """
        if not self.is_fitted or self.encoder is None:
            raise ValueError("Model must be fitted before encoding")
        
        X = self.validate_input(X)
        
        # Scale features
        if self.scaler is not None:
            X_scaled = self.scaler.transform(X)
        else:
            X_scaled = X.copy()
        
        # Create sequences
        X_sequences = self._create_sequences(X_scaled)
        
        if len(X_sequences) == 0:
            return np.array([])
        
        # Encode sequences
        encoded = self.encoder.predict(X_sequences, verbose=0)
        return encoded
    
    def reconstruct(self, X: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        """
        Reconstruct input sequences.
        
        Args:
            X: Input features
            
        Returns:
            Reconstructed sequences
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before reconstruction")
        
        X = self.validate_input(X)
        
        # Scale features
        if self.scaler is not None:
            X_scaled = self.scaler.transform(X)
        else:
            X_scaled = X.copy()
        
        # Create sequences
        X_sequences = self._create_sequences(X_scaled)
        
        if len(X_sequences) == 0:
            return np.array([])
        
        # Reconstruct sequences
        X_reconstructed = self._model.predict(X_sequences, verbose=0)
        
        # Inverse transform if scaler was used
        if self.scaler is not None:
            # Reshape for inverse transform
            original_shape = X_reconstructed.shape
            X_reconstructed_flat = X_reconstructed.reshape(-1, X_reconstructed.shape[-1])
            X_reconstructed_unscaled = self.scaler.inverse_transform(X_reconstructed_flat)
            X_reconstructed = X_reconstructed_unscaled.reshape(original_shape)
        
        return X_reconstructed
    
    def set_threshold(self, threshold: float) -> None:
        """
        Set custom anomaly threshold.
        
        Args:
            threshold: New threshold value
        """
        if threshold <= 0:
            raise ValueError("Threshold must be positive")
        
        self.threshold_ = threshold
        logger.info(f"Anomaly threshold updated to {threshold:.6f}")
    
    def set_threshold_percentile(self, percentile: float) -> None:
        """
        Set threshold based on percentile of training reconstruction errors.
        
        Args:
            percentile: Percentile value (0-100)
        """
        if not 0 <= percentile <= 100:
            raise ValueError("Percentile must be between 0 and 100")
        
        if self.reconstruction_errors_ is None:
            raise ValueError("No training reconstruction errors available")
        
        self.threshold_percentile = percentile
        self.threshold_ = self._determine_threshold(self.reconstruction_errors_)
    
    def plot_training_history(self) -> Optional[object]:
        """
        Plot training history.
        
        Returns:
            Matplotlib figure object or None if matplotlib not available
        """
        if self.training_history_ is None:
            logger.warning("No training history available")
            return None
        
        try:
            import matplotlib.pyplot as plt
            
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
            
            # Plot loss
            ax1.plot(self.training_history_['loss'], label='Training Loss')
            if 'val_loss' in self.training_history_:
                ax1.plot(self.training_history_['val_loss'], label='Validation Loss')
            ax1.set_title('Model Loss')
            ax1.set_xlabel('Epoch')
            ax1.set_ylabel('Loss')
            ax1.legend()
            
            # Plot MAE
            ax2.plot(self.training_history_['mean_absolute_error'], label='Training MAE')
            if 'val_mean_absolute_error' in self.training_history_:
                ax2.plot(self.training_history_['val_mean_absolute_error'], label='Validation MAE')
            ax2.set_title('Mean Absolute Error')
            ax2.set_xlabel('Epoch')
            ax2.set_ylabel('MAE')
            ax2.legend()
            
            plt.tight_layout()
            return fig
            
        except ImportError:
            logger.warning("Matplotlib not available for plotting")
            return None
    
    def get_model_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive model summary.
        
        Returns:
            Dictionary with model information and statistics
        """
        summary = self.get_model_info()
        
        if self.is_fitted:
            summary.update({
                'sequence_length': self.sequence_length,
                'n_features': self.n_features,
                'encoder_layers': self.encoder_layers,
                'decoder_layers': self.decoder_layers,
                'threshold': self.threshold_,
                'threshold_percentile': self.threshold_percentile,
                'training_epochs': len(self.training_history_['loss']) if self.training_history_ else None,
                'final_training_loss': self.training_history_['loss'][-1] if self.training_history_ else None,
                'reconstruction_errors_stats': {
                    'mean': float(np.mean(self.reconstruction_errors_)) if self.reconstruction_errors_ is not None else None,
                    'std': float(np.std(self.reconstruction_errors_)) if self.reconstruction_errors_ is not None else None,
                    'min': float(np.min(self.reconstruction_errors_)) if self.reconstruction_errors_ is not None else None,
                    'max': float(np.max(self.reconstruction_errors_)) if self.reconstruction_errors_ is not None else None,
                }
            })
        
        return summary