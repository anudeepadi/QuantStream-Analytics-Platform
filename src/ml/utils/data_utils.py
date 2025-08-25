"""
Data Utilities for QuantStream Analytics Platform

This module provides utility functions for data processing and manipulation
in the ML pipeline.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple, Union
import json
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)


class DataProcessor:
    """Utility class for data processing operations."""
    
    @staticmethod
    def handle_missing_values(
        data: pd.DataFrame,
        strategy: str = 'forward_fill',
        columns: Optional[List[str]] = None,
        fill_value: Optional[Any] = None
    ) -> pd.DataFrame:
        """
        Handle missing values in the dataset.
        
        Args:
            data: Input dataframe
            strategy: Strategy for handling missing values
            columns: Specific columns to process (None for all)
            fill_value: Value to use for 'constant' strategy
            
        Returns:
            Processed dataframe
        """
        df = data.copy()
        
        if columns is None:
            columns = df.columns.tolist()
        
        for col in columns:
            if col not in df.columns:
                continue
                
            missing_count = df[col].isnull().sum()
            if missing_count == 0:
                continue
            
            logger.info(f"Handling {missing_count} missing values in column '{col}' using {strategy}")
            
            if strategy == 'forward_fill':
                df[col] = df[col].ffill()
            elif strategy == 'backward_fill':
                df[col] = df[col].bfill()
            elif strategy == 'interpolate':
                df[col] = df[col].interpolate(method='linear')
            elif strategy == 'mean':
                df[col] = df[col].fillna(df[col].mean())
            elif strategy == 'median':
                df[col] = df[col].fillna(df[col].median())
            elif strategy == 'mode':
                df[col] = df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else 0)
            elif strategy == 'constant':
                df[col] = df[col].fillna(fill_value)
            elif strategy == 'drop':
                df = df.dropna(subset=[col])
            else:
                raise ValueError(f"Unknown missing value strategy: {strategy}")
        
        return df
    
    @staticmethod
    def detect_outliers(
        data: pd.DataFrame,
        columns: Optional[List[str]] = None,
        method: str = 'iqr',
        threshold: float = 1.5
    ) -> Dict[str, List[int]]:
        """
        Detect outliers in the dataset.
        
        Args:
            data: Input dataframe
            columns: Columns to check for outliers
            method: Outlier detection method ('iqr', 'zscore', 'modified_zscore')
            threshold: Threshold for outlier detection
            
        Returns:
            Dictionary mapping column names to lists of outlier indices
        """
        if columns is None:
            columns = data.select_dtypes(include=[np.number]).columns.tolist()
        
        outliers = {}
        
        for col in columns:
            if col not in data.columns:
                continue
            
            series = data[col].dropna()
            outlier_indices = []
            
            if method == 'iqr':
                Q1 = series.quantile(0.25)
                Q3 = series.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR
                outlier_mask = (series < lower_bound) | (series > upper_bound)
                outlier_indices = series[outlier_mask].index.tolist()
            
            elif method == 'zscore':
                z_scores = np.abs(stats.zscore(series))
                outlier_mask = z_scores > threshold
                outlier_indices = series[outlier_mask].index.tolist()
            
            elif method == 'modified_zscore':
                median = np.median(series)
                mad = np.median(np.abs(series - median))
                modified_z_scores = 0.6745 * (series - median) / mad
                outlier_mask = np.abs(modified_z_scores) > threshold
                outlier_indices = series[outlier_mask].index.tolist()
            
            else:
                raise ValueError(f"Unknown outlier detection method: {method}")
            
            if outlier_indices:
                outliers[col] = outlier_indices
                logger.info(f"Detected {len(outlier_indices)} outliers in column '{col}'")
        
        return outliers
    
    @staticmethod
    def remove_outliers(
        data: pd.DataFrame,
        outlier_indices: Dict[str, List[int]],
        strategy: str = 'remove'
    ) -> pd.DataFrame:
        """
        Remove or treat outliers in the dataset.
        
        Args:
            data: Input dataframe
            outlier_indices: Dictionary of outlier indices per column
            strategy: How to handle outliers ('remove', 'clip', 'replace')
            
        Returns:
            Processed dataframe
        """
        df = data.copy()
        
        if strategy == 'remove':
            # Remove rows that contain outliers
            all_outlier_indices = set()
            for indices in outlier_indices.values():
                all_outlier_indices.update(indices)
            df = df.drop(index=list(all_outlier_indices))
        
        elif strategy == 'clip':
            # Clip outliers to reasonable bounds
            for col, indices in outlier_indices.items():
                if col not in df.columns:
                    continue
                
                series = df[col]
                Q1 = series.quantile(0.25)
                Q3 = series.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                df[col] = df[col].clip(lower=lower_bound, upper=upper_bound)
        
        elif strategy == 'replace':
            # Replace outliers with median values
            for col, indices in outlier_indices.items():
                if col not in df.columns:
                    continue
                
                median_value = df[col].median()
                df.loc[indices, col] = median_value
        
        return df


class TimeSeriesProcessor:
    """Utility class for time series specific data processing."""
    
    @staticmethod
    def create_rolling_features(
        data: pd.DataFrame,
        columns: List[str],
        windows: List[int],
        operations: List[str] = None,
        min_periods: int = 1
    ) -> pd.DataFrame:
        """
        Create rolling window features.
        
        Args:
            data: Input dataframe
            columns: Columns to create rolling features for
            windows: List of window sizes
            operations: List of operations ('mean', 'std', 'min', 'max', 'median')
            min_periods: Minimum number of observations in window
            
        Returns:
            Dataframe with rolling features
        """
        if operations is None:
            operations = ['mean', 'std']
        
        df = data.copy()
        
        for col in columns:
            if col not in df.columns:
                continue
            
            for window in windows:
                for op in operations:
                    feature_name = f"{col}_rolling_{window}_{op}"
                    
                    if op == 'mean':
                        df[feature_name] = df[col].rolling(window=window, min_periods=min_periods).mean()
                    elif op == 'std':
                        df[feature_name] = df[col].rolling(window=window, min_periods=min_periods).std()
                    elif op == 'min':
                        df[feature_name] = df[col].rolling(window=window, min_periods=min_periods).min()
                    elif op == 'max':
                        df[feature_name] = df[col].rolling(window=window, min_periods=min_periods).max()
                    elif op == 'median':
                        df[feature_name] = df[col].rolling(window=window, min_periods=min_periods).median()
                    elif op == 'sum':
                        df[feature_name] = df[col].rolling(window=window, min_periods=min_periods).sum()
                    elif op == 'var':
                        df[feature_name] = df[col].rolling(window=window, min_periods=min_periods).var()
        
        return df
    
    @staticmethod
    def create_lag_features(
        data: pd.DataFrame,
        columns: List[str],
        lags: List[int]
    ) -> pd.DataFrame:
        """
        Create lag features for time series.
        
        Args:
            data: Input dataframe
            columns: Columns to create lag features for
            lags: List of lag values
            
        Returns:
            Dataframe with lag features
        """
        df = data.copy()
        
        for col in columns:
            if col not in df.columns:
                continue
            
            for lag in lags:
                feature_name = f"{col}_lag_{lag}"
                df[feature_name] = df[col].shift(lag)
        
        return df
    
    @staticmethod
    def create_difference_features(
        data: pd.DataFrame,
        columns: List[str],
        periods: List[int] = None
    ) -> pd.DataFrame:
        """
        Create difference features (first difference, etc.).
        
        Args:
            data: Input dataframe
            columns: Columns to create difference features for
            periods: List of periods for differencing
            
        Returns:
            Dataframe with difference features
        """
        if periods is None:
            periods = [1]
        
        df = data.copy()
        
        for col in columns:
            if col not in df.columns:
                continue
            
            for period in periods:
                feature_name = f"{col}_diff_{period}"
                df[feature_name] = df[col].diff(periods=period)
        
        return df
    
    @staticmethod
    def create_pct_change_features(
        data: pd.DataFrame,
        columns: List[str],
        periods: List[int] = None
    ) -> pd.DataFrame:
        """
        Create percentage change features.
        
        Args:
            data: Input dataframe
            columns: Columns to create pct change features for
            periods: List of periods for percentage change
            
        Returns:
            Dataframe with percentage change features
        """
        if periods is None:
            periods = [1]
        
        df = data.copy()
        
        for col in columns:
            if col not in df.columns:
                continue
            
            for period in periods:
                feature_name = f"{col}_pct_change_{period}"
                df[feature_name] = df[col].pct_change(periods=period)
        
        return df


class MarketDataProcessor:
    """Specialized processor for financial market data."""
    
    @staticmethod
    def validate_ohlcv_data(data: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate OHLCV market data format and quality.
        
        Args:
            data: DataFrame with OHLCV data
            
        Returns:
            Validation report
        """
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        
        validation_report = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'data_quality': {}
        }
        
        # Check required columns
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            validation_report['valid'] = False
            validation_report['errors'].append(f"Missing required columns: {missing_columns}")
        
        if not validation_report['valid']:
            return validation_report
        
        # Check data quality
        for col in required_columns:
            null_count = data[col].isnull().sum()
            if null_count > 0:
                validation_report['warnings'].append(f"Column '{col}' has {null_count} null values")
        
        # Check OHLC relationships
        invalid_ohlc = data[
            (data['high'] < data['low']) |
            (data['high'] < data['open']) |
            (data['high'] < data['close']) |
            (data['low'] > data['open']) |
            (data['low'] > data['close'])
        ]
        
        if len(invalid_ohlc) > 0:
            validation_report['warnings'].append(f"{len(invalid_ohlc)} rows have invalid OHLC relationships")
        
        # Check for negative values
        for col in ['open', 'high', 'low', 'close', 'volume']:
            negative_count = (data[col] < 0).sum()
            if negative_count > 0:
                validation_report['warnings'].append(f"Column '{col}' has {negative_count} negative values")
        
        # Data quality metrics
        validation_report['data_quality'] = {
            'total_rows': len(data),
            'date_range': {
                'start': data.index.min().isoformat() if hasattr(data.index, 'min') else None,
                'end': data.index.max().isoformat() if hasattr(data.index, 'max') else None
            },
            'completeness': {col: 1 - data[col].isnull().sum() / len(data) for col in required_columns}
        }
        
        return validation_report
    
    @staticmethod
    def calculate_returns(
        data: pd.DataFrame,
        price_column: str = 'close',
        return_types: List[str] = None
    ) -> pd.DataFrame:
        """
        Calculate various types of returns.
        
        Args:
            data: Input dataframe with price data
            price_column: Column name for price data
            return_types: Types of returns to calculate
            
        Returns:
            Dataframe with return features
        """
        if return_types is None:
            return_types = ['simple', 'log']
        
        df = data.copy()
        
        if 'simple' in return_types:
            df[f'{price_column}_return'] = df[price_column].pct_change()
        
        if 'log' in return_types:
            df[f'{price_column}_log_return'] = np.log(df[price_column] / df[price_column].shift(1))
        
        if 'cumulative' in return_types:
            df[f'{price_column}_cumulative_return'] = (1 + df[f'{price_column}_return']).cumprod() - 1
        
        return df
    
    @staticmethod
    def calculate_volatility(
        data: pd.DataFrame,
        price_column: str = 'close',
        windows: List[int] = None,
        return_column: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Calculate rolling volatility measures.
        
        Args:
            data: Input dataframe
            price_column: Column name for price data
            windows: List of rolling windows
            return_column: Pre-calculated return column name
            
        Returns:
            Dataframe with volatility features
        """
        if windows is None:
            windows = [10, 20, 30]
        
        df = data.copy()
        
        # Calculate returns if not provided
        if return_column is None:
            return_column = f'{price_column}_return'
            df[return_column] = df[price_column].pct_change()
        
        # Calculate rolling volatility
        for window in windows:
            vol_col = f'{price_column}_volatility_{window}'
            df[vol_col] = df[return_column].rolling(window=window).std()
            
            # Annualized volatility (assuming daily data)
            df[f'{vol_col}_annualized'] = df[vol_col] * np.sqrt(252)
        
        return df


class FeatureSelector:
    """Utility class for feature selection operations."""
    
    @staticmethod
    def remove_correlated_features(
        data: pd.DataFrame,
        correlation_threshold: float = 0.95,
        method: str = 'pearson'
    ) -> Tuple[pd.DataFrame, List[str]]:
        """
        Remove highly correlated features.
        
        Args:
            data: Input dataframe
            correlation_threshold: Threshold for correlation
            method: Correlation method
            
        Returns:
            Tuple of (filtered_data, removed_columns)
        """
        # Calculate correlation matrix
        corr_matrix = data.corr(method=method).abs()
        
        # Find pairs of highly correlated features
        upper_triangle = corr_matrix.where(
            np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
        )
        
        # Identify columns to remove
        to_remove = [
            column for column in upper_triangle.columns 
            if any(upper_triangle[column] > correlation_threshold)
        ]
        
        # Remove highly correlated features
        filtered_data = data.drop(columns=to_remove)
        
        logger.info(f"Removed {len(to_remove)} highly correlated features")
        return filtered_data, to_remove
    
    @staticmethod
    def remove_low_variance_features(
        data: pd.DataFrame,
        variance_threshold: float = 0.01
    ) -> Tuple[pd.DataFrame, List[str]]:
        """
        Remove features with low variance.
        
        Args:
            data: Input dataframe
            variance_threshold: Minimum variance threshold
            
        Returns:
            Tuple of (filtered_data, removed_columns)
        """
        # Calculate variance for numeric columns
        numeric_columns = data.select_dtypes(include=[np.number]).columns
        variances = data[numeric_columns].var()
        
        # Identify low variance columns
        low_variance_cols = variances[variances < variance_threshold].index.tolist()
        
        # Remove low variance features
        filtered_data = data.drop(columns=low_variance_cols)
        
        logger.info(f"Removed {len(low_variance_cols)} low variance features")
        return filtered_data, low_variance_cols


def create_market_data_pipeline(
    handle_missing: str = 'forward_fill',
    remove_outliers: bool = True,
    outlier_method: str = 'iqr',
    create_returns: bool = True,
    create_volatility: bool = True,
    create_rolling: bool = True
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """
    Create a data processing pipeline for market data.
    
    Args:
        handle_missing: Strategy for missing values
        remove_outliers: Whether to remove outliers
        outlier_method: Method for outlier detection
        create_returns: Whether to create return features
        create_volatility: Whether to create volatility features
        create_rolling: Whether to create rolling features
        
    Returns:
        Data processing pipeline function
    """
    def pipeline(data: pd.DataFrame) -> pd.DataFrame:
        """Apply the data processing pipeline."""
        logger.info("Starting market data processing pipeline")
        
        df = data.copy()
        
        # Validate data format
        processor = MarketDataProcessor()
        validation = processor.validate_ohlcv_data(df)
        if not validation['valid']:
            raise ValueError(f"Data validation failed: {validation['errors']}")
        
        # Handle missing values
        if handle_missing != 'none':
            df = DataProcessor.handle_missing_values(df, strategy=handle_missing)
        
        # Remove outliers
        if remove_outliers:
            outliers = DataProcessor.detect_outliers(df, method=outlier_method)
            if outliers:
                df = DataProcessor.remove_outliers(df, outliers, strategy='clip')
        
        # Create return features
        if create_returns:
            df = processor.calculate_returns(df)
        
        # Create volatility features
        if create_volatility:
            df = processor.calculate_volatility(df)
        
        # Create rolling features
        if create_rolling:
            price_columns = ['open', 'high', 'low', 'close']
            df = TimeSeriesProcessor.create_rolling_features(
                df, price_columns, windows=[5, 10, 20], 
                operations=['mean', 'std']
            )
        
        logger.info(f"Pipeline completed. Output shape: {df.shape}")
        return df
    
    return pipeline