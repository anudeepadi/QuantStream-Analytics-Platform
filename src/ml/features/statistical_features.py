"""
Statistical Features for QuantStream Analytics Platform

This module provides statistical feature engineering functions for
financial time series data used in anomaly detection.
"""

import logging
from typing import Dict, List, Optional, Tuple, Union, Callable
import numpy as np
import pandas as pd
from scipy import stats
from scipy.stats import skew, kurtosis
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)


class StatisticalFeatures:
    """Collection of statistical features for time series analysis."""
    
    @staticmethod
    def rolling_statistics(
        data: pd.Series,
        windows: List[int],
        statistics: List[str] = None,
        min_periods: int = 1
    ) -> pd.DataFrame:
        """
        Calculate rolling statistics for different window sizes.
        
        Args:
            data: Input time series
            windows: List of window sizes
            statistics: List of statistics to calculate
            min_periods: Minimum number of observations
            
        Returns:
            DataFrame with rolling statistics
        """
        if statistics is None:
            statistics = ['mean', 'std', 'min', 'max', 'median', 'quantile_25', 'quantile_75']
        
        results = {}
        
        for window in windows:
            rolling = data.rolling(window=window, min_periods=min_periods)
            
            for stat in statistics:
                col_name = f'{data.name}_{stat}_{window}' if data.name else f'{stat}_{window}'
                
                if stat == 'mean':
                    results[col_name] = rolling.mean()
                elif stat == 'std':
                    results[col_name] = rolling.std()
                elif stat == 'var':
                    results[col_name] = rolling.var()
                elif stat == 'min':
                    results[col_name] = rolling.min()
                elif stat == 'max':
                    results[col_name] = rolling.max()
                elif stat == 'median':
                    results[col_name] = rolling.median()
                elif stat == 'sum':
                    results[col_name] = rolling.sum()
                elif stat == 'quantile_25':
                    results[col_name] = rolling.quantile(0.25)
                elif stat == 'quantile_75':
                    results[col_name] = rolling.quantile(0.75)
                elif stat == 'skew':
                    results[col_name] = rolling.skew()
                elif stat == 'kurt':
                    results[col_name] = rolling.kurt()
        
        return pd.DataFrame(results, index=data.index)
    
    @staticmethod
    def rolling_z_score(
        data: pd.Series,
        window: int,
        min_periods: int = 1
    ) -> pd.Series:
        """
        Calculate rolling Z-score.
        
        Args:
            data: Input time series
            window: Rolling window size
            min_periods: Minimum number of observations
            
        Returns:
            Rolling Z-scores
        """
        rolling_mean = data.rolling(window=window, min_periods=min_periods).mean()
        rolling_std = data.rolling(window=window, min_periods=min_periods).std()
        
        z_score = (data - rolling_mean) / rolling_std
        return z_score
    
    @staticmethod
    def rolling_percentile_rank(
        data: pd.Series,
        window: int,
        min_periods: int = 1
    ) -> pd.Series:
        """
        Calculate rolling percentile rank.
        
        Args:
            data: Input time series
            window: Rolling window size
            min_periods: Minimum number of observations
            
        Returns:
            Percentile ranks (0-100)
        """
        def percentile_rank(x):
            return stats.percentileofscore(x[:-1], x[-1]) if len(x) > 1 else 50.0
        
        rolling_rank = data.rolling(window=window, min_periods=min_periods).apply(
            percentile_rank, raw=True
        )
        
        return rolling_rank
    
    @staticmethod
    def rolling_entropy(
        data: pd.Series,
        window: int,
        bins: int = 10,
        min_periods: int = 1
    ) -> pd.Series:
        """
        Calculate rolling entropy of the data distribution.
        
        Args:
            data: Input time series
            window: Rolling window size
            bins: Number of histogram bins
            min_periods: Minimum number of observations
            
        Returns:
            Rolling entropy values
        """
        def entropy(x):
            if len(x) < 2:
                return 0.0
            
            hist, _ = np.histogram(x, bins=bins)
            hist = hist[hist > 0]  # Remove zero counts
            prob = hist / hist.sum()
            return -np.sum(prob * np.log2(prob))
        
        rolling_entropy = data.rolling(window=window, min_periods=min_periods).apply(
            entropy, raw=True
        )
        
        return rolling_entropy
    
    @staticmethod
    def rolling_correlation(
        data1: pd.Series,
        data2: pd.Series,
        window: int,
        min_periods: int = 1
    ) -> pd.Series:
        """
        Calculate rolling correlation between two series.
        
        Args:
            data1: First time series
            data2: Second time series
            window: Rolling window size
            min_periods: Minimum number of observations
            
        Returns:
            Rolling correlations
        """
        return data1.rolling(window=window, min_periods=min_periods).corr(data2)
    
    @staticmethod
    def rolling_beta(
        returns: pd.Series,
        market_returns: pd.Series,
        window: int,
        min_periods: int = 1
    ) -> pd.Series:
        """
        Calculate rolling beta (systematic risk measure).
        
        Args:
            returns: Asset returns
            market_returns: Market returns
            window: Rolling window size
            min_periods: Minimum number of observations
            
        Returns:
            Rolling beta values
        """
        def calculate_beta(asset_ret, market_ret):
            if len(asset_ret) < 2 or len(market_ret) < 2:
                return np.nan
            
            covariance = np.cov(asset_ret, market_ret)[0, 1]
            market_variance = np.var(market_ret)
            
            if market_variance == 0:
                return np.nan
            
            return covariance / market_variance
        
        rolling_beta = returns.rolling(window=window, min_periods=min_periods).apply(
            lambda x: calculate_beta(x.values, market_returns.loc[x.index].values),
            raw=False
        )
        
        return rolling_beta


class VolatilityFeatures:
    """Volatility-related statistical features."""
    
    @staticmethod
    def realized_volatility(
        returns: pd.Series,
        window: int,
        annualization_factor: int = 252
    ) -> pd.Series:
        """
        Calculate realized volatility.
        
        Args:
            returns: Return series
            window: Rolling window
            annualization_factor: Factor for annualization
            
        Returns:
            Realized volatility
        """
        return returns.rolling(window=window).std() * np.sqrt(annualization_factor)
    
    @staticmethod
    def garch_volatility(
        returns: pd.Series,
        window: int = 252,
        alpha: float = 0.1,
        beta: float = 0.85
    ) -> pd.Series:
        """
        GARCH(1,1) volatility estimate.
        
        Args:
            returns: Return series
            window: Window for initial variance estimate
            alpha: GARCH alpha parameter
            beta: GARCH beta parameter
            
        Returns:
            GARCH volatility estimates
        """
        # Initialize variance with rolling variance
        variance = returns.rolling(window=window).var()
        garch_var = variance.copy()
        
        # GARCH recursion
        for i in range(window, len(returns)):
            garch_var.iloc[i] = (
                alpha * returns.iloc[i-1]**2 +
                beta * garch_var.iloc[i-1] +
                (1 - alpha - beta) * variance.iloc[i]
            )
        
        return np.sqrt(garch_var * 252)  # Annualized volatility
    
    @staticmethod
    def volatility_of_volatility(
        returns: pd.Series,
        vol_window: int = 20,
        volvol_window: int = 20
    ) -> pd.Series:
        """
        Calculate volatility of volatility (second-order volatility).
        
        Args:
            returns: Return series
            vol_window: Window for volatility calculation
            volvol_window: Window for vol-of-vol calculation
            
        Returns:
            Volatility of volatility
        """
        volatility = returns.rolling(window=vol_window).std()
        vol_of_vol = volatility.rolling(window=volvol_window).std()
        
        return vol_of_vol
    
    @staticmethod
    def parkinson_volatility(
        high: pd.Series,
        low: pd.Series,
        window: int = 20
    ) -> pd.Series:
        """
        Parkinson volatility estimator using high-low range.
        
        Args:
            high: High price series
            low: Low price series
            window: Rolling window
            
        Returns:
            Parkinson volatility estimates
        """
        log_hl_ratio = np.log(high / low)
        parkinson_var = (log_hl_ratio**2).rolling(window=window).mean() / (4 * np.log(2))
        
        return np.sqrt(parkinson_var * 252)  # Annualized


class MomentumFeatures:
    """Momentum and mean reversion features."""
    
    @staticmethod
    def momentum_indicators(
        data: pd.Series,
        periods: List[int]
    ) -> pd.DataFrame:
        """
        Calculate various momentum indicators.
        
        Args:
            data: Price series
            periods: List of lookback periods
            
        Returns:
            DataFrame with momentum indicators
        """
        results = {}
        
        for period in periods:
            # Simple momentum (price change)
            results[f'momentum_{period}'] = data.diff(periods=period)
            
            # Rate of change (percentage change)
            results[f'roc_{period}'] = data.pct_change(periods=period)
            
            # Log return
            results[f'log_return_{period}'] = np.log(data / data.shift(period))
            
            # Normalized momentum (z-score)
            momentum = data.diff(periods=period)
            results[f'momentum_zscore_{period}'] = (
                momentum - momentum.rolling(window=50).mean()
            ) / momentum.rolling(window=50).std()
        
        return pd.DataFrame(results, index=data.index)
    
    @staticmethod
    def mean_reversion_features(
        data: pd.Series,
        lookback_periods: List[int]
    ) -> pd.DataFrame:
        """
        Calculate mean reversion features.
        
        Args:
            data: Price series
            lookback_periods: List of lookback periods
            
        Returns:
            DataFrame with mean reversion features
        """
        results = {}
        
        for period in lookback_periods:
            # Distance from moving average
            ma = data.rolling(window=period).mean()
            results[f'distance_from_ma_{period}'] = (data - ma) / ma
            
            # Percentile position within lookback window
            results[f'percentile_rank_{period}'] = StatisticalFeatures.rolling_percentile_rank(
                data, window=period
            )
            
            # RSI-like oscillator
            changes = data.diff()
            gains = changes.where(changes > 0, 0)
            losses = -changes.where(changes < 0, 0)
            
            avg_gain = gains.rolling(window=period).mean()
            avg_loss = losses.rolling(window=period).mean()
            
            rs = avg_gain / avg_loss
            results[f'rsi_{period}'] = 100 - (100 / (1 + rs))
        
        return pd.DataFrame(results, index=data.index)


class HigherMomentFeatures:
    """Higher-order moment features (skewness, kurtosis, etc.)."""
    
    @staticmethod
    def rolling_moments(
        data: pd.Series,
        windows: List[int],
        moments: List[str] = None
    ) -> pd.DataFrame:
        """
        Calculate rolling higher-order moments.
        
        Args:
            data: Input time series
            windows: List of window sizes
            moments: List of moments to calculate
            
        Returns:
            DataFrame with rolling moments
        """
        if moments is None:
            moments = ['skew', 'kurt']
        
        results = {}
        
        for window in windows:
            rolling = data.rolling(window=window, min_periods=max(4, window//4))
            
            if 'skew' in moments:
                results[f'skew_{window}'] = rolling.skew()
            
            if 'kurt' in moments:
                results[f'kurt_{window}'] = rolling.kurt()
            
            # Custom higher moments
            if 'moment_3' in moments:
                results[f'moment_3_{window}'] = rolling.apply(
                    lambda x: stats.moment(x, moment=3), raw=True
                )
            
            if 'moment_4' in moments:
                results[f'moment_4_{window}'] = rolling.apply(
                    lambda x: stats.moment(x, moment=4), raw=True
                )
        
        return pd.DataFrame(results, index=data.index)
    
    @staticmethod
    def distribution_features(
        data: pd.Series,
        window: int = 50
    ) -> pd.DataFrame:
        """
        Calculate distribution-related features.
        
        Args:
            data: Input time series
            window: Rolling window size
            
        Returns:
            DataFrame with distribution features
        """
        results = {}
        rolling = data.rolling(window=window, min_periods=max(10, window//5))
        
        # Basic moments
        results['mean'] = rolling.mean()
        results['std'] = rolling.std()
        results['var'] = rolling.var()
        results['skew'] = rolling.skew()
        results['kurt'] = rolling.kurt()
        
        # Quantile-based features
        results['iqr'] = rolling.quantile(0.75) - rolling.quantile(0.25)
        results['quantile_range_90'] = rolling.quantile(0.95) - rolling.quantile(0.05)
        
        # Tail measures
        results['left_tail'] = rolling.quantile(0.05)
        results['right_tail'] = rolling.quantile(0.95)
        
        # Coefficient of variation
        results['cv'] = results['std'] / np.abs(results['mean'])
        
        return pd.DataFrame(results, index=data.index)


class AutocorrelationFeatures:
    """Autocorrelation and serial dependence features."""
    
    @staticmethod
    def rolling_autocorrelations(
        data: pd.Series,
        lags: List[int],
        window: int = 50
    ) -> pd.DataFrame:
        """
        Calculate rolling autocorrelations at different lags.
        
        Args:
            data: Input time series
            lags: List of lag values
            window: Rolling window size
            
        Returns:
            DataFrame with autocorrelation features
        """
        results = {}
        
        for lag in lags:
            def autocorr(x):
                if len(x) <= lag + 1:
                    return np.nan
                return np.corrcoef(x[:-lag], x[lag:])[0, 1]
            
            results[f'autocorr_lag_{lag}'] = data.rolling(
                window=window, min_periods=lag+10
            ).apply(autocorr, raw=True)
        
        return pd.DataFrame(results, index=data.index)
    
    @staticmethod
    def ljung_box_statistic(
        data: pd.Series,
        lags: int = 10,
        window: int = 100
    ) -> pd.Series:
        """
        Rolling Ljung-Box test statistic for serial correlation.
        
        Args:
            data: Input time series
            lags: Number of lags to test
            window: Rolling window size
            
        Returns:
            Ljung-Box statistics
        """
        def ljung_box(x):
            if len(x) <= 2 * lags:
                return np.nan
            
            try:
                # Simple Ljung-Box statistic calculation
                n = len(x)
                acf_values = []
                
                for k in range(1, lags + 1):
                    if len(x) > k:
                        acf_k = np.corrcoef(x[:-k], x[k:])[0, 1]
                        if not np.isnan(acf_k):
                            acf_values.append(acf_k**2 / (n - k))
                
                if acf_values:
                    return n * (n + 2) * sum(acf_values)
                else:
                    return np.nan
                    
            except:
                return np.nan
        
        return data.rolling(window=window, min_periods=2*lags).apply(
            ljung_box, raw=True
        )


def create_comprehensive_statistical_features(
    data: pd.DataFrame,
    price_columns: List[str] = None,
    volume_column: str = 'volume',
    windows: List[int] = None,
    include_higher_moments: bool = True,
    include_autocorr: bool = True,
    include_volatility: bool = True
) -> pd.DataFrame:
    """
    Create comprehensive statistical features for anomaly detection.
    
    Args:
        data: Input DataFrame with OHLCV data
        price_columns: List of price column names
        volume_column: Volume column name
        windows: List of rolling windows
        include_higher_moments: Whether to include skewness/kurtosis
        include_autocorr: Whether to include autocorrelation features
        include_volatility: Whether to include volatility features
        
    Returns:
        DataFrame with statistical features
    """
    if price_columns is None:
        price_columns = ['close']
    
    if windows is None:
        windows = [5, 10, 20, 50]
    
    logger.info("Creating comprehensive statistical features")
    
    # Collect all features
    all_features = []
    
    for col in price_columns:
        if col not in data.columns:
            continue
        
        series = data[col]
        
        # Basic rolling statistics
        rolling_stats = StatisticalFeatures.rolling_statistics(
            series, windows=windows
        )
        all_features.append(rolling_stats)
        
        # Z-scores for different windows
        for window in windows:
            z_score = StatisticalFeatures.rolling_z_score(series, window)
            z_score.name = f'{col}_zscore_{window}'
            all_features.append(z_score.to_frame())
        
        # Momentum features
        momentum_feat = MomentumFeatures.momentum_indicators(
            series, periods=[1, 5, 10, 20]
        )
        momentum_feat.columns = [f'{col}_{c}' for c in momentum_feat.columns]
        all_features.append(momentum_feat)
        
        # Mean reversion features
        mean_rev_feat = MomentumFeatures.mean_reversion_features(
            series, lookback_periods=windows
        )
        mean_rev_feat.columns = [f'{col}_{c}' for c in mean_rev_feat.columns]
        all_features.append(mean_rev_feat)
        
        # Higher moments
        if include_higher_moments:
            higher_moments = HigherMomentFeatures.rolling_moments(
                series, windows=windows
            )
            higher_moments.columns = [f'{col}_{c}' for c in higher_moments.columns]
            all_features.append(higher_moments)
        
        # Distribution features
        dist_features = HigherMomentFeatures.distribution_features(series)
        dist_features.columns = [f'{col}_{c}' for c in dist_features.columns]
        all_features.append(dist_features)
        
        # Autocorrelation features
        if include_autocorr:
            autocorr_features = AutocorrelationFeatures.rolling_autocorrelations(
                series, lags=[1, 2, 5, 10], window=50
            )
            autocorr_features.columns = [f'{col}_{c}' for c in autocorr_features.columns]
            all_features.append(autocorr_features)
    
    # Returns-based features
    if 'close' in data.columns:
        returns = data['close'].pct_change()
        
        if include_volatility:
            # Volatility features
            vol_features = []
            
            for window in windows:
                realized_vol = VolatilityFeatures.realized_volatility(returns, window)
                realized_vol.name = f'realized_vol_{window}'
                vol_features.append(realized_vol.to_frame())
            
            # Vol of vol
            volvol = VolatilityFeatures.volatility_of_volatility(returns)
            volvol.name = 'vol_of_vol'
            vol_features.append(volvol.to_frame())
            
            all_features.extend(vol_features)
    
    # Volume features if available
    if volume_column in data.columns:
        volume_series = data[volume_column]
        
        # Volume statistics
        vol_stats = StatisticalFeatures.rolling_statistics(
            volume_series, windows=windows, statistics=['mean', 'std', 'max']
        )
        vol_stats.columns = [f'volume_{c}' for c in vol_stats.columns]
        all_features.append(vol_stats)
        
        # Volume Z-score
        for window in windows:
            vol_zscore = StatisticalFeatures.rolling_z_score(volume_series, window)
            vol_zscore.name = f'volume_zscore_{window}'
            all_features.append(vol_zscore.to_frame())
    
    # Combine all features
    if all_features:
        result = pd.concat(all_features, axis=1)
        logger.info(f"Created {result.shape[1]} statistical features")
        return result
    else:
        logger.warning("No statistical features were created")
        return pd.DataFrame(index=data.index)