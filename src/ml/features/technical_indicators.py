"""
Technical Indicators for QuantStream Analytics Platform

This module provides implementations of common technical indicators
used in financial market analysis and anomaly detection.
"""

import logging
from typing import Dict, List, Optional, Tuple, Union
import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)


class TechnicalIndicators:
    """Collection of technical indicators for financial market data."""
    
    @staticmethod
    def sma(data: pd.Series, period: int) -> pd.Series:
        """
        Simple Moving Average.
        
        Args:
            data: Price series
            period: Moving average period
            
        Returns:
            Simple moving average
        """
        return data.rolling(window=period, min_periods=1).mean()
    
    @staticmethod
    def ema(data: pd.Series, period: int) -> pd.Series:
        """
        Exponential Moving Average.
        
        Args:
            data: Price series
            period: EMA period
            
        Returns:
            Exponential moving average
        """
        return data.ewm(span=period).mean()
    
    @staticmethod
    def rsi(data: pd.Series, period: int = 14) -> pd.Series:
        """
        Relative Strength Index.
        
        Args:
            data: Price series
            period: RSI period
            
        Returns:
            RSI values (0-100)
        """
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    @staticmethod
    def macd(
        data: pd.Series, 
        fast_period: int = 12, 
        slow_period: int = 26, 
        signal_period: int = 9
    ) -> Dict[str, pd.Series]:
        """
        MACD (Moving Average Convergence Divergence).
        
        Args:
            data: Price series
            fast_period: Fast EMA period
            slow_period: Slow EMA period
            signal_period: Signal line EMA period
            
        Returns:
            Dictionary with MACD line, signal line, and histogram
        """
        fast_ema = TechnicalIndicators.ema(data, fast_period)
        slow_ema = TechnicalIndicators.ema(data, slow_period)
        
        macd_line = fast_ema - slow_ema
        signal_line = TechnicalIndicators.ema(macd_line, signal_period)
        histogram = macd_line - signal_line
        
        return {
            'macd': macd_line,
            'signal': signal_line,
            'histogram': histogram
        }
    
    @staticmethod
    def bollinger_bands(
        data: pd.Series, 
        period: int = 20, 
        std_dev: float = 2.0
    ) -> Dict[str, pd.Series]:
        """
        Bollinger Bands.
        
        Args:
            data: Price series
            period: Moving average period
            std_dev: Standard deviation multiplier
            
        Returns:
            Dictionary with upper, middle, and lower bands
        """
        middle_band = TechnicalIndicators.sma(data, period)
        std = data.rolling(window=period).std()
        
        upper_band = middle_band + (std * std_dev)
        lower_band = middle_band - (std * std_dev)
        
        return {
            'upper': upper_band,
            'middle': middle_band,
            'lower': lower_band
        }
    
    @staticmethod
    def stochastic_oscillator(
        high: pd.Series, 
        low: pd.Series, 
        close: pd.Series,
        k_period: int = 14, 
        d_period: int = 3
    ) -> Dict[str, pd.Series]:
        """
        Stochastic Oscillator.
        
        Args:
            high: High price series
            low: Low price series
            close: Close price series
            k_period: %K period
            d_period: %D period
            
        Returns:
            Dictionary with %K and %D values
        """
        lowest_low = low.rolling(window=k_period).min()
        highest_high = high.rolling(window=k_period).max()
        
        k_percent = 100 * (close - lowest_low) / (highest_high - lowest_low)
        d_percent = k_percent.rolling(window=d_period).mean()
        
        return {
            'k_percent': k_percent,
            'd_percent': d_percent
        }
    
    @staticmethod
    def atr(
        high: pd.Series, 
        low: pd.Series, 
        close: pd.Series, 
        period: int = 14
    ) -> pd.Series:
        """
        Average True Range.
        
        Args:
            high: High price series
            low: Low price series
            close: Close price series
            period: ATR period
            
        Returns:
            Average True Range values
        """
        prev_close = close.shift(1)
        
        true_range = np.maximum(
            high - low,
            np.maximum(
                np.abs(high - prev_close),
                np.abs(low - prev_close)
            )
        )
        
        return pd.Series(true_range).rolling(window=period).mean()
    
    @staticmethod
    def adx(
        high: pd.Series, 
        low: pd.Series, 
        close: pd.Series, 
        period: int = 14
    ) -> Dict[str, pd.Series]:
        """
        Average Directional Index.
        
        Args:
            high: High price series
            low: Low price series
            close: Close price series
            period: ADX period
            
        Returns:
            Dictionary with ADX, +DI, and -DI values
        """
        # True Range
        atr_values = TechnicalIndicators.atr(high, low, close, period)
        
        # Directional Movement
        up_move = high.diff()
        down_move = -low.diff()
        
        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)
        
        plus_dm = pd.Series(plus_dm, index=high.index).rolling(window=period).mean()
        minus_dm = pd.Series(minus_dm, index=high.index).rolling(window=period).mean()
        
        # Directional Indicators
        plus_di = 100 * plus_dm / atr_values
        minus_di = 100 * minus_dm / atr_values
        
        # ADX
        dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di)
        adx = dx.rolling(window=period).mean()
        
        return {
            'adx': adx,
            'plus_di': plus_di,
            'minus_di': minus_di
        }
    
    @staticmethod
    def cci(
        high: pd.Series, 
        low: pd.Series, 
        close: pd.Series, 
        period: int = 20
    ) -> pd.Series:
        """
        Commodity Channel Index.
        
        Args:
            high: High price series
            low: Low price series
            close: Close price series
            period: CCI period
            
        Returns:
            CCI values
        """
        typical_price = (high + low + close) / 3
        sma_tp = typical_price.rolling(window=period).mean()
        mean_deviation = typical_price.rolling(window=period).apply(
            lambda x: np.mean(np.abs(x - x.mean()))
        )
        
        cci = (typical_price - sma_tp) / (0.015 * mean_deviation)
        return cci
    
    @staticmethod
    def williams_r(
        high: pd.Series, 
        low: pd.Series, 
        close: pd.Series, 
        period: int = 14
    ) -> pd.Series:
        """
        Williams %R.
        
        Args:
            high: High price series
            low: Low price series
            close: Close price series
            period: Williams %R period
            
        Returns:
            Williams %R values
        """
        highest_high = high.rolling(window=period).max()
        lowest_low = low.rolling(window=period).min()
        
        williams_r = -100 * (highest_high - close) / (highest_high - lowest_low)
        return williams_r
    
    @staticmethod
    def momentum(data: pd.Series, period: int = 10) -> pd.Series:
        """
        Price Momentum.
        
        Args:
            data: Price series
            period: Momentum period
            
        Returns:
            Momentum values
        """
        return data.diff(periods=period)
    
    @staticmethod
    def rate_of_change(data: pd.Series, period: int = 10) -> pd.Series:
        """
        Rate of Change (ROC).
        
        Args:
            data: Price series
            period: ROC period
            
        Returns:
            Rate of change values (percentage)
        """
        return data.pct_change(periods=period) * 100
    
    @staticmethod
    def price_volume_trend(close: pd.Series, volume: pd.Series) -> pd.Series:
        """
        Price Volume Trend.
        
        Args:
            close: Close price series
            volume: Volume series
            
        Returns:
            PVT values
        """
        price_change_pct = close.pct_change()
        pvt = (price_change_pct * volume).cumsum()
        return pvt
    
    @staticmethod
    def on_balance_volume(close: pd.Series, volume: pd.Series) -> pd.Series:
        """
        On Balance Volume.
        
        Args:
            close: Close price series
            volume: Volume series
            
        Returns:
            OBV values
        """
        price_change = close.diff()
        obv = np.where(price_change > 0, volume,
                      np.where(price_change < 0, -volume, 0))
        return pd.Series(obv, index=close.index).cumsum()


class AdvancedTechnicalIndicators:
    """Advanced technical indicators for sophisticated analysis."""
    
    @staticmethod
    def ichimoku_cloud(
        high: pd.Series, 
        low: pd.Series, 
        close: pd.Series,
        conversion_period: int = 9,
        base_period: int = 26,
        leading_span_b_period: int = 52,
        displacement: int = 26
    ) -> Dict[str, pd.Series]:
        """
        Ichimoku Cloud components.
        
        Args:
            high: High price series
            low: Low price series
            close: Close price series
            conversion_period: Conversion line period
            base_period: Base line period
            leading_span_b_period: Leading Span B period
            displacement: Displacement for leading spans
            
        Returns:
            Dictionary with Ichimoku components
        """
        # Conversion Line (Tenkan-sen)
        conversion_line = (
            high.rolling(window=conversion_period).max() + 
            low.rolling(window=conversion_period).min()
        ) / 2
        
        # Base Line (Kijun-sen)
        base_line = (
            high.rolling(window=base_period).max() + 
            low.rolling(window=base_period).min()
        ) / 2
        
        # Leading Span A (Senkou Span A)
        leading_span_a = ((conversion_line + base_line) / 2).shift(displacement)
        
        # Leading Span B (Senkou Span B)
        leading_span_b = (
            high.rolling(window=leading_span_b_period).max() + 
            low.rolling(window=leading_span_b_period).min()
        ) / 2
        leading_span_b = leading_span_b.shift(displacement)
        
        # Lagging Span (Chikou Span)
        lagging_span = close.shift(-displacement)
        
        return {
            'conversion_line': conversion_line,
            'base_line': base_line,
            'leading_span_a': leading_span_a,
            'leading_span_b': leading_span_b,
            'lagging_span': lagging_span
        }
    
    @staticmethod
    def vortex_indicator(
        high: pd.Series, 
        low: pd.Series, 
        close: pd.Series, 
        period: int = 14
    ) -> Dict[str, pd.Series]:
        """
        Vortex Indicator.
        
        Args:
            high: High price series
            low: Low price series
            close: Close price series
            period: VI period
            
        Returns:
            Dictionary with VI+ and VI- values
        """
        prev_close = close.shift(1)
        
        vm_plus = np.abs(high - low.shift(1))
        vm_minus = np.abs(low - high.shift(1))
        
        true_range = np.maximum(
            high - low,
            np.maximum(
                np.abs(high - prev_close),
                np.abs(low - prev_close)
            )
        )
        
        vi_plus = (
            pd.Series(vm_plus, index=high.index).rolling(window=period).sum() /
            pd.Series(true_range, index=high.index).rolling(window=period).sum()
        )
        
        vi_minus = (
            pd.Series(vm_minus, index=high.index).rolling(window=period).sum() /
            pd.Series(true_range, index=high.index).rolling(window=period).sum()
        )
        
        return {
            'vi_plus': vi_plus,
            'vi_minus': vi_minus
        }
    
    @staticmethod
    def keltner_channels(
        high: pd.Series, 
        low: pd.Series, 
        close: pd.Series,
        period: int = 20, 
        atr_period: int = 10, 
        multiplier: float = 2.0
    ) -> Dict[str, pd.Series]:
        """
        Keltner Channels.
        
        Args:
            high: High price series
            low: Low price series
            close: Close price series
            period: EMA period for center line
            atr_period: ATR period
            multiplier: ATR multiplier
            
        Returns:
            Dictionary with upper, center, and lower channels
        """
        center_line = TechnicalIndicators.ema(close, period)
        atr_values = TechnicalIndicators.atr(high, low, close, atr_period)
        
        upper_channel = center_line + (multiplier * atr_values)
        lower_channel = center_line - (multiplier * atr_values)
        
        return {
            'upper': upper_channel,
            'center': center_line,
            'lower': lower_channel
        }
    
    @staticmethod
    def parabolic_sar(
        high: pd.Series, 
        low: pd.Series, 
        af_start: float = 0.02, 
        af_increment: float = 0.02, 
        af_max: float = 0.2
    ) -> pd.Series:
        """
        Parabolic SAR.
        
        Args:
            high: High price series
            low: Low price series
            af_start: Starting acceleration factor
            af_increment: AF increment
            af_max: Maximum AF
            
        Returns:
            Parabolic SAR values
        """
        length = len(high)
        psar = np.zeros(length)
        af = af_start
        ep = low.iloc[0]
        trend = 1  # 1 for uptrend, -1 for downtrend
        
        psar[0] = high.iloc[0]
        
        for i in range(1, length):
            if trend == 1:  # Uptrend
                psar[i] = psar[i-1] + af * (ep - psar[i-1])
                
                if high.iloc[i] > ep:
                    ep = high.iloc[i]
                    af = min(af + af_increment, af_max)
                
                if low.iloc[i] <= psar[i]:
                    trend = -1
                    psar[i] = ep
                    af = af_start
                    ep = low.iloc[i]
            
            else:  # Downtrend
                psar[i] = psar[i-1] - af * (psar[i-1] - ep)
                
                if low.iloc[i] < ep:
                    ep = low.iloc[i]
                    af = min(af + af_increment, af_max)
                
                if high.iloc[i] >= psar[i]:
                    trend = 1
                    psar[i] = ep
                    af = af_start
                    ep = high.iloc[i]
        
        return pd.Series(psar, index=high.index)


class VolumeIndicators:
    """Volume-based technical indicators."""
    
    @staticmethod
    def volume_sma(volume: pd.Series, period: int = 20) -> pd.Series:
        """Volume Simple Moving Average."""
        return volume.rolling(window=period).mean()
    
    @staticmethod
    def volume_weighted_average_price(
        high: pd.Series, 
        low: pd.Series, 
        close: pd.Series, 
        volume: pd.Series
    ) -> pd.Series:
        """
        Volume Weighted Average Price.
        
        Args:
            high: High price series
            low: Low price series
            close: Close price series
            volume: Volume series
            
        Returns:
            VWAP values
        """
        typical_price = (high + low + close) / 3
        return (typical_price * volume).cumsum() / volume.cumsum()
    
    @staticmethod
    def accumulation_distribution_line(
        high: pd.Series, 
        low: pd.Series, 
        close: pd.Series, 
        volume: pd.Series
    ) -> pd.Series:
        """
        Accumulation/Distribution Line.
        
        Args:
            high: High price series
            low: Low price series
            close: Close price series
            volume: Volume series
            
        Returns:
            A/D Line values
        """
        money_flow_multiplier = ((close - low) - (high - close)) / (high - low)
        money_flow_volume = money_flow_multiplier * volume
        return money_flow_volume.cumsum()
    
    @staticmethod
    def chaikin_money_flow(
        high: pd.Series, 
        low: pd.Series, 
        close: pd.Series, 
        volume: pd.Series, 
        period: int = 20
    ) -> pd.Series:
        """
        Chaikin Money Flow.
        
        Args:
            high: High price series
            low: Low price series
            close: Close price series
            volume: Volume series
            period: CMF period
            
        Returns:
            CMF values
        """
        money_flow_multiplier = ((close - low) - (high - close)) / (high - low)
        money_flow_volume = money_flow_multiplier * volume
        
        cmf = (
            money_flow_volume.rolling(window=period).sum() /
            volume.rolling(window=period).sum()
        )
        
        return cmf