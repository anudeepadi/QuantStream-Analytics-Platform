"""
Featurized Technical Indicators

Transforms existing technical indicators into feature store-compatible
computation functions with metadata and validation.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Callable, Union
from datetime import datetime, timezone
import logging

# Import existing technical indicators
from ...ml.features.technical_indicators import (
    TechnicalIndicators, 
    AdvancedTechnicalIndicators, 
    VolumeIndicators
)
from ..store.feature_metadata import (
    FeatureMetadata,
    FeatureSchema,
    FeatureType,
    IndicatorCategory,
    FeatureVersion
)


logger = logging.getLogger(__name__)


class FeaturizedIndicators:
    """
    Wrapper class that converts technical indicators into feature store
    compatible computation functions with standardized interfaces.
    """
    
    @staticmethod
    def create_feature_function(
        indicator_func: Callable,
        output_columns: List[str],
        required_columns: List[str],
        parameters: Optional[Dict[str, Any]] = None
    ) -> Callable:
        """
        Create a standardized feature computation function.
        
        Args:
            indicator_func: Original indicator function
            output_columns: Names of output columns
            required_columns: Required input columns
            parameters: Default parameters
            
        Returns:
            Standardized feature computation function
        """
        def compute_feature(
            data: pd.DataFrame,
            **kwargs
        ) -> pd.DataFrame:
            """Standardized feature computation function."""
            try:
                # Validate required columns
                missing_cols = [col for col in required_columns if col not in data.columns]
                if missing_cols:
                    raise ValueError(f"Missing required columns: {missing_cols}")
                
                # Merge parameters
                params = (parameters or {}).copy()
                params.update(kwargs)
                
                # Call indicator function based on signature
                if len(required_columns) == 1:
                    # Single column input (price series)
                    result = indicator_func(data[required_columns[0]], **params)
                elif len(required_columns) == 2:
                    # Two column input (e.g., close, volume)
                    result = indicator_func(
                        data[required_columns[0]], 
                        data[required_columns[1]], 
                        **params
                    )
                elif len(required_columns) == 3:
                    # Three column input (high, low, close)
                    result = indicator_func(
                        data[required_columns[0]], 
                        data[required_columns[1]], 
                        data[required_columns[2]], 
                        **params
                    )
                elif len(required_columns) == 4:
                    # Four column input (high, low, close, volume)
                    result = indicator_func(
                        data[required_columns[0]], 
                        data[required_columns[1]], 
                        data[required_columns[2]], 
                        data[required_columns[3]], 
                        **params
                    )
                else:
                    raise ValueError(f"Unsupported number of required columns: {len(required_columns)}")
                
                # Format output
                if isinstance(result, dict):
                    # Multi-output indicator (e.g., MACD, Bollinger Bands)
                    output_df = data[['timestamp'] + [col for col in ['entity_id', 'symbol'] if col in data.columns]].copy()
                    
                    for i, col_name in enumerate(output_columns):
                        if col_name in result:
                            output_df[col_name] = result[col_name]
                        elif i < len(result):
                            # Fallback: use positional mapping
                            key = list(result.keys())[i]
                            output_df[col_name] = result[key]
                    
                    return output_df
                
                elif isinstance(result, pd.Series):
                    # Single output indicator
                    output_df = data[['timestamp'] + [col for col in ['entity_id', 'symbol'] if col in data.columns]].copy()
                    output_df[output_columns[0]] = result
                    return output_df
                
                else:
                    raise ValueError(f"Unexpected result type: {type(result)}")
                
            except Exception as e:
                logger.error(f"Error computing feature: {e}")
                raise
        
        return compute_feature


# Feature creation functions
def create_sma_feature(period: int = 20) -> Tuple[FeatureMetadata, Callable]:
    """Create Simple Moving Average feature."""
    feature_id = f"sma_{period}"
    
    metadata = FeatureMetadata(
        feature_id=feature_id,
        name=f"Simple Moving Average ({period})",
        namespace="technical_indicators",
        version="1.0.0",
        schema=FeatureSchema(
            name="sma_value",
            feature_type=FeatureType.FLOAT,
            description=f"{period}-period simple moving average"
        ),
        category=IndicatorCategory.TREND,
        window_size=period,
        parameters={"period": period},
        description=f"Simple moving average over {period} periods",
        calculation_logic=f"SMA = sum(close_prices[t-{period+1}:t]) / {period}",
        data_source="market_data.close"
    )
    
    computation_func = FeaturizedIndicators.create_feature_function(
        indicator_func=TechnicalIndicators.sma,
        output_columns=["sma_value"],
        required_columns=["close"],
        parameters={"period": period}
    )
    
    return metadata, computation_func


def create_ema_feature(period: int = 20) -> Tuple[FeatureMetadata, Callable]:
    """Create Exponential Moving Average feature."""
    feature_id = f"ema_{period}"
    
    metadata = FeatureMetadata(
        feature_id=feature_id,
        name=f"Exponential Moving Average ({period})",
        namespace="technical_indicators",
        version="1.0.0",
        schema=FeatureSchema(
            name="ema_value",
            feature_type=FeatureType.FLOAT,
            description=f"{period}-period exponential moving average"
        ),
        category=IndicatorCategory.TREND,
        window_size=period,
        parameters={"period": period},
        description=f"Exponential moving average over {period} periods",
        calculation_logic=f"EMA = alpha * price + (1 - alpha) * EMA_prev, where alpha = 2/{period+1}",
        data_source="market_data.close"
    )
    
    computation_func = FeaturizedIndicators.create_feature_function(
        indicator_func=TechnicalIndicators.ema,
        output_columns=["ema_value"],
        required_columns=["close"],
        parameters={"period": period}
    )
    
    return metadata, computation_func


def create_rsi_feature(period: int = 14) -> Tuple[FeatureMetadata, Callable]:
    """Create Relative Strength Index feature."""
    feature_id = f"rsi_{period}"
    
    metadata = FeatureMetadata(
        feature_id=feature_id,
        name=f"Relative Strength Index ({period})",
        namespace="technical_indicators",
        version="1.0.0",
        schema=FeatureSchema(
            name="rsi_value",
            feature_type=FeatureType.FLOAT,
            description=f"{period}-period RSI oscillator (0-100)",
            constraints={"min_value": 0, "max_value": 100}
        ),
        category=IndicatorCategory.MOMENTUM,
        window_size=period + 1,  # Needs one extra for diff calculation
        parameters={"period": period},
        description=f"RSI momentum oscillator over {period} periods",
        calculation_logic="RSI = 100 - (100 / (1 + RS)), where RS = avg_gain / avg_loss",
        data_source="market_data.close",
        tags=["momentum", "oscillator", "overbought_oversold"]
    )
    
    computation_func = FeaturizedIndicators.create_feature_function(
        indicator_func=TechnicalIndicators.rsi,
        output_columns=["rsi_value"],
        required_columns=["close"],
        parameters={"period": period}
    )
    
    return metadata, computation_func


def create_macd_features(
    fast_period: int = 12, 
    slow_period: int = 26, 
    signal_period: int = 9
) -> List[Tuple[FeatureMetadata, Callable]]:
    """Create MACD feature set."""
    features = []
    
    # MACD Line
    macd_metadata = FeatureMetadata(
        feature_id=f"macd_line_{fast_period}_{slow_period}",
        name=f"MACD Line ({fast_period}/{slow_period})",
        namespace="technical_indicators",
        version="1.0.0",
        schema=FeatureSchema(
            name="macd_line",
            feature_type=FeatureType.FLOAT,
            description=f"MACD line (EMA{fast_period} - EMA{slow_period})"
        ),
        category=IndicatorCategory.MOMENTUM,
        window_size=max(fast_period, slow_period),
        parameters={"fast_period": fast_period, "slow_period": slow_period, "signal_period": signal_period},
        description=f"MACD line: difference between {fast_period} and {slow_period} period EMAs",
        calculation_logic=f"MACD = EMA{fast_period} - EMA{slow_period}",
        data_source="market_data.close"
    )
    
    def macd_line_func(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        result = TechnicalIndicators.macd(data["close"], fast_period, slow_period, signal_period)
        output_df = data[['timestamp'] + [col for col in ['entity_id', 'symbol'] if col in data.columns]].copy()
        output_df["macd_line"] = result["macd"]
        return output_df
    
    features.append((macd_metadata, macd_line_func))
    
    # MACD Signal Line  
    signal_metadata = FeatureMetadata(
        feature_id=f"macd_signal_{fast_period}_{slow_period}_{signal_period}",
        name=f"MACD Signal ({fast_period}/{slow_period}/{signal_period})",
        namespace="technical_indicators",
        version="1.0.0",
        schema=FeatureSchema(
            name="macd_signal",
            feature_type=FeatureType.FLOAT,
            description=f"MACD signal line (EMA{signal_period} of MACD line)"
        ),
        category=IndicatorCategory.MOMENTUM,
        window_size=max(fast_period, slow_period) + signal_period,
        parameters={"fast_period": fast_period, "slow_period": slow_period, "signal_period": signal_period},
        dependencies=[f"macd_line_{fast_period}_{slow_period}"],
        description=f"MACD signal line: {signal_period}-period EMA of MACD line",
        calculation_logic=f"Signal = EMA{signal_period}(MACD)",
        data_source="market_data.close"
    )
    
    def macd_signal_func(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        result = TechnicalIndicators.macd(data["close"], fast_period, slow_period, signal_period)
        output_df = data[['timestamp'] + [col for col in ['entity_id', 'symbol'] if col in data.columns]].copy()
        output_df["macd_signal"] = result["signal"]
        return output_df
    
    features.append((signal_metadata, macd_signal_func))
    
    # MACD Histogram
    histogram_metadata = FeatureMetadata(
        feature_id=f"macd_histogram_{fast_period}_{slow_period}_{signal_period}",
        name=f"MACD Histogram ({fast_period}/{slow_period}/{signal_period})",
        namespace="technical_indicators", 
        version="1.0.0",
        schema=FeatureSchema(
            name="macd_histogram",
            feature_type=FeatureType.FLOAT,
            description="MACD histogram (MACD line - Signal line)"
        ),
        category=IndicatorCategory.MOMENTUM,
        window_size=max(fast_period, slow_period) + signal_period,
        parameters={"fast_period": fast_period, "slow_period": slow_period, "signal_period": signal_period},
        dependencies=[
            f"macd_line_{fast_period}_{slow_period}",
            f"macd_signal_{fast_period}_{slow_period}_{signal_period}"
        ],
        description="MACD histogram: difference between MACD line and signal line",
        calculation_logic="Histogram = MACD - Signal",
        data_source="market_data.close"
    )
    
    def macd_histogram_func(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        result = TechnicalIndicators.macd(data["close"], fast_period, slow_period, signal_period)
        output_df = data[['timestamp'] + [col for col in ['entity_id', 'symbol'] if col in data.columns]].copy()
        output_df["macd_histogram"] = result["histogram"]
        return output_df
    
    features.append((histogram_metadata, macd_histogram_func))
    
    return features


def create_bollinger_bands_features(
    period: int = 20, 
    std_dev: float = 2.0
) -> List[Tuple[FeatureMetadata, Callable]]:
    """Create Bollinger Bands feature set."""
    features = []
    
    # Upper Band
    upper_metadata = FeatureMetadata(
        feature_id=f"bb_upper_{period}_{int(std_dev*10)}",
        name=f"Bollinger Upper Band ({period}, {std_dev}σ)",
        namespace="technical_indicators",
        version="1.0.0",
        schema=FeatureSchema(
            name="bb_upper",
            feature_type=FeatureType.FLOAT,
            description=f"Bollinger upper band (SMA + {std_dev}σ)"
        ),
        category=IndicatorCategory.VOLATILITY,
        window_size=period,
        parameters={"period": period, "std_dev": std_dev},
        description=f"Bollinger upper band: {period}-period SMA + {std_dev} standard deviations",
        calculation_logic=f"Upper = SMA{period} + {std_dev} * STD{period}",
        data_source="market_data.close"
    )
    
    def bb_upper_func(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        result = TechnicalIndicators.bollinger_bands(data["close"], period, std_dev)
        output_df = data[['timestamp'] + [col for col in ['entity_id', 'symbol'] if col in data.columns]].copy()
        output_df["bb_upper"] = result["upper"]
        return output_df
    
    features.append((upper_metadata, bb_upper_func))
    
    # Middle Band (SMA)
    middle_metadata = FeatureMetadata(
        feature_id=f"bb_middle_{period}",
        name=f"Bollinger Middle Band ({period})",
        namespace="technical_indicators",
        version="1.0.0",
        schema=FeatureSchema(
            name="bb_middle", 
            feature_type=FeatureType.FLOAT,
            description=f"Bollinger middle band ({period}-period SMA)"
        ),
        category=IndicatorCategory.TREND,
        window_size=period,
        parameters={"period": period, "std_dev": std_dev},
        description=f"Bollinger middle band: {period}-period simple moving average",
        calculation_logic=f"Middle = SMA{period}",
        data_source="market_data.close"
    )
    
    def bb_middle_func(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        result = TechnicalIndicators.bollinger_bands(data["close"], period, std_dev)
        output_df = data[['timestamp'] + [col for col in ['entity_id', 'symbol'] if col in data.columns]].copy()
        output_df["bb_middle"] = result["middle"]
        return output_df
    
    features.append((middle_metadata, bb_middle_func))
    
    # Lower Band
    lower_metadata = FeatureMetadata(
        feature_id=f"bb_lower_{period}_{int(std_dev*10)}",
        name=f"Bollinger Lower Band ({period}, {std_dev}σ)",
        namespace="technical_indicators",
        version="1.0.0",
        schema=FeatureSchema(
            name="bb_lower",
            feature_type=FeatureType.FLOAT,
            description=f"Bollinger lower band (SMA - {std_dev}σ)"
        ),
        category=IndicatorCategory.VOLATILITY,
        window_size=period,
        parameters={"period": period, "std_dev": std_dev},
        description=f"Bollinger lower band: {period}-period SMA - {std_dev} standard deviations",
        calculation_logic=f"Lower = SMA{period} - {std_dev} * STD{period}",
        data_source="market_data.close"
    )
    
    def bb_lower_func(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        result = TechnicalIndicators.bollinger_bands(data["close"], period, std_dev)
        output_df = data[['timestamp'] + [col for col in ['entity_id', 'symbol'] if col in data.columns]].copy()
        output_df["bb_lower"] = result["lower"]
        return output_df
    
    features.append((lower_metadata, bb_lower_func))
    
    return features


def create_stochastic_features(
    k_period: int = 14, 
    d_period: int = 3
) -> List[Tuple[FeatureMetadata, Callable]]:
    """Create Stochastic Oscillator feature set."""
    features = []
    
    # %K
    k_metadata = FeatureMetadata(
        feature_id=f"stoch_k_{k_period}",
        name=f"Stochastic %K ({k_period})",
        namespace="technical_indicators",
        version="1.0.0",
        schema=FeatureSchema(
            name="stoch_k",
            feature_type=FeatureType.FLOAT,
            description=f"Stochastic %K oscillator (0-100)",
            constraints={"min_value": 0, "max_value": 100}
        ),
        category=IndicatorCategory.MOMENTUM,
        window_size=k_period,
        parameters={"k_period": k_period, "d_period": d_period},
        description=f"Stochastic %K: position of close within {k_period}-period high-low range",
        calculation_logic=f"%K = 100 * (close - low{k_period}) / (high{k_period} - low{k_period})",
        data_source="market_data.ohlc",
        tags=["momentum", "oscillator", "overbought_oversold"]
    )
    
    def stoch_k_func(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        result = TechnicalIndicators.stochastic_oscillator(
            data["high"], data["low"], data["close"], k_period, d_period
        )
        output_df = data[['timestamp'] + [col for col in ['entity_id', 'symbol'] if col in data.columns]].copy()
        output_df["stoch_k"] = result["k_percent"]
        return output_df
    
    features.append((k_metadata, stoch_k_func))
    
    # %D
    d_metadata = FeatureMetadata(
        feature_id=f"stoch_d_{k_period}_{d_period}",
        name=f"Stochastic %D ({k_period}, {d_period})",
        namespace="technical_indicators",
        version="1.0.0",
        schema=FeatureSchema(
            name="stoch_d",
            feature_type=FeatureType.FLOAT,
            description=f"Stochastic %D signal line (0-100)",
            constraints={"min_value": 0, "max_value": 100}
        ),
        category=IndicatorCategory.MOMENTUM,
        window_size=k_period + d_period,
        parameters={"k_period": k_period, "d_period": d_period},
        dependencies=[f"stoch_k_{k_period}"],
        description=f"Stochastic %D: {d_period}-period SMA of %K",
        calculation_logic=f"%D = SMA{d_period}(%K)",
        data_source="market_data.ohlc",
        tags=["momentum", "oscillator", "signal"]
    )
    
    def stoch_d_func(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        result = TechnicalIndicators.stochastic_oscillator(
            data["high"], data["low"], data["close"], k_period, d_period
        )
        output_df = data[['timestamp'] + [col for col in ['entity_id', 'symbol'] if col in data.columns]].copy()
        output_df["stoch_d"] = result["d_percent"]
        return output_df
    
    features.append((d_metadata, stoch_d_func))
    
    return features


def create_volume_features() -> List[Tuple[FeatureMetadata, Callable]]:
    """Create volume-based features."""
    features = []
    
    # On-Balance Volume
    obv_metadata = FeatureMetadata(
        feature_id="obv",
        name="On-Balance Volume",
        namespace="technical_indicators",
        version="1.0.0",
        schema=FeatureSchema(
            name="obv",
            feature_type=FeatureType.FLOAT,
            description="On-Balance Volume cumulative indicator"
        ),
        category=IndicatorCategory.VOLUME,
        window_size=1,  # Cumulative indicator
        parameters={},
        description="On-Balance Volume: cumulative volume based on price direction",
        calculation_logic="OBV += volume if close > prev_close else OBV -= volume",
        data_source="market_data.close_volume",
        tags=["volume", "cumulative", "trend_confirmation"]
    )
    
    computation_func = FeaturizedIndicators.create_feature_function(
        indicator_func=TechnicalIndicators.on_balance_volume,
        output_columns=["obv"],
        required_columns=["close", "volume"]
    )
    
    features.append((obv_metadata, computation_func))
    
    # VWAP
    vwap_metadata = FeatureMetadata(
        feature_id="vwap",
        name="Volume Weighted Average Price",
        namespace="technical_indicators", 
        version="1.0.0",
        schema=FeatureSchema(
            name="vwap",
            feature_type=FeatureType.FLOAT,
            description="Volume Weighted Average Price"
        ),
        category=IndicatorCategory.VOLUME,
        window_size=1,  # Cumulative indicator
        parameters={},
        description="VWAP: cumulative volume-weighted average of typical price",
        calculation_logic="VWAP = sum(typical_price * volume) / sum(volume)",
        data_source="market_data.ohlcv",
        tags=["volume", "price", "institutional_benchmark"]
    )
    
    def vwap_func(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        result = VolumeIndicators.volume_weighted_average_price(
            data["high"], data["low"], data["close"], data["volume"]
        )
        output_df = data[['timestamp'] + [col for col in ['entity_id', 'symbol'] if col in data.columns]].copy()
        output_df["vwap"] = result
        return output_df
    
    features.append((vwap_metadata, vwap_func))
    
    # A/D Line
    ad_metadata = FeatureMetadata(
        feature_id="ad_line",
        name="Accumulation/Distribution Line",
        namespace="technical_indicators",
        version="1.0.0",
        schema=FeatureSchema(
            name="ad_line",
            feature_type=FeatureType.FLOAT,
            description="Accumulation/Distribution Line"
        ),
        category=IndicatorCategory.VOLUME,
        window_size=1,  # Cumulative indicator
        parameters={},
        description="A/D Line: cumulative money flow based on price position in range",
        calculation_logic="A/D += ((close-low)-(high-close))/(high-low) * volume",
        data_source="market_data.ohlcv",
        tags=["volume", "accumulation", "distribution"]
    )
    
    def ad_func(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        result = VolumeIndicators.accumulation_distribution_line(
            data["high"], data["low"], data["close"], data["volume"]
        )
        output_df = data[['timestamp'] + [col for col in ['entity_id', 'symbol'] if col in data.columns]].copy()
        output_df["ad_line"] = result
        return output_df
    
    features.append((ad_metadata, ad_func))
    
    return features


def create_advanced_features() -> List[Tuple[FeatureMetadata, Callable]]:
    """Create advanced technical indicator features."""
    features = []
    
    # Parabolic SAR
    psar_metadata = FeatureMetadata(
        feature_id="parabolic_sar",
        name="Parabolic SAR",
        namespace="technical_indicators",
        version="1.0.0",
        schema=FeatureSchema(
            name="psar",
            feature_type=FeatureType.FLOAT,
            description="Parabolic Stop and Reverse values"
        ),
        category=IndicatorCategory.TREND,
        window_size=10,  # Adaptive window
        parameters={"af_start": 0.02, "af_increment": 0.02, "af_max": 0.2},
        description="Parabolic SAR: trend-following indicator with stop and reverse points",
        calculation_logic="SAR = SAR_prev + AF * (EP - SAR_prev)",
        data_source="market_data.hl",
        tags=["trend", "stop_loss", "trend_reversal"]
    )
    
    def psar_func(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        result = AdvancedTechnicalIndicators.parabolic_sar(
            data["high"], data["low"],
            kwargs.get("af_start", 0.02),
            kwargs.get("af_increment", 0.02),
            kwargs.get("af_max", 0.2)
        )
        output_df = data[['timestamp'] + [col for col in ['entity_id', 'symbol'] if col in data.columns]].copy()
        output_df["psar"] = result
        return output_df
    
    features.append((psar_metadata, psar_func))
    
    # Average True Range
    atr_metadata = FeatureMetadata(
        feature_id="atr_14",
        name="Average True Range (14)",
        namespace="technical_indicators",
        version="1.0.0",
        schema=FeatureSchema(
            name="atr",
            feature_type=FeatureType.FLOAT,
            description="14-period Average True Range"
        ),
        category=IndicatorCategory.VOLATILITY,
        window_size=15,  # 14 + 1 for diff calculation
        parameters={"period": 14},
        description="ATR: measure of volatility based on true range",
        calculation_logic="ATR = SMA(max(H-L, |H-C_prev|, |L-C_prev|))",
        data_source="market_data.ohlc",
        tags=["volatility", "risk", "position_sizing"]
    )
    
    def atr_func(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        period = kwargs.get("period", 14)
        result = TechnicalIndicators.atr(
            data["high"], data["low"], data["close"], period
        )
        output_df = data[['timestamp'] + [col for col in ['entity_id', 'symbol'] if col in data.columns]].copy()
        output_df["atr"] = result
        return output_df
    
    features.append((atr_metadata, atr_func))
    
    return features


async def register_all_technical_indicators(feature_store) -> Dict[str, bool]:
    """
    Register all technical indicator features with the feature store.
    
    Args:
        feature_store: FeatureStore instance
        
    Returns:
        Dictionary of registration results
    """
    results = {}
    
    try:
        # Basic trend indicators
        for period in [10, 20, 50, 100, 200]:
            metadata, func = create_sma_feature(period)
            results[metadata.feature_id] = await feature_store.register_feature(metadata, func)
            
            metadata, func = create_ema_feature(period)
            results[metadata.feature_id] = await feature_store.register_feature(metadata, func)
        
        # RSI indicators
        for period in [14, 21]:
            metadata, func = create_rsi_feature(period)
            results[metadata.feature_id] = await feature_store.register_feature(metadata, func)
        
        # MACD features
        macd_features = create_macd_features()
        for metadata, func in macd_features:
            results[metadata.feature_id] = await feature_store.register_feature(metadata, func)
        
        # Bollinger Bands
        bb_features = create_bollinger_bands_features()
        for metadata, func in bb_features:
            results[metadata.feature_id] = await feature_store.register_feature(metadata, func)
        
        # Stochastic Oscillator
        stoch_features = create_stochastic_features()
        for metadata, func in stoch_features:
            results[metadata.feature_id] = await feature_store.register_feature(metadata, func)
        
        # Volume indicators
        volume_features = create_volume_features()
        for metadata, func in volume_features:
            results[metadata.feature_id] = await feature_store.register_feature(metadata, func)
        
        # Advanced indicators
        advanced_features = create_advanced_features()
        for metadata, func in advanced_features:
            results[metadata.feature_id] = await feature_store.register_feature(metadata, func)
        
        logger.info(f"Registered {len(results)} technical indicator features")
        return results
        
    except Exception as e:
        logger.error(f"Failed to register technical indicators: {e}")
        return results