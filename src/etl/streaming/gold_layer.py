"""
Gold layer streaming job for business-ready aggregated metrics and analytics.

The Gold layer represents business-ready data optimized for analytics,
reporting, and machine learning applications.
"""

from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, window, count, sum as spark_sum, avg, min as spark_min, 
    max as spark_max, first, last, stddev, variance, skewness, kurtosis,
    collect_list, array_sort, size, percentile_approx, expr,
    current_timestamp, lit, round as spark_round, coalesce,
    lag, lead, row_number, rank, dense_rank, desc, asc,
    unix_timestamp, from_unixtime, date_format
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pyspark.sql.window import Window
import structlog

from .base_streaming_job import BaseStreamingJob

logger = structlog.get_logger(__name__)


class GoldLayerJob(BaseStreamingJob):
    """
    Gold layer streaming job for business metrics and aggregations.
    
    Features:
    - Real-time OHLCV bar generation
    - Volume-weighted metrics
    - Market microstructure indicators
    - Risk metrics and volatility measures
    - Performance analytics
    - Time-series aggregations
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Gold layer job.
        
        Args:
            config: Job configuration including aggregation rules and time windows
        """
        super().__init__(config, "gold-layer")
        self.silver_path = config.get("silver_layer", {}).get("output_path", "/tmp/silver")
        
        # Time windows for aggregations
        self.time_windows = config.get("gold_layer", {}).get("time_windows", [
            "1 minute", "5 minutes", "15 minutes", "1 hour", "1 day"
        ])
        
        # Aggregation types
        self.aggregation_types = config.get("gold_layer", {}).get("aggregation_types", [
            "ohlcv_bars", "quote_metrics", "trade_metrics", "market_stats"
        ])
        
    def create_source_stream(self) -> DataFrame:
        """
        Create source stream from Silver layer Delta table.
        
        Returns:
            DataFrame: Silver layer streaming DataFrame
        """
        try:
            self.logger.info("Creating source stream from Silver layer", 
                           silver_path=self.silver_path)
            
            # Read from Silver layer with watermarking
            df = (
                self.spark
                .readStream
                .format("delta")
                .option("ignoreChanges", "true")
                .option("ignoreDeletes", "true")
                .load(self.silver_path)
                .filter(col("is_silver_quality") == True)  # Only high-quality Silver data
                .withWatermark("timestamp_parsed", "15 minutes")  # Handle late data
            )
            
            return df
            
        except Exception as e:
            self.logger.error("Failed to create Silver layer source stream", error=str(e))
            raise
            
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply Gold layer aggregations and business logic.
        
        Args:
            df: Silver layer DataFrame
            
        Returns:
            DataFrame: Gold layer aggregated DataFrame
        """
        try:
            gold_dfs = []
            
            # Generate different types of aggregations
            if "ohlcv_bars" in self.aggregation_types:
                ohlcv_df = self._create_ohlcv_bars(df)
                gold_dfs.append(ohlcv_df)
                
            if "quote_metrics" in self.aggregation_types:
                quote_metrics_df = self._create_quote_metrics(df)
                gold_dfs.append(quote_metrics_df)
                
            if "trade_metrics" in self.aggregation_types:
                trade_metrics_df = self._create_trade_metrics(df)
                gold_dfs.append(trade_metrics_df)
                
            if "market_stats" in self.aggregation_types:
                market_stats_df = self._create_market_statistics(df)
                gold_dfs.append(market_stats_df)
            
            # Union all aggregated DataFrames
            if gold_dfs:
                # Add metadata to each DataFrame
                gold_dfs_with_metadata = []
                for i, gdf in enumerate(gold_dfs):
                    gdf_with_meta = gdf.withColumn(
                        "gold_processed_timestamp",
                        current_timestamp()
                    ).withColumn(
                        "aggregation_type",
                        lit(self.aggregation_types[i] if i < len(self.aggregation_types) else "unknown")
                    ).withColumn(
                        "gold_job_id",
                        lit(self.job_name)
                    )
                    gold_dfs_with_metadata.append(gdf_with_meta)
                
                # For simplicity, return the first aggregation type
                # In production, you might want separate sinks for each type
                result_df = gold_dfs_with_metadata[0]
                
                self.logger.info("Applied Gold layer transformations", 
                               aggregation_types=len(gold_dfs))
                return result_df
            else:
                raise ValueError("No aggregation types configured")
                
        except Exception as e:
            self.logger.error("Failed to transform Gold layer data", error=str(e))
            raise
            
    def _create_ohlcv_bars(self, df: DataFrame) -> DataFrame:
        """
        Create OHLCV (Open, High, Low, Close, Volume) bars from trade data.
        
        Args:
            df: Input DataFrame with trade data
            
        Returns:
            DataFrame: OHLCV bars DataFrame
        """
        # Filter for trade data
        trade_df = df.filter(col("topic") == "market_data_trades")
        
        if trade_df.count() == 0:
            # Return empty DataFrame with schema if no trade data
            return self._get_empty_ohlcv_schema()
        
        ohlcv_dfs = []
        
        for time_window in self.time_windows:
            # Group by symbol and time window
            windowed_df = (
                trade_df
                .groupBy(
                    col("symbol_clean"),
                    window(col("timestamp_parsed"), time_window).alias("time_window")
                )
                .agg(
                    first("price_decimal").alias("open_price"),
                    spark_max("price_decimal").alias("high_price"),
                    spark_min("price_decimal").alias("low_price"),
                    last("price_decimal").alias("close_price"),
                    spark_sum("size_long").alias("volume"),
                    count("*").alias("trade_count"),
                    avg("price_decimal").alias("avg_price"),
                    stddev("price_decimal").alias("price_volatility"),
                    spark_sum(col("price_decimal") * col("size_long")).alias("value_traded")
                )
                .select(
                    col("symbol_clean").alias("symbol"),
                    col("time_window.start").alias("window_start"),
                    col("time_window.end").alias("window_end"),
                    lit(time_window).alias("timeframe"),
                    col("open_price"),
                    col("high_price"),
                    col("low_price"),
                    col("close_price"),
                    col("volume"),
                    col("trade_count"),
                    col("avg_price"),
                    col("price_volatility"),
                    col("value_traded"),
                    (col("value_traded") / col("volume")).alias("vwap"),  # Volume Weighted Average Price
                    ((col("close_price") - col("open_price")) / col("open_price") * 100).alias("price_change_pct"),
                    (col("high_price") - col("low_price")).alias("price_range"),
                    ((col("high_price") - col("low_price")) / col("open_price") * 100).alias("price_range_pct")
                )
            )
            
            ohlcv_dfs.append(windowed_df)
        
        # Union all timeframes
        if ohlcv_dfs:
            return ohlcv_dfs[0].unionByName(*ohlcv_dfs[1:]) if len(ohlcv_dfs) > 1 else ohlcv_dfs[0]
        else:
            return self._get_empty_ohlcv_schema()
            
    def _create_quote_metrics(self, df: DataFrame) -> DataFrame:
        """
        Create quote-based metrics from market data quotes.
        
        Args:
            df: Input DataFrame with quote data
            
        Returns:
            DataFrame: Quote metrics DataFrame
        """
        # Filter for quote data
        quote_df = df.filter(col("topic") == "market_data_quotes")
        
        if quote_df.count() == 0:
            return self._get_empty_quote_metrics_schema()
            
        quote_metrics_dfs = []
        
        for time_window in self.time_windows:
            windowed_df = (
                quote_df
                .groupBy(
                    col("symbol_clean"),
                    window(col("timestamp_parsed"), time_window).alias("time_window")
                )
                .agg(
                    # Bid metrics
                    avg("bid_price_decimal").alias("avg_bid_price"),
                    spark_max("bid_price_decimal").alias("max_bid_price"),
                    spark_min("bid_price_decimal").alias("min_bid_price"),
                    last("bid_price_decimal").alias("last_bid_price"),
                    
                    # Ask metrics
                    avg("ask_price_decimal").alias("avg_ask_price"),
                    spark_max("ask_price_decimal").alias("max_ask_price"),
                    spark_min("ask_price_decimal").alias("min_ask_price"),
                    last("ask_price_decimal").alias("last_ask_price"),
                    
                    # Spread metrics
                    avg("spread_bps").alias("avg_spread_bps"),
                    spark_max("spread_bps").alias("max_spread_bps"),
                    spark_min("spread_bps").alias("min_spread_bps"),
                    stddev("spread_bps").alias("spread_volatility"),
                    
                    # Mid price metrics
                    avg("mid_price").alias("avg_mid_price"),
                    stddev("mid_price").alias("mid_price_volatility"),
                    
                    # Quote counts
                    count("*").alias("quote_count"),
                    spark_sum(when(col("spread_bps") <= 10, 1).otherwise(0)).alias("tight_spread_count")
                )
                .select(
                    col("symbol_clean").alias("symbol"),
                    col("time_window.start").alias("window_start"),
                    col("time_window.end").alias("window_end"),
                    lit(time_window).alias("timeframe"),
                    col("avg_bid_price"),
                    col("max_bid_price"),
                    col("min_bid_price"),
                    col("last_bid_price"),
                    col("avg_ask_price"),
                    col("max_ask_price"),
                    col("min_ask_price"),
                    col("last_ask_price"),
                    col("avg_spread_bps"),
                    col("max_spread_bps"),
                    col("min_spread_bps"),
                    col("spread_volatility"),
                    col("avg_mid_price"),
                    col("mid_price_volatility"),
                    col("quote_count"),
                    col("tight_spread_count"),
                    (col("tight_spread_count") / col("quote_count") * 100).alias("tight_spread_pct")
                )
            )
            
            quote_metrics_dfs.append(windowed_df)
            
        # Union all timeframes
        if quote_metrics_dfs:
            return quote_metrics_dfs[0].unionByName(*quote_metrics_dfs[1:]) if len(quote_metrics_dfs) > 1 else quote_metrics_dfs[0]
        else:
            return self._get_empty_quote_metrics_schema()
            
    def _create_trade_metrics(self, df: DataFrame) -> DataFrame:
        """
        Create trade-based metrics and microstructure indicators.
        
        Args:
            df: Input DataFrame with trade data
            
        Returns:
            DataFrame: Trade metrics DataFrame
        """
        # Filter for trade data
        trade_df = df.filter(col("topic") == "market_data_trades")
        
        if trade_df.count() == 0:
            return self._get_empty_trade_metrics_schema()
            
        trade_metrics_dfs = []
        
        for time_window in self.time_windows:
            windowed_df = (
                trade_df
                .groupBy(
                    col("symbol_clean"),
                    window(col("timestamp_parsed"), time_window).alias("time_window")
                )
                .agg(
                    # Volume metrics
                    spark_sum("size_long").alias("total_volume"),
                    avg("size_long").alias("avg_trade_size"),
                    spark_max("size_long").alias("max_trade_size"),
                    spark_min("size_long").alias("min_trade_size"),
                    stddev("size_long").alias("trade_size_volatility"),
                    
                    # Price metrics
                    spark_sum(col("price_decimal") * col("size_long")).alias("dollar_volume"),
                    avg("price_decimal").alias("avg_trade_price"),
                    stddev("price_decimal").alias("trade_price_volatility"),
                    
                    # Trade intensity
                    count("*").alias("trade_count"),
                    
                    # Percentiles
                    percentile_approx("size_long", 0.5).alias("median_trade_size"),
                    percentile_approx("size_long", 0.9).alias("trade_size_90th_pct"),
                    percentile_approx("price_decimal", 0.5).alias("median_trade_price")
                )
                .select(
                    col("symbol_clean").alias("symbol"),
                    col("time_window.start").alias("window_start"),
                    col("time_window.end").alias("window_end"),
                    lit(time_window).alias("timeframe"),
                    col("total_volume"),
                    col("avg_trade_size"),
                    col("max_trade_size"),
                    col("min_trade_size"),
                    col("trade_size_volatility"),
                    col("dollar_volume"),
                    col("avg_trade_price"),
                    col("trade_price_volatility"),
                    col("trade_count"),
                    col("median_trade_size"),
                    col("trade_size_90th_pct"),
                    col("median_trade_price"),
                    (col("dollar_volume") / col("total_volume")).alias("vwap"),
                    (col("trade_count") / (unix_timestamp(col("window_end")) - unix_timestamp(col("window_start")))).alias("trades_per_second")
                )
            )
            
            trade_metrics_dfs.append(windowed_df)
            
        # Union all timeframes
        if trade_metrics_dfs:
            return trade_metrics_dfs[0].unionByName(*trade_metrics_dfs[1:]) if len(trade_metrics_dfs) > 1 else trade_metrics_dfs[0]
        else:
            return self._get_empty_trade_metrics_schema()
            
    def _create_market_statistics(self, df: DataFrame) -> DataFrame:
        """
        Create market-wide statistics and risk metrics.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: Market statistics DataFrame
        """
        # Combine all market data types
        market_stats_dfs = []
        
        for time_window in self.time_windows:
            # Overall market activity
            windowed_df = (
                df
                .groupBy(
                    window(col("timestamp_parsed"), time_window).alias("time_window")
                )
                .agg(
                    # Counts by type
                    spark_sum(when(col("topic") == "market_data_quotes", 1).otherwise(0)).alias("total_quotes"),
                    spark_sum(when(col("topic") == "market_data_trades", 1).otherwise(0)).alias("total_trades"),
                    spark_sum(when(col("topic") == "market_data_bars", 1).otherwise(0)).alias("total_bars"),
                    
                    # Unique symbols
                    count(col("symbol_clean").distinct()).alias("active_symbols"),
                    
                    # Data quality metrics
                    avg("overall_quality_score").alias("avg_data_quality"),
                    spark_sum(when(col("is_silver_quality") == True, 1).otherwise(0)).alias("high_quality_records"),
                    count("*").alias("total_records"),
                    
                    # Processing metrics
                    avg((unix_timestamp(col("silver_processed_timestamp")) - 
                         unix_timestamp(col("timestamp_parsed")))).alias("avg_processing_latency_seconds")
                )
                .select(
                    col("time_window.start").alias("window_start"),
                    col("time_window.end").alias("window_end"),
                    lit(time_window).alias("timeframe"),
                    lit("market_statistics").alias("symbol"),  # Use literal for market-wide stats
                    col("total_quotes"),
                    col("total_trades"),
                    col("total_bars"),
                    col("active_symbols"),
                    col("avg_data_quality"),
                    col("high_quality_records"),
                    col("total_records"),
                    (col("high_quality_records") / col("total_records") * 100).alias("data_quality_pct"),
                    col("avg_processing_latency_seconds")
                )
            )
            
            market_stats_dfs.append(windowed_df)
            
        # Union all timeframes
        if market_stats_dfs:
            return market_stats_dfs[0].unionByName(*market_stats_dfs[1:]) if len(market_stats_dfs) > 1 else market_stats_dfs[0]
        else:
            return self._get_empty_market_stats_schema()
            
    def _get_empty_ohlcv_schema(self) -> DataFrame:
        """Get empty DataFrame with OHLCV schema."""
        schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("window_start", TimestampType(), True),
            StructField("window_end", TimestampType(), True),
            StructField("timeframe", StringType(), True),
            StructField("open_price", DoubleType(), True),
            StructField("high_price", DoubleType(), True),
            StructField("low_price", DoubleType(), True),
            StructField("close_price", DoubleType(), True),
            StructField("volume", LongType(), True),
            StructField("trade_count", LongType(), True),
            StructField("avg_price", DoubleType(), True),
            StructField("price_volatility", DoubleType(), True),
            StructField("value_traded", DoubleType(), True),
            StructField("vwap", DoubleType(), True),
            StructField("price_change_pct", DoubleType(), True),
            StructField("price_range", DoubleType(), True),
            StructField("price_range_pct", DoubleType(), True)
        ])
        return self.spark.createDataFrame([], schema)
        
    def _get_empty_quote_metrics_schema(self) -> DataFrame:
        """Get empty DataFrame with quote metrics schema."""
        # Simplified - would define complete schema
        return self.spark.createDataFrame([], StructType([]))
        
    def _get_empty_trade_metrics_schema(self) -> DataFrame:
        """Get empty DataFrame with trade metrics schema."""
        # Simplified - would define complete schema
        return self.spark.createDataFrame([], StructType([]))
        
    def _get_empty_market_stats_schema(self) -> DataFrame:
        """Get empty DataFrame with market stats schema."""
        # Simplified - would define complete schema
        return self.spark.createDataFrame([], StructType([]))
        
    def get_sink_options(self) -> Dict[str, Any]:
        """Get Gold layer sink configuration."""
        gold_config = self.config.get("gold_layer", {})
        
        return {
            "path": gold_config.get("output_path", "/tmp/gold"),
            "format": "delta",
            "output_mode": "append",
            "trigger_interval": gold_config.get("trigger_interval", "2 minutes"),
            "options": {
                "mergeSchema": "true",
                "autoOptimize": "true",
                "optimizeWrite": "true",
                "partitionBy": "symbol,timeframe,year,month,day",
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true",
                "delta.columnMapping.mode": "name",
                "delta.enableChangeDataFeed": "true"  # Enable CDC for downstream consumers
            }
        }