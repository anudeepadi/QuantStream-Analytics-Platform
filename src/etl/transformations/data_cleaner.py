"""
Data cleaning module for standardizing and correcting data issues.
"""

from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, regexp_replace, upper, lower, trim,
    coalesce, lit, round as spark_round, abs as spark_abs,
    lag, lead, avg, stddev, max as spark_max, min as spark_min,
    percentile_approx, collect_list, size, array_contains
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType, TimestampType
import structlog

logger = structlog.get_logger(__name__)


class DataCleaner:
    """
    Data cleaning component for ETL pipeline.
    
    Provides comprehensive data cleaning including:
    - Missing value handling
    - Outlier detection and correction
    - Data type standardization
    - Format normalization
    - Error correction
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize data cleaner.
        
        Args:
            config: Cleaning configuration including rules and methods
        """
        self.config = config
        self.cleaning_rules = config.get("rules", {})
        self.logger = logger.bind(component="DataCleaner")
        
        # Default cleaning configuration
        self.default_config = {
            "missing_value_strategy": "interpolate",  # interpolate, fill_forward, fill_backward, median, mean
            "outlier_method": "iqr",  # iqr, zscore, isolation_forest
            "outlier_threshold": 3.0,
            "outlier_action": "cap",  # cap, remove, interpolate
            "price_precision": 4,
            "volume_precision": 0,
            "symbol_case": "upper",
            "remove_invalid_symbols": True,
            "interpolation_window": 10
        }
        
        # Merge with user config
        self.config = {**self.default_config, **config}
        
    def clean_dataframe(self, df: DataFrame, data_type: str = "unknown") -> DataFrame:
        """
        Perform comprehensive data cleaning.
        
        Args:
            df: Input DataFrame to clean
            data_type: Type of data (quotes, trades, bars)
            
        Returns:
            DataFrame: Cleaned DataFrame
        """
        try:
            self.logger.info("Starting data cleaning", 
                           data_type=data_type, 
                           row_count=df.count())
            
            # Apply cleaning steps in order
            df_cleaned = df
            
            # 1. Standardize formats
            df_cleaned = self._standardize_formats(df_cleaned)
            
            # 2. Handle missing values
            df_cleaned = self._handle_missing_values(df_cleaned, data_type)
            
            # 3. Handle outliers
            df_cleaned = self._handle_outliers(df_cleaned, data_type)
            
            # 4. Correct data errors
            df_cleaned = self._correct_data_errors(df_cleaned, data_type)
            
            # 5. Apply precision rules
            df_cleaned = self._apply_precision_rules(df_cleaned)
            
            # 6. Add cleaning metadata
            df_cleaned = self._add_cleaning_metadata(df_cleaned)
            
            self.logger.info("Data cleaning completed")
            return df_cleaned
            
        except Exception as e:
            self.logger.error("Data cleaning failed", error=str(e))
            raise
            
    def _standardize_formats(self, df: DataFrame) -> DataFrame:
        """Standardize field formats."""
        df_standardized = df
        
        # Standardize symbol format
        if "symbol" in df.columns:
            if self.config["symbol_case"] == "upper":
                df_standardized = df_standardized.withColumn(
                    "symbol_standardized",
                    upper(trim(regexp_replace(col("symbol"), "[^A-Za-z0-9]", "")))
                )
            else:
                df_standardized = df_standardized.withColumn(
                    "symbol_standardized",
                    lower(trim(regexp_replace(col("symbol"), "[^A-Za-z0-9]", "")))
                )
                
        # Clean numeric fields
        numeric_fields = [
            "price", "bid_price", "ask_price", "last_price",
            "open", "high", "low", "close",
            "volume", "size", "bid_size", "ask_size"
        ]
        
        for field in numeric_fields:
            if field in df.columns:
                # Remove non-numeric characters and convert to double
                df_standardized = df_standardized.withColumn(
                    f"{field}_clean",
                    regexp_replace(col(field), "[^0-9.-]", "").cast(DoubleType())
                )
                
        # Standardize timestamp format
        if "timestamp" in df.columns:
            # Basic timestamp cleaning - remove extra spaces and standardize format
            df_standardized = df_standardized.withColumn(
                "timestamp_clean",
                trim(regexp_replace(col("timestamp"), "\\s+", " "))
            )
            
        return df_standardized
        
    def _handle_missing_values(self, df: DataFrame, data_type: str) -> DataFrame:
        """Handle missing values based on strategy."""
        strategy = self.config["missing_value_strategy"]
        
        if strategy == "interpolate":
            return self._interpolate_missing_values(df, data_type)
        elif strategy == "fill_forward":
            return self._fill_forward(df)
        elif strategy == "fill_backward":
            return self._fill_backward(df)
        elif strategy == "median":
            return self._fill_with_median(df)
        elif strategy == "mean":
            return self._fill_with_mean(df)
        else:
            self.logger.warning("Unknown missing value strategy", strategy=strategy)
            return df
            
    def _interpolate_missing_values(self, df: DataFrame, data_type: str) -> DataFrame:
        """Interpolate missing values using linear interpolation."""
        # Define window for interpolation
        window_spec = (Window
                      .partitionBy("symbol_standardized")
                      .orderBy("timestamp_clean")
                      .rowsBetween(-self.config["interpolation_window"], 
                                  self.config["interpolation_window"]))
        
        # Fields to interpolate
        numeric_fields = [col_name for col_name in df.columns 
                         if col_name.endswith("_clean") and "price" in col_name or "volume" in col_name]
        
        df_interpolated = df
        
        for field in numeric_fields:
            if field in df.columns:
                # Simple interpolation using average of non-null values in window
                df_interpolated = df_interpolated.withColumn(
                    f"{field}_interpolated",
                    when(
                        col(field).isNull() | isnan(col(field)),
                        avg(col(field)).over(window_spec)
                    ).otherwise(col(field))
                )
                
        return df_interpolated
        
    def _fill_forward(self, df: DataFrame) -> DataFrame:
        """Forward fill missing values."""
        window_spec = (Window
                      .partitionBy("symbol_standardized")
                      .orderBy("timestamp_clean")
                      .rowsBetween(Window.unboundedPreceding, 0))
        
        numeric_fields = [col_name for col_name in df.columns 
                         if col_name.endswith("_clean")]
        
        df_filled = df
        
        for field in numeric_fields:
            if field in df.columns:
                df_filled = df_filled.withColumn(
                    f"{field}_filled",
                    when(
                        col(field).isNull() | isnan(col(field)),
                        last(col(field), ignorenulls=True).over(window_spec)
                    ).otherwise(col(field))
                )
                
        return df_filled
        
    def _fill_backward(self, df: DataFrame) -> DataFrame:
        """Backward fill missing values."""
        window_spec = (Window
                      .partitionBy("symbol_standardized")
                      .orderBy("timestamp_clean")
                      .rowsBetween(0, Window.unboundedFollowing))
        
        numeric_fields = [col_name for col_name in df.columns 
                         if col_name.endswith("_clean")]
        
        df_filled = df
        
        for field in numeric_fields:
            if field in df.columns:
                df_filled = df_filled.withColumn(
                    f"{field}_filled",
                    when(
                        col(field).isNull() | isnan(col(field)),
                        first(col(field), ignorenulls=True).over(window_spec)
                    ).otherwise(col(field))
                )
                
        return df_filled
        
    def _fill_with_median(self, df: DataFrame) -> DataFrame:
        """Fill missing values with median per symbol."""
        numeric_fields = [col_name for col_name in df.columns 
                         if col_name.endswith("_clean")]
        
        df_filled = df
        
        for field in numeric_fields:
            if field in df.columns:
                # Calculate median per symbol
                median_values = (df
                               .filter(col(field).isNotNull() & (~isnan(col(field))))
                               .groupBy("symbol_standardized")
                               .agg(percentile_approx(col(field), 0.5).alias(f"{field}_median")))
                
                # Join and fill
                df_filled = (df_filled
                           .join(median_values, "symbol_standardized", "left")
                           .withColumn(
                               f"{field}_filled",
                               when(
                                   col(field).isNull() | isnan(col(field)),
                                   col(f"{field}_median")
                               ).otherwise(col(field))
                           )
                           .drop(f"{field}_median"))
                
        return df_filled
        
    def _fill_with_mean(self, df: DataFrame) -> DataFrame:
        """Fill missing values with mean per symbol."""
        numeric_fields = [col_name for col_name in df.columns 
                         if col_name.endswith("_clean")]
        
        df_filled = df
        
        for field in numeric_fields:
            if field in df.columns:
                # Calculate mean per symbol
                mean_values = (df
                             .filter(col(field).isNotNull() & (~isnan(col(field))))
                             .groupBy("symbol_standardized")
                             .agg(avg(col(field)).alias(f"{field}_mean")))
                
                # Join and fill
                df_filled = (df_filled
                           .join(mean_values, "symbol_standardized", "left")
                           .withColumn(
                               f"{field}_filled",
                               when(
                                   col(field).isNull() | isnan(col(field)),
                                   col(f"{field}_mean")
                               ).otherwise(col(field))
                           )
                           .drop(f"{field}_mean"))
                
        return df_filled
        
    def _handle_outliers(self, df: DataFrame, data_type: str) -> DataFrame:
        """Handle outliers based on configured method."""
        method = self.config["outlier_method"]
        
        if method == "iqr":
            return self._handle_outliers_iqr(df)
        elif method == "zscore":
            return self._handle_outliers_zscore(df)
        else:
            self.logger.warning("Unknown outlier method", method=method)
            return df
            
    def _handle_outliers_iqr(self, df: DataFrame) -> DataFrame:
        """Handle outliers using Interquartile Range method."""
        numeric_fields = [col_name for col_name in df.columns 
                         if "_clean" in col_name and any(keyword in col_name for keyword in ["price", "volume"])]
        
        df_outliers = df
        
        for field in numeric_fields:
            if field in df.columns:
                # Calculate IQR per symbol
                percentiles = (df
                             .filter(col(field).isNotNull() & (~isnan(col(field))))
                             .groupBy("symbol_standardized")
                             .agg(
                                 percentile_approx(col(field), 0.25).alias(f"{field}_q1"),
                                 percentile_approx(col(field), 0.75).alias(f"{field}_q3")
                             ))
                
                # Join and identify outliers
                df_outliers = (df_outliers
                             .join(percentiles, "symbol_standardized", "left")
                             .withColumn(
                                 f"{field}_iqr",
                                 col(f"{field}_q3") - col(f"{field}_q1")
                             )
                             .withColumn(
                                 f"{field}_lower_bound",
                                 col(f"{field}_q1") - (1.5 * col(f"{field}_iqr"))
                             )
                             .withColumn(
                                 f"{field}_upper_bound",
                                 col(f"{field}_q3") + (1.5 * col(f"{field}_iqr"))
                             )
                             .withColumn(
                                 f"is_outlier_{field}",
                                 when(
                                     col(field).isNotNull() & 
                                     ((col(field) < col(f"{field}_lower_bound")) |
                                      (col(field) > col(f"{field}_upper_bound"))),
                                     True
                                 ).otherwise(False)
                             ))
                
                # Apply outlier action
                action = self.config["outlier_action"]
                if action == "cap":
                    df_outliers = df_outliers.withColumn(
                        f"{field}_outlier_handled",
                        when(
                            col(field) < col(f"{field}_lower_bound"),
                            col(f"{field}_lower_bound")
                        ).when(
                            col(field) > col(f"{field}_upper_bound"),
                            col(f"{field}_upper_bound")
                        ).otherwise(col(field))
                    )
                elif action == "remove":
                    # Mark for removal rather than actually removing
                    df_outliers = df_outliers.withColumn(
                        f"{field}_outlier_handled",
                        when(col(f"is_outlier_{field}"), lit(None)).otherwise(col(field))
                    )
                else:  # interpolate
                    window_spec = (Window
                                  .partitionBy("symbol_standardized")
                                  .orderBy("timestamp_clean")
                                  .rowsBetween(-5, 5))
                    
                    df_outliers = df_outliers.withColumn(
                        f"{field}_outlier_handled",
                        when(
                            col(f"is_outlier_{field}"),
                            avg(col(field)).over(window_spec)
                        ).otherwise(col(field))
                    )
                
                # Clean up temporary columns
                df_outliers = df_outliers.drop(
                    f"{field}_q1", f"{field}_q3", f"{field}_iqr",
                    f"{field}_lower_bound", f"{field}_upper_bound"
                )
                
        return df_outliers
        
    def _handle_outliers_zscore(self, df: DataFrame) -> DataFrame:
        """Handle outliers using Z-score method."""
        threshold = self.config["outlier_threshold"]
        
        numeric_fields = [col_name for col_name in df.columns 
                         if "_clean" in col_name and any(keyword in col_name for keyword in ["price", "volume"])]
        
        df_outliers = df
        
        for field in numeric_fields:
            if field in df.columns:
                # Calculate mean and std per symbol
                stats = (df
                        .filter(col(field).isNotNull() & (~isnan(col(field))))
                        .groupBy("symbol_standardized")
                        .agg(
                            avg(col(field)).alias(f"{field}_mean"),
                            stddev(col(field)).alias(f"{field}_std")
                        ))
                
                # Join and calculate z-scores
                df_outliers = (df_outliers
                             .join(stats, "symbol_standardized", "left")
                             .withColumn(
                                 f"{field}_zscore",
                                 when(
                                     col(f"{field}_std") > 0,
                                     (col(field) - col(f"{field}_mean")) / col(f"{field}_std")
                                 ).otherwise(0)
                             )
                             .withColumn(
                                 f"is_outlier_{field}",
                                 spark_abs(col(f"{field}_zscore")) > threshold
                             ))
                
                # Apply outlier action (similar to IQR method)
                action = self.config["outlier_action"]
                if action == "cap":
                    df_outliers = df_outliers.withColumn(
                        f"{field}_outlier_handled",
                        when(
                            spark_abs(col(f"{field}_zscore")) > threshold,
                            col(f"{field}_mean") + (threshold * col(f"{field}_std") * 
                                                   when(col(f"{field}_zscore") > 0, 1).otherwise(-1))
                        ).otherwise(col(field))
                    )
                elif action == "remove":
                    df_outliers = df_outliers.withColumn(
                        f"{field}_outlier_handled",
                        when(col(f"is_outlier_{field}"), lit(None)).otherwise(col(field))
                    )
                
                # Clean up temporary columns
                df_outliers = df_outliers.drop(f"{field}_mean", f"{field}_std", f"{field}_zscore")
                
        return df_outliers
        
    def _correct_data_errors(self, df: DataFrame, data_type: str) -> DataFrame:
        """Correct common data errors."""
        df_corrected = df
        
        # Correct negative prices (set to null for interpolation)
        price_fields = [col_name for col_name in df.columns 
                       if "price" in col_name and "_clean" in col_name]
        
        for field in price_fields:
            if field in df.columns:
                df_corrected = df_corrected.withColumn(
                    f"{field}_corrected",
                    when(col(field) <= 0, lit(None)).otherwise(col(field))
                )
                
        # Correct negative volumes (set to 0)
        volume_fields = [col_name for col_name in df.columns 
                        if any(keyword in col_name for keyword in ["volume", "size"]) and "_clean" in col_name]
        
        for field in volume_fields:
            if field in df.columns:
                df_corrected = df_corrected.withColumn(
                    f"{field}_corrected",
                    when(col(field) < 0, lit(0)).otherwise(col(field))
                )
                
        # Correct OHLC inconsistencies for bar data
        if data_type == "bars" and all(f"{price}_clean" in df.columns for price in ["open", "high", "low", "close"]):
            df_corrected = self._correct_ohlc_inconsistencies(df_corrected)
            
        return df_corrected
        
    def _correct_ohlc_inconsistencies(self, df: DataFrame) -> DataFrame:
        """Correct OHLC price inconsistencies."""
        # Ensure high is the maximum and low is the minimum
        df_corrected = df.withColumn(
            "actual_high",
            spark_max(col("open_clean"), col("high_clean"), col("low_clean"), col("close_clean"))
        ).withColumn(
            "actual_low", 
            spark_min(col("open_clean"), col("high_clean"), col("low_clean"), col("close_clean"))
        ).withColumn(
            "high_clean_corrected",
            col("actual_high")
        ).withColumn(
            "low_clean_corrected",
            col("actual_low")
        ).drop("actual_high", "actual_low")
        
        return df_corrected
        
    def _apply_precision_rules(self, df: DataFrame) -> DataFrame:
        """Apply precision rules to numeric fields."""
        price_precision = self.config["price_precision"]
        volume_precision = self.config["volume_precision"]
        
        df_precision = df
        
        # Apply price precision
        price_fields = [col_name for col_name in df.columns 
                       if "price" in col_name and ("_clean" in col_name or "_corrected" in col_name)]
        
        for field in price_fields:
            if field in df.columns:
                df_precision = df_precision.withColumn(
                    f"{field}_final",
                    spark_round(col(field), price_precision)
                )
                
        # Apply volume precision
        volume_fields = [col_name for col_name in df.columns 
                        if any(keyword in col_name for keyword in ["volume", "size"]) and 
                        ("_clean" in col_name or "_corrected" in col_name)]
        
        for field in volume_fields:
            if field in df.columns:
                df_precision = df_precision.withColumn(
                    f"{field}_final",
                    spark_round(col(field), volume_precision)
                )
                
        return df_precision
        
    def _add_cleaning_metadata(self, df: DataFrame) -> DataFrame:
        """Add metadata about the cleaning process."""
        from pyspark.sql.functions import current_timestamp, lit, array, when
        
        # Count cleaning actions performed
        outlier_columns = [col_name for col_name in df.columns if col_name.startswith("is_outlier_")]
        
        df_metadata = df.withColumn(
            "cleaning_timestamp",
            current_timestamp()
        ).withColumn(
            "cleaning_version",
            lit("1.0")
        )
        
        if outlier_columns:
            outlier_sum = sum([col(c).cast("int") for c in outlier_columns])
            df_metadata = df_metadata.withColumn(
                "outliers_detected",
                outlier_sum
            ).withColumn(
                "data_cleaned",
                outlier_sum > 0
            )
        else:
            df_metadata = df_metadata.withColumn(
                "outliers_detected",
                lit(0)
            ).withColumn(
                "data_cleaned",
                lit(False)
            )
            
        return df_metadata