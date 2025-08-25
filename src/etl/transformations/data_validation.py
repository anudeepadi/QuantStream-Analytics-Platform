"""
Data validation module for ensuring data quality and integrity.
"""

from typing import Dict, Any, List, Tuple, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, regexp_extract, length,
    count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    lit, current_timestamp, struct, array, map_from_arrays
)
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType
import structlog

logger = structlog.get_logger(__name__)


class DataValidator:
    """
    Data validation component for ETL pipeline.
    
    Provides comprehensive data validation including:
    - Schema validation
    - Business rule validation  
    - Data type validation
    - Range validation
    - Pattern validation
    - Completeness validation
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize data validator.
        
        Args:
            config: Validation configuration including rules and thresholds
        """
        self.config = config
        self.validation_rules = config.get("rules", {})
        self.thresholds = config.get("thresholds", {})
        self.logger = logger.bind(component="DataValidator")
        
        # Default validation rules
        self.default_rules = {
            "price_range": {"min": 0.01, "max": 100000.0},
            "volume_range": {"min": 0, "max": 1000000000},
            "spread_range": {"min": 0, "max": 1000},  # basis points
            "symbol_pattern": r"^[A-Z]{1,10}$",
            "timestamp_pattern": r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",
            "required_fields": ["symbol", "timestamp"],
            "completeness_threshold": 0.95,
            "quality_threshold": 0.8
        }
        
        # Merge with user config
        self.rules = {**self.default_rules, **self.validation_rules}
        
    def validate_dataframe(self, df: DataFrame, data_type: str = "unknown") -> DataFrame:
        """
        Perform comprehensive validation on DataFrame.
        
        Args:
            df: Input DataFrame to validate
            data_type: Type of data (quotes, trades, bars)
            
        Returns:
            DataFrame: DataFrame with validation flags and metrics
        """
        try:
            self.logger.info("Starting data validation", 
                           data_type=data_type, 
                           row_count=df.count())
            
            # Apply validation rules based on data type
            if data_type == "quotes":
                df_validated = self._validate_quotes(df)
            elif data_type == "trades":
                df_validated = self._validate_trades(df)
            elif data_type == "bars":
                df_validated = self._validate_bars(df)
            else:
                df_validated = self._validate_generic(df)
                
            # Add overall validation summary
            df_final = self._add_validation_summary(df_validated)
            
            self.logger.info("Data validation completed")
            return df_final
            
        except Exception as e:
            self.logger.error("Data validation failed", error=str(e))
            raise
            
    def _validate_quotes(self, df: DataFrame) -> DataFrame:
        """Validate market data quotes."""
        df_validated = df
        
        # Validate required fields
        df_validated = self._validate_required_fields(df_validated, 
                                                    ["symbol", "timestamp", "bid_price", "ask_price"])
        
        # Validate price fields
        for price_field in ["bid_price", "ask_price", "last_price"]:
            if price_field in df.columns:
                df_validated = self._validate_price_range(df_validated, price_field)
                
        # Validate spread
        if "bid_price" in df.columns and "ask_price" in df.columns:
            df_validated = self._validate_spread(df_validated)
            
        # Validate symbol format
        df_validated = self._validate_symbol_format(df_validated)
        
        # Validate timestamp format
        df_validated = self._validate_timestamp_format(df_validated)
        
        return df_validated
        
    def _validate_trades(self, df: DataFrame) -> DataFrame:
        """Validate market data trades."""
        df_validated = df
        
        # Validate required fields
        df_validated = self._validate_required_fields(df_validated,
                                                    ["symbol", "timestamp", "price", "size"])
        
        # Validate price
        if "price" in df.columns:
            df_validated = self._validate_price_range(df_validated, "price")
            
        # Validate volume/size
        if "size" in df.columns:
            df_validated = self._validate_volume_range(df_validated, "size")
            
        # Validate symbol and timestamp
        df_validated = self._validate_symbol_format(df_validated)
        df_validated = self._validate_timestamp_format(df_validated)
        
        return df_validated
        
    def _validate_bars(self, df: DataFrame) -> DataFrame:
        """Validate OHLCV bar data."""
        df_validated = df
        
        # Validate required fields
        df_validated = self._validate_required_fields(df_validated,
                                                    ["symbol", "timestamp", "open", "high", "low", "close", "volume"])
        
        # Validate OHLC prices
        for price_field in ["open", "high", "low", "close"]:
            if price_field in df.columns:
                df_validated = self._validate_price_range(df_validated, price_field)
                
        # Validate OHLC relationships
        df_validated = self._validate_ohlc_relationships(df_validated)
        
        # Validate volume
        if "volume" in df.columns:
            df_validated = self._validate_volume_range(df_validated, "volume")
            
        # Validate symbol and timestamp
        df_validated = self._validate_symbol_format(df_validated)
        df_validated = self._validate_timestamp_format(df_validated)
        
        return df_validated
        
    def _validate_generic(self, df: DataFrame) -> DataFrame:
        """Generic validation for unknown data types."""
        df_validated = df
        
        # Basic validations that apply to all data types
        df_validated = self._validate_symbol_format(df_validated)
        df_validated = self._validate_timestamp_format(df_validated)
        
        # Validate any price-like fields
        price_fields = [col_name for col_name in df.columns 
                       if "price" in col_name.lower()]
        for field in price_fields:
            df_validated = self._validate_price_range(df_validated, field)
            
        # Validate any volume-like fields
        volume_fields = [col_name for col_name in df.columns 
                        if any(keyword in col_name.lower() for keyword in ["volume", "size", "quantity"])]
        for field in volume_fields:
            df_validated = self._validate_volume_range(df_validated, field)
            
        return df_validated
        
    def _validate_required_fields(self, df: DataFrame, required_fields: List[str]) -> DataFrame:
        """Validate presence of required fields."""
        for field in required_fields:
            if field in df.columns:
                df = df.withColumn(
                    f"is_valid_{field}_present",
                    when(col(field).isNotNull() & (col(field) != ""), True).otherwise(False)
                )
            else:
                df = df.withColumn(f"is_valid_{field}_present", lit(False))
                
        return df
        
    def _validate_price_range(self, df: DataFrame, price_field: str) -> DataFrame:
        """Validate price is within reasonable range."""
        if price_field not in df.columns:
            return df.withColumn(f"is_valid_{price_field}_range", lit(False))
            
        min_price = self.rules["price_range"]["min"]
        max_price = self.rules["price_range"]["max"]
        
        df = df.withColumn(
            f"is_valid_{price_field}_range",
            when(
                col(price_field).isNotNull() &
                (~isnan(col(price_field))) &
                (col(price_field) >= min_price) &
                (col(price_field) <= max_price),
                True
            ).otherwise(False)
        )
        
        return df
        
    def _validate_volume_range(self, df: DataFrame, volume_field: str) -> DataFrame:
        """Validate volume is within reasonable range."""
        if volume_field not in df.columns:
            return df.withColumn(f"is_valid_{volume_field}_range", lit(False))
            
        min_volume = self.rules["volume_range"]["min"]
        max_volume = self.rules["volume_range"]["max"]
        
        df = df.withColumn(
            f"is_valid_{volume_field}_range",
            when(
                col(volume_field).isNotNull() &
                (~isnan(col(volume_field))) &
                (col(volume_field) >= min_volume) &
                (col(volume_field) <= max_volume),
                True
            ).otherwise(False)
        )
        
        return df
        
    def _validate_spread(self, df: DataFrame) -> DataFrame:
        """Validate bid-ask spread is reasonable."""
        if "bid_price" not in df.columns or "ask_price" not in df.columns:
            return df.withColumn("is_valid_spread", lit(False))
            
        max_spread_bps = self.rules["spread_range"]["max"]
        
        df = df.withColumn(
            "spread_bps",
            when(
                col("bid_price").isNotNull() &
                col("ask_price").isNotNull() &
                (col("bid_price") > 0),
                ((col("ask_price") - col("bid_price")) / col("bid_price")) * 10000
            ).otherwise(None)
        ).withColumn(
            "is_valid_spread",
            when(
                col("spread_bps").isNotNull() &
                (col("spread_bps") >= 0) &
                (col("spread_bps") <= max_spread_bps),
                True
            ).otherwise(False)
        )
        
        return df
        
    def _validate_ohlc_relationships(self, df: DataFrame) -> DataFrame:
        """Validate OHLC price relationships."""
        required_fields = ["open", "high", "low", "close"]
        if not all(field in df.columns for field in required_fields):
            return df.withColumn("is_valid_ohlc", lit(False))
            
        df = df.withColumn(
            "is_valid_ohlc",
            when(
                # High >= Low, Open, Close
                (col("high") >= col("low")) &
                (col("high") >= col("open")) &
                (col("high") >= col("close")) &
                # Low <= Open, Close
                (col("low") <= col("open")) &
                (col("low") <= col("close")),
                True
            ).otherwise(False)
        )
        
        return df
        
    def _validate_symbol_format(self, df: DataFrame) -> DataFrame:
        """Validate symbol follows expected pattern."""
        if "symbol" not in df.columns:
            return df.withColumn("is_valid_symbol_format", lit(False))
            
        pattern = self.rules["symbol_pattern"]
        
        df = df.withColumn(
            "is_valid_symbol_format",
            when(
                regexp_extract(col("symbol"), pattern, 0) == col("symbol"),
                True
            ).otherwise(False)
        )
        
        return df
        
    def _validate_timestamp_format(self, df: DataFrame) -> DataFrame:
        """Validate timestamp format."""
        if "timestamp" not in df.columns:
            return df.withColumn("is_valid_timestamp_format", lit(False))
            
        # Basic timestamp validation
        df = df.withColumn(
            "is_valid_timestamp_format",
            when(
                col("timestamp").isNotNull() &
                (length(col("timestamp")) >= 19),  # Basic length check
                True
            ).otherwise(False)
        )
        
        return df
        
    def _add_validation_summary(self, df: DataFrame) -> DataFrame:
        """Add overall validation summary to DataFrame."""
        # Get all validation columns
        validation_cols = [col_name for col_name in df.columns 
                          if col_name.startswith("is_valid_")]
        
        if not validation_cols:
            return df.withColumn("validation_score", lit(0.0)).withColumn("is_valid", lit(False))
            
        # Calculate validation score
        validation_sum = sum([col(c).cast("int") for c in validation_cols])
        
        df = df.withColumn(
            "validation_score",
            validation_sum / len(validation_cols)
        ).withColumn(
            "validation_passed_count",
            validation_sum
        ).withColumn(
            "validation_total_count", 
            lit(len(validation_cols))
        ).withColumn(
            "is_valid",
            col("validation_score") >= self.rules["quality_threshold"]
        ).withColumn(
            "validation_timestamp",
            current_timestamp()
        )
        
        return df
        
    def get_validation_summary(self, df: DataFrame) -> Dict[str, Any]:
        """
        Get validation summary statistics.
        
        Args:
            df: Validated DataFrame
            
        Returns:
            Dictionary containing validation statistics
        """
        try:
            # Calculate summary statistics
            total_records = df.count()
            
            if total_records == 0:
                return {"error": "No records to validate"}
                
            summary_stats = df.agg(
                count("*").alias("total_records"),
                spark_sum(col("is_valid").cast("int")).alias("valid_records"),
                avg("validation_score").alias("avg_validation_score"),
                spark_min("validation_score").alias("min_validation_score"),
                spark_max("validation_score").alias("max_validation_score")
            ).collect()[0]
            
            valid_records = summary_stats["valid_records"]
            avg_score = summary_stats["avg_validation_score"]
            
            # Calculate validation rates for each rule
            validation_cols = [col_name for col_name in df.columns 
                             if col_name.startswith("is_valid_")]
            
            rule_stats = {}
            for col_name in validation_cols:
                rule_name = col_name.replace("is_valid_", "")
                passed_count = df.agg(spark_sum(col(col_name).cast("int")).alias("passed")).collect()[0]["passed"]
                rule_stats[rule_name] = {
                    "passed": passed_count,
                    "failed": total_records - passed_count,
                    "pass_rate": passed_count / total_records if total_records > 0 else 0
                }
                
            summary = {
                "total_records": total_records,
                "valid_records": valid_records,
                "invalid_records": total_records - valid_records,
                "overall_pass_rate": valid_records / total_records if total_records > 0 else 0,
                "average_validation_score": float(avg_score) if avg_score else 0.0,
                "min_validation_score": float(summary_stats["min_validation_score"]) if summary_stats["min_validation_score"] else 0.0,
                "max_validation_score": float(summary_stats["max_validation_score"]) if summary_stats["max_validation_score"] else 0.0,
                "rule_statistics": rule_stats,
                "quality_threshold": self.rules["quality_threshold"],
                "meets_quality_threshold": (valid_records / total_records) >= self.rules["completeness_threshold"] if total_records > 0 else False
            }
            
            return summary
            
        except Exception as e:
            self.logger.error("Failed to generate validation summary", error=str(e))
            return {"error": str(e)}