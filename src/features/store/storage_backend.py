"""
Delta Lake Storage Backend

Provides optimized storage for feature data using Delta Lake format
with time-travel capabilities and efficient querying.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union, Tuple
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max as spark_max, min as spark_min
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
import numpy as np

from .feature_metadata import FeatureMetadata, FeatureType


logger = logging.getLogger(__name__)


class StorageError(Exception):
    """Storage operation error."""
    pass


class DeltaStorageBackend:
    """
    Delta Lake-based storage backend for feature data.
    
    Provides:
    - Efficient columnar storage with compression
    - ACID transactions and schema evolution
    - Time-travel queries for point-in-time features
    - Optimized partitioning and indexing
    - Automatic compaction and optimization
    """
    
    def __init__(
        self,
        storage_path: str,
        spark_config: Optional[Dict[str, str]] = None,
        executor: Optional[ThreadPoolExecutor] = None
    ):
        self.storage_path = Path(storage_path)
        self.spark_config = spark_config or {}
        self.executor = executor or ThreadPoolExecutor(max_workers=4)
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        # Feature table paths
        self.feature_data_path = self.storage_path / "features"
        self.metadata_path = self.storage_path / "metadata"
        
        # Create directories
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.feature_data_path.mkdir(exist_ok=True)
        self.metadata_path.mkdir(exist_ok=True)
    
    def _create_spark_session(self) -> SparkSession:
        """Create optimized Spark session for Delta Lake."""
        builder = (
            SparkSession.builder
            .appName("FeatureStoreDelta")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.parquet.columnarReaderBatchSize", "8192")
            .config("spark.sql.parquet.compression.codec", "snappy")
        )
        
        # Add custom configurations
        for key, value in self.spark_config.items():
            builder = builder.config(key, value)
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    
    async def write_features(
        self,
        feature_id: str,
        data: pd.DataFrame,
        timestamp_column: str = "timestamp",
        entity_columns: Optional[List[str]] = None,
        mode: str = "append"
    ) -> bool:
        """
        Write feature data to Delta Lake.
        
        Args:
            feature_id: Feature identifier
            data: Feature data DataFrame
            timestamp_column: Timestamp column name
            entity_columns: Entity identifier columns
            mode: Write mode (append, overwrite, merge)
            
        Returns:
            Success status
        """
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                self.executor,
                self._write_features_sync,
                feature_id, data, timestamp_column, entity_columns, mode
            )
        except Exception as e:
            logger.error(f"Failed to write features for {feature_id}: {e}")
            return False
    
    def _write_features_sync(
        self,
        feature_id: str,
        data: pd.DataFrame,
        timestamp_column: str,
        entity_columns: Optional[List[str]],
        mode: str
    ) -> bool:
        """Synchronous feature writing."""
        try:
            # Prepare data
            feature_path = self.feature_data_path / feature_id
            
            # Ensure timestamp column is properly typed
            if timestamp_column not in data.columns:
                raise StorageError(f"Timestamp column '{timestamp_column}' not found in data")
            
            # Convert to Spark DataFrame
            df_spark = self.spark.createDataFrame(data)
            
            # Add metadata columns
            df_spark = df_spark.withColumn("_feature_id", lit(feature_id))
            df_spark = df_spark.withColumn("_ingestion_time", lit(datetime.now(timezone.utc)))
            
            # Partition by date for better query performance
            if timestamp_column in data.columns:
                df_spark = df_spark.withColumn(
                    "_partition_date",
                    col(timestamp_column).cast("date")
                )
            
            # Write to Delta Lake
            writer = df_spark.write.format("delta").mode(mode)
            
            # Configure partitioning
            if entity_columns:
                partition_cols = ["_partition_date"] + entity_columns
            else:
                partition_cols = ["_partition_date"]
            
            writer = writer.partitionBy(*partition_cols)
            
            # Write data
            writer.save(str(feature_path))
            
            # Optimize table after write
            if mode in ["append", "overwrite"]:
                self._optimize_table(str(feature_path))
            
            logger.info(f"Successfully wrote {len(data)} records for feature {feature_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error in sync feature write for {feature_id}: {e}")
            return False
    
    async def read_features(
        self,
        feature_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        entities: Optional[List[str]] = None,
        entity_column: str = "entity_id",
        as_of_timestamp: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> Optional[pd.DataFrame]:
        """
        Read feature data from Delta Lake.
        
        Args:
            feature_id: Feature identifier
            start_time: Start time filter
            end_time: End time filter  
            entities: List of entity IDs to filter
            entity_column: Entity column name
            as_of_timestamp: Point-in-time query timestamp
            limit: Maximum rows to return
            
        Returns:
            Feature data DataFrame or None
        """
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                self.executor,
                self._read_features_sync,
                feature_id, start_time, end_time, entities, 
                entity_column, as_of_timestamp, limit
            )
        except Exception as e:
            logger.error(f"Failed to read features for {feature_id}: {e}")
            return None
    
    def _read_features_sync(
        self,
        feature_id: str,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        entities: Optional[List[str]],
        entity_column: str,
        as_of_timestamp: Optional[datetime],
        limit: Optional[int]
    ) -> Optional[pd.DataFrame]:
        """Synchronous feature reading."""
        try:
            feature_path = self.feature_data_path / feature_id
            
            if not feature_path.exists():
                logger.warning(f"Feature path does not exist: {feature_path}")
                return None
            
            # Create Delta table
            delta_table = DeltaTable.forPath(self.spark, str(feature_path))
            
            # Start with full table
            df = delta_table.toDF()
            
            # Apply time-travel query if requested
            if as_of_timestamp:
                version = self._get_version_at_timestamp(delta_table, as_of_timestamp)
                if version is not None:
                    df = self.spark.read.format("delta").option("versionAsOf", version).load(str(feature_path))
            
            # Apply filters
            if start_time:
                df = df.filter(col("timestamp") >= lit(start_time))
            
            if end_time:
                df = df.filter(col("timestamp") <= lit(end_time))
            
            if entities and entity_column in df.columns:
                df = df.filter(col(entity_column).isin(entities))
            
            # Apply limit
            if limit:
                df = df.limit(limit)
            
            # Convert to Pandas
            pandas_df = df.toPandas()
            
            logger.info(f"Retrieved {len(pandas_df)} records for feature {feature_id}")
            return pandas_df
            
        except Exception as e:
            logger.error(f"Error reading feature {feature_id}: {e}")
            return None
    
    async def get_latest_features(
        self,
        feature_ids: List[str],
        entities: List[str],
        entity_column: str = "entity_id"
    ) -> Dict[str, pd.DataFrame]:
        """
        Get latest feature values for specified entities.
        
        Args:
            feature_ids: List of feature identifiers
            entities: List of entity identifiers
            entity_column: Entity column name
            
        Returns:
            Dictionary mapping feature_id to latest values DataFrame
        """
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                self.executor,
                self._get_latest_features_sync,
                feature_ids, entities, entity_column
            )
        except Exception as e:
            logger.error(f"Failed to get latest features: {e}")
            return {}
    
    def _get_latest_features_sync(
        self,
        feature_ids: List[str],
        entities: List[str],
        entity_column: str
    ) -> Dict[str, pd.DataFrame]:
        """Synchronous latest features retrieval."""
        results = {}
        
        for feature_id in feature_ids:
            try:
                feature_path = self.feature_data_path / feature_id
                if not feature_path.exists():
                    continue
                
                # Read Delta table
                df = self.spark.read.format("delta").load(str(feature_path))
                
                # Filter entities
                if entities:
                    df = df.filter(col(entity_column).isin(entities))
                
                # Get latest values per entity using window function
                from pyspark.sql.window import Window
                from pyspark.sql.functions import row_number
                
                window = Window.partitionBy(entity_column).orderBy(col("timestamp").desc())
                df_latest = df.withColumn("row_num", row_number().over(window)) \
                             .filter(col("row_num") == 1) \
                             .drop("row_num")
                
                # Convert to Pandas
                pandas_df = df_latest.toPandas()
                results[feature_id] = pandas_df
                
            except Exception as e:
                logger.error(f"Error getting latest values for feature {feature_id}: {e}")
                continue
        
        return results
    
    async def get_feature_statistics(
        self,
        feature_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get statistical summary of feature data.
        
        Args:
            feature_id: Feature identifier
            start_time: Start time filter
            end_time: End time filter
            
        Returns:
            Dictionary with statistics
        """
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                self.executor,
                self._get_feature_statistics_sync,
                feature_id, start_time, end_time
            )
        except Exception as e:
            logger.error(f"Failed to get statistics for feature {feature_id}: {e}")
            return None
    
    def _get_feature_statistics_sync(
        self,
        feature_id: str,
        start_time: Optional[datetime],
        end_time: Optional[datetime]
    ) -> Optional[Dict[str, Any]]:
        """Synchronous feature statistics calculation."""
        try:
            feature_path = self.feature_data_path / feature_id
            if not feature_path.exists():
                return None
            
            df = self.spark.read.format("delta").load(str(feature_path))
            
            # Apply time filters
            if start_time:
                df = df.filter(col("timestamp") >= lit(start_time))
            if end_time:
                df = df.filter(col("timestamp") <= lit(end_time))
            
            # Calculate statistics for numeric columns
            numeric_cols = [
                field.name for field in df.schema.fields 
                if field.dataType in [DoubleType(), LongType()]
                and not field.name.startswith("_")
            ]
            
            stats = {
                "total_records": df.count(),
                "time_range": {
                    "start": df.agg(spark_min("timestamp")).collect()[0][0],
                    "end": df.agg(spark_max("timestamp")).collect()[0][0]
                }
            }
            
            if numeric_cols:
                # Get descriptive statistics
                desc_stats = df.select(*numeric_cols).describe()
                stats_pandas = desc_stats.toPandas().set_index("summary").to_dict()
                stats["column_statistics"] = stats_pandas
            
            return stats
            
        except Exception as e:
            logger.error(f"Error calculating statistics for {feature_id}: {e}")
            return None
    
    async def compact_feature_table(self, feature_id: str) -> bool:
        """
        Compact and optimize feature table.
        
        Args:
            feature_id: Feature identifier
            
        Returns:
            Success status
        """
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                self.executor,
                self._compact_feature_table_sync,
                feature_id
            )
        except Exception as e:
            logger.error(f"Failed to compact feature table {feature_id}: {e}")
            return False
    
    def _compact_feature_table_sync(self, feature_id: str) -> bool:
        """Synchronous table compaction."""
        try:
            feature_path = self.feature_data_path / feature_id
            if not feature_path.exists():
                return False
            
            delta_table = DeltaTable.forPath(self.spark, str(feature_path))
            
            # Optimize table
            delta_table.optimize().executeCompaction()
            
            # Z-order optimization for better query performance
            delta_table.optimize().executeZOrderBy("timestamp", "_partition_date")
            
            # Vacuum old files (keep 7 days of history)
            delta_table.vacuum(retentionHours=168)
            
            logger.info(f"Successfully compacted feature table {feature_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error compacting feature table {feature_id}: {e}")
            return False
    
    def _optimize_table(self, table_path: str) -> None:
        """Optimize Delta table after write operations."""
        try:
            delta_table = DeltaTable.forPath(self.spark, table_path)
            delta_table.optimize().executeCompaction()
            
        except Exception as e:
            logger.warning(f"Failed to optimize table at {table_path}: {e}")
    
    def _get_version_at_timestamp(
        self,
        delta_table: DeltaTable,
        timestamp: datetime
    ) -> Optional[int]:
        """Get Delta table version closest to specified timestamp."""
        try:
            history = delta_table.history().select("version", "timestamp").orderBy("version").collect()
            
            target_version = None
            for row in history:
                if row.timestamp <= timestamp:
                    target_version = row.version
                else:
                    break
            
            return target_version
            
        except Exception as e:
            logger.error(f"Error getting version at timestamp: {e}")
            return None
    
    async def delete_feature_data(
        self,
        feature_id: str,
        condition: Optional[str] = None
    ) -> bool:
        """
        Delete feature data with optional condition.
        
        Args:
            feature_id: Feature identifier
            condition: SQL condition for selective deletion
            
        Returns:
            Success status
        """
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                self.executor,
                self._delete_feature_data_sync,
                feature_id, condition
            )
        except Exception as e:
            logger.error(f"Failed to delete feature data for {feature_id}: {e}")
            return False
    
    def _delete_feature_data_sync(self, feature_id: str, condition: Optional[str]) -> bool:
        """Synchronous feature data deletion."""
        try:
            feature_path = self.feature_data_path / feature_id
            if not feature_path.exists():
                return True
            
            delta_table = DeltaTable.forPath(self.spark, str(feature_path))
            
            if condition:
                # Conditional delete
                delta_table.delete(condition)
            else:
                # Delete all data
                delta_table.delete()
            
            logger.info(f"Successfully deleted data for feature {feature_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting feature data for {feature_id}: {e}")
            return False
    
    def close(self) -> None:
        """Clean up resources."""
        try:
            self.spark.stop()
            self.executor.shutdown(wait=True)
        except Exception as e:
            logger.error(f"Error closing storage backend: {e}")