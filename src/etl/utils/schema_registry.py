"""
Schema registry for managing and evolving data schemas.
"""

from typing import Dict, Any, List, Optional
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType, BooleanType
import json
import structlog

logger = structlog.get_logger(__name__)


class SchemaRegistry:
    """
    Schema registry for managing data schemas and evolution.
    
    Features:
    - Schema versioning
    - Schema validation
    - Schema evolution
    - Compatibility checking
    - Schema storage and retrieval
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize schema registry.
        
        Args:
            config: Schema registry configuration
        """
        self.config = config
        self.logger = logger.bind(component="SchemaRegistry")
        
        # Schema storage (in production, this would be external storage)
        self.schemas: Dict[str, Dict[str, StructType]] = {}
        
        # Initialize built-in schemas
        self._initialize_builtin_schemas()
        
    def _initialize_builtin_schemas(self):
        """Initialize built-in schemas for market data."""
        # Market data quote schema v1.0
        quote_schema_v1 = StructType([
            StructField("symbol", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("bid_price", StringType(), True),
            StructField("ask_price", StringType(), True),
            StructField("bid_size", StringType(), True),
            StructField("ask_size", StringType(), True),
            StructField("last_price", StringType(), True),
            StructField("last_size", StringType(), True),
            StructField("volume", StringType(), True),
            StructField("data_source", StringType(), False)
        ])
        
        # Market data trade schema v1.0
        trade_schema_v1 = StructType([
            StructField("symbol", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("price", StringType(), False),
            StructField("size", StringType(), False),
            StructField("exchange", StringType(), True),
            StructField("conditions", StringType(), True),
            StructField("data_source", StringType(), False)
        ])
        
        # Market data bar schema v1.0
        bar_schema_v1 = StructType([
            StructField("symbol", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("open", StringType(), False),
            StructField("high", StringType(), False),
            StructField("low", StringType(), False),
            StructField("close", StringType(), False),
            StructField("volume", StringType(), False),
            StructField("timeframe", StringType(), False),
            StructField("data_source", StringType(), False)
        ])
        
        # Silver layer schema (enhanced)
        silver_schema_v1 = StructType([
            StructField("symbol_clean", StringType(), False),
            StructField("timestamp_parsed", TimestampType(), False),
            StructField("bid_price_decimal", DoubleType(), True),
            StructField("ask_price_decimal", DoubleType(), True),
            StructField("last_price_decimal", DoubleType(), True),
            StructField("price_decimal", DoubleType(), True),
            StructField("volume_long", LongType(), True),
            StructField("size_long", LongType(), True),
            StructField("mid_price", DoubleType(), True),
            StructField("spread_bps", DoubleType(), True),
            StructField("is_silver_quality", BooleanType(), False),
            StructField("overall_quality_score", DoubleType(), False),
            StructField("silver_processed_timestamp", TimestampType(), False),
            StructField("data_source", StringType(), False),
            StructField("topic", StringType(), False)
        ])
        
        # Gold layer OHLCV schema
        gold_ohlcv_schema_v1 = StructType([
            StructField("symbol", StringType(), False),
            StructField("window_start", TimestampType(), False),
            StructField("window_end", TimestampType(), False),
            StructField("timeframe", StringType(), False),
            StructField("open_price", DoubleType(), True),
            StructField("high_price", DoubleType(), True),
            StructField("low_price", DoubleType(), True),
            StructField("close_price", DoubleType(), True),
            StructField("volume", LongType(), True),
            StructField("trade_count", LongType(), True),
            StructField("vwap", DoubleType(), True),
            StructField("price_change_pct", DoubleType(), True),
            StructField("aggregation_type", StringType(), False),
            StructField("gold_processed_timestamp", TimestampType(), False)
        ])
        
        # Register schemas
        self.register_schema("market_data_quotes", "1.0", quote_schema_v1)
        self.register_schema("market_data_trades", "1.0", trade_schema_v1)
        self.register_schema("market_data_bars", "1.0", bar_schema_v1)
        self.register_schema("silver_layer", "1.0", silver_schema_v1)
        self.register_schema("gold_ohlcv", "1.0", gold_ohlcv_schema_v1)
        
    def register_schema(self, schema_name: str, version: str, schema: StructType) -> bool:
        """
        Register a new schema version.
        
        Args:
            schema_name: Name of the schema
            version: Schema version
            schema: Spark StructType schema
            
        Returns:
            True if registration successful
        """
        try:
            if schema_name not in self.schemas:
                self.schemas[schema_name] = {}
                
            self.schemas[schema_name][version] = schema
            
            self.logger.info("Schema registered successfully", 
                           schema_name=schema_name, 
                           version=version,
                           field_count=len(schema.fields))
            
            return True
            
        except Exception as e:
            self.logger.error("Failed to register schema", 
                            schema_name=schema_name, 
                            version=version, 
                            error=str(e))
            return False
            
    def get_schema(self, schema_name: str, version: Optional[str] = None) -> Optional[StructType]:
        """
        Get schema by name and version.
        
        Args:
            schema_name: Name of the schema
            version: Schema version (latest if None)
            
        Returns:
            StructType schema or None if not found
        """
        try:
            if schema_name not in self.schemas:
                self.logger.warning("Schema not found", schema_name=schema_name)
                return None
                
            schema_versions = self.schemas[schema_name]
            
            if version is None:
                # Get latest version
                latest_version = max(schema_versions.keys())
                version = latest_version
                
            if version not in schema_versions:
                self.logger.warning("Schema version not found", 
                                  schema_name=schema_name, 
                                  version=version)
                return None
                
            return schema_versions[version]
            
        except Exception as e:
            self.logger.error("Failed to get schema", 
                            schema_name=schema_name, 
                            version=version, 
                            error=str(e))
            return None
            
    def list_schemas(self) -> Dict[str, List[str]]:
        """
        List all registered schemas and their versions.
        
        Returns:
            Dictionary mapping schema names to version lists
        """
        result = {}
        for schema_name, versions in self.schemas.items():
            result[schema_name] = list(versions.keys())
        return result
        
    def validate_schema_evolution(self, schema_name: str, old_version: str, new_schema: StructType) -> Dict[str, Any]:
        """
        Validate schema evolution compatibility.
        
        Args:
            schema_name: Name of the schema
            old_version: Previous schema version
            new_schema: New schema to validate
            
        Returns:
            Validation result with compatibility information
        """
        try:
            old_schema = self.get_schema(schema_name, old_version)
            if old_schema is None:
                return {"valid": False, "error": "Old schema version not found"}
                
            # Check compatibility
            compatibility_result = self._check_schema_compatibility(old_schema, new_schema)
            
            return {
                "valid": compatibility_result["compatible"],
                "changes": compatibility_result["changes"],
                "breaking_changes": compatibility_result["breaking_changes"],
                "compatibility_type": compatibility_result["compatibility_type"]
            }
            
        except Exception as e:
            self.logger.error("Schema evolution validation failed", error=str(e))
            return {"valid": False, "error": str(e)}
            
    def _check_schema_compatibility(self, old_schema: StructType, new_schema: StructType) -> Dict[str, Any]:
        """Check compatibility between two schemas."""
        old_fields = {field.name: field for field in old_schema.fields}
        new_fields = {field.name: field for field in new_schema.fields}
        
        changes = []
        breaking_changes = []
        
        # Check for removed fields
        for field_name in old_fields:
            if field_name not in new_fields:
                change = f"Field '{field_name}' removed"
                changes.append(change)
                if not old_fields[field_name].nullable:
                    breaking_changes.append(change)
                    
        # Check for added fields
        for field_name in new_fields:
            if field_name not in old_fields:
                change = f"Field '{field_name}' added"
                changes.append(change)
                if not new_fields[field_name].nullable:
                    breaking_changes.append(change)
                    
        # Check for modified fields
        for field_name in old_fields:
            if field_name in new_fields:
                old_field = old_fields[field_name]
                new_field = new_fields[field_name]
                
                # Check type changes
                if old_field.dataType != new_field.dataType:
                    change = f"Field '{field_name}' type changed from {old_field.dataType} to {new_field.dataType}"
                    changes.append(change)
                    breaking_changes.append(change)
                    
                # Check nullability changes
                if old_field.nullable != new_field.nullable:
                    if new_field.nullable and not old_field.nullable:
                        changes.append(f"Field '{field_name}' became nullable")
                    elif not new_field.nullable and old_field.nullable:
                        change = f"Field '{field_name}' became non-nullable"
                        changes.append(change)
                        breaking_changes.append(change)
                        
        # Determine compatibility type
        if breaking_changes:
            compatibility_type = "incompatible"
            compatible = False
        elif changes:
            compatibility_type = "backward_compatible"
            compatible = True
        else:
            compatibility_type = "identical"
            compatible = True
            
        return {
            "compatible": compatible,
            "compatibility_type": compatibility_type,
            "changes": changes,
            "breaking_changes": breaking_changes
        }
        
    def evolve_schema(self, schema_name: str, new_schema: StructType, new_version: str, 
                     force: bool = False) -> Dict[str, Any]:
        """
        Evolve schema to a new version.
        
        Args:
            schema_name: Name of the schema
            new_schema: New schema version
            new_version: Version string for new schema
            force: Force evolution even if incompatible
            
        Returns:
            Evolution result
        """
        try:
            # Get current latest version
            current_versions = self.schemas.get(schema_name, {})
            if current_versions:
                latest_version = max(current_versions.keys())
                
                # Validate evolution
                validation_result = self.validate_schema_evolution(schema_name, latest_version, new_schema)
                
                if not validation_result["valid"] and not force:
                    return {
                        "success": False,
                        "error": "Schema evolution validation failed",
                        "validation_result": validation_result
                    }
                    
            # Register new schema version
            success = self.register_schema(schema_name, new_version, new_schema)
            
            if success:
                return {
                    "success": True,
                    "schema_name": schema_name,
                    "new_version": new_version,
                    "validation_result": validation_result if current_versions else None
                }
            else:
                return {
                    "success": False,
                    "error": "Failed to register new schema version"
                }
                
        except Exception as e:
            self.logger.error("Schema evolution failed", error=str(e))
            return {"success": False, "error": str(e)}
            
    def get_schema_metadata(self, schema_name: str, version: str) -> Dict[str, Any]:
        """
        Get metadata about a schema.
        
        Args:
            schema_name: Name of the schema
            version: Schema version
            
        Returns:
            Schema metadata
        """
        schema = self.get_schema(schema_name, version)
        if schema is None:
            return {"error": "Schema not found"}
            
        field_info = []
        for field in schema.fields:
            field_info.append({
                "name": field.name,
                "type": str(field.dataType),
                "nullable": field.nullable,
                "metadata": dict(field.metadata) if field.metadata else {}
            })
            
        return {
            "schema_name": schema_name,
            "version": version,
            "field_count": len(schema.fields),
            "fields": field_info,
            "json_schema": schema.json()
        }
        
    def export_schema(self, schema_name: str, version: str, format: str = "json") -> Optional[str]:
        """
        Export schema in specified format.
        
        Args:
            schema_name: Name of the schema
            version: Schema version
            format: Export format (json, ddl)
            
        Returns:
            Exported schema string
        """
        schema = self.get_schema(schema_name, version)
        if schema is None:
            return None
            
        try:
            if format == "json":
                return schema.json()
            elif format == "ddl":
                return schema.simpleString()
            else:
                self.logger.warning("Unsupported export format", format=format)
                return None
                
        except Exception as e:
            self.logger.error("Schema export failed", error=str(e))
            return None