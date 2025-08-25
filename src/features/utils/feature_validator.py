"""
Feature Validation Utilities

Provides comprehensive validation for feature metadata and data quality.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union
import re
import pandas as pd
import numpy as np
from pydantic import BaseModel

from ..store.feature_metadata import FeatureMetadata, FeatureType, FeatureValidationRule


logger = logging.getLogger(__name__)


class ValidationResult(BaseModel):
    """Result of validation operation."""
    
    is_valid: bool
    errors: List[str] = []
    warnings: List[str] = []
    metadata: Dict[str, Any] = {}


class FeatureValidator:
    """
    Comprehensive feature validation system.
    
    Provides validation for:
    - Feature metadata compliance
    - Data type and schema validation
    - Data quality rules
    - Business logic constraints
    """
    
    def __init__(self):
        # Standard validation rules
        self.standard_rules = {
            'feature_id_format': r'^[a-zA-Z][a-zA-Z0-9_]*$',
            'namespace_format': r'^[a-zA-Z][a-zA-Z0-9_]*$',
            'max_feature_id_length': 64,
            'max_description_length': 1000
        }
        
        # Data quality thresholds
        self.quality_thresholds = {
            'min_completeness': 0.95,  # 95% non-null values
            'max_cardinality_ratio': 0.5,  # Unique values / total values
            'max_outlier_ratio': 0.05,  # 5% outliers acceptable
            'min_variance_threshold': 1e-8  # Minimum variance for numeric features
        }
    
    async def validate_metadata(self, metadata: FeatureMetadata) -> ValidationResult:
        """
        Validate feature metadata for compliance and consistency.
        
        Args:
            metadata: Feature metadata to validate
            
        Returns:
            Validation result
        """
        result = ValidationResult(is_valid=True)
        
        try:
            # Validate feature ID format
            if not re.match(self.standard_rules['feature_id_format'], metadata.feature_id):
                result.errors.append(
                    f"Invalid feature_id format: {metadata.feature_id}. "
                    "Must start with letter, contain only alphanumeric and underscore"
                )
            
            # Validate feature ID length
            if len(metadata.feature_id) > self.standard_rules['max_feature_id_length']:
                result.errors.append(
                    f"Feature ID too long: {len(metadata.feature_id)} > "
                    f"{self.standard_rules['max_feature_id_length']}"
                )
            
            # Validate namespace format
            if not re.match(self.standard_rules['namespace_format'], metadata.namespace):
                result.errors.append(
                    f"Invalid namespace format: {metadata.namespace}"
                )
            
            # Validate description length
            if len(metadata.description) > self.standard_rules['max_description_length']:
                result.warnings.append(
                    f"Description very long: {len(metadata.description)} characters"
                )
            
            # Validate version format
            version_pattern = r'^\d+\.\d+\.\d+$'
            if not re.match(version_pattern, metadata.version):
                result.errors.append(
                    f"Invalid version format: {metadata.version}. "
                    "Must be semantic version (e.g., 1.0.0)"
                )
            
            # Validate window size
            if metadata.window_size is not None and metadata.window_size < 1:
                result.errors.append(
                    f"Invalid window size: {metadata.window_size}. Must be >= 1"
                )
            
            # Validate parameter types
            if metadata.parameters:
                for param_name, param_value in metadata.parameters.items():
                    if not isinstance(param_name, str):
                        result.errors.append(f"Parameter name must be string: {param_name}")
                    
                    # Check for reasonable parameter values
                    if isinstance(param_value, (int, float)) and param_value < 0:
                        result.warnings.append(
                            f"Negative parameter value: {param_name}={param_value}"
                        )
            
            # Validate schema constraints
            schema_validation = await self._validate_schema_constraints(metadata)
            result.errors.extend(schema_validation.errors)
            result.warnings.extend(schema_validation.warnings)
            
            # Validate dependencies
            if metadata.dependencies:
                for dep in metadata.dependencies:
                    if not re.match(self.standard_rules['feature_id_format'], dep):
                        result.errors.append(
                            f"Invalid dependency feature ID: {dep}"
                        )
            
            # Business logic validations
            business_validation = await self._validate_business_logic(metadata)
            result.errors.extend(business_validation.errors)
            result.warnings.extend(business_validation.warnings)
            
            result.is_valid = len(result.errors) == 0
            
            return result
            
        except Exception as e:
            logger.error(f"Error validating metadata: {e}")
            result.is_valid = False
            result.errors.append(f"Validation error: {e}")
            return result
    
    async def validate_data(
        self,
        metadata: FeatureMetadata,
        data: Union[pd.DataFrame, pd.Series, Any]
    ) -> ValidationResult:
        """
        Validate feature data against metadata and quality rules.
        
        Args:
            metadata: Feature metadata
            data: Feature data to validate
            
        Returns:
            Validation result
        """
        result = ValidationResult(is_valid=True)
        
        try:
            # Convert to DataFrame if necessary
            if isinstance(data, pd.Series):
                df = data.to_frame(name=metadata.schema.name)
            elif isinstance(data, pd.DataFrame):
                df = data
            else:
                result.errors.append(f"Unsupported data type: {type(data)}")
                result.is_valid = False
                return result
            
            # Basic data presence check
            if df.empty:
                result.errors.append("No data provided")
                result.is_valid = False
                return result
            
            # Schema validation
            schema_result = await self._validate_data_schema(metadata, df)
            result.errors.extend(schema_result.errors)
            result.warnings.extend(schema_result.warnings)
            
            # Data type validation
            dtype_result = await self._validate_data_types(metadata, df)
            result.errors.extend(dtype_result.errors)
            result.warnings.extend(dtype_result.warnings)
            
            # Data quality validation
            quality_result = await self._validate_data_quality(metadata, df)
            result.errors.extend(quality_result.errors)
            result.warnings.extend(quality_result.warnings)
            result.metadata.update(quality_result.metadata)
            
            # Constraint validation
            constraint_result = await self._validate_constraints(metadata, df)
            result.errors.extend(constraint_result.errors)
            result.warnings.extend(constraint_result.warnings)
            
            result.is_valid = len(result.errors) == 0
            
            return result
            
        except Exception as e:
            logger.error(f"Error validating data: {e}")
            result.is_valid = False
            result.errors.append(f"Data validation error: {e}")
            return result
    
    async def validate_rules(
        self,
        metadata: FeatureMetadata,
        data: pd.DataFrame,
        rules: List[FeatureValidationRule]
    ) -> ValidationResult:
        """
        Validate data against custom validation rules.
        
        Args:
            metadata: Feature metadata
            data: Feature data
            rules: Custom validation rules
            
        Returns:
            Validation result
        """
        result = ValidationResult(is_valid=True)
        
        try:
            for rule in rules:
                if not rule.is_active:
                    continue
                
                rule_result = await self._apply_validation_rule(metadata, data, rule)
                
                if rule.severity == "error":
                    result.errors.extend(rule_result.errors)
                elif rule.severity == "warning":
                    result.warnings.extend(rule_result.errors)  # Treat rule errors as warnings
                
            result.is_valid = len(result.errors) == 0
            
            return result
            
        except Exception as e:
            logger.error(f"Error validating rules: {e}")
            result.is_valid = False
            result.errors.append(f"Rule validation error: {e}")
            return result
    
    async def _validate_schema_constraints(self, metadata: FeatureMetadata) -> ValidationResult:
        """Validate schema-level constraints."""
        result = ValidationResult(is_valid=True)
        
        # Validate feature type specific constraints
        if metadata.schema.feature_type == FeatureType.FLOAT:
            if 'min_value' in metadata.schema.constraints:
                if not isinstance(metadata.schema.constraints['min_value'], (int, float)):
                    result.errors.append("min_value constraint must be numeric for FLOAT type")
        
        elif metadata.schema.feature_type == FeatureType.STRING:
            if 'max_length' in metadata.schema.constraints:
                if not isinstance(metadata.schema.constraints['max_length'], int):
                    result.errors.append("max_length constraint must be integer for STRING type")
        
        return result
    
    async def _validate_business_logic(self, metadata: FeatureMetadata) -> ValidationResult:
        """Validate business logic constraints."""
        result = ValidationResult(is_valid=True)
        
        # Category-specific validations
        if metadata.category.value == "momentum":
            # Momentum indicators should have reasonable periods
            if 'period' in metadata.parameters:
                period = metadata.parameters['period']
                if isinstance(period, int) and (period < 2 or period > 500):
                    result.warnings.append(
                        f"Unusual period for momentum indicator: {period}"
                    )
        
        elif metadata.category.value == "volatility":
            # Volatility indicators should have positive multipliers
            if 'std_dev' in metadata.parameters:
                std_dev = metadata.parameters['std_dev']
                if isinstance(std_dev, (int, float)) and std_dev <= 0:
                    result.errors.append(
                        f"Standard deviation multiplier must be positive: {std_dev}"
                    )
        
        return result
    
    async def _validate_data_schema(
        self,
        metadata: FeatureMetadata,
        data: pd.DataFrame
    ) -> ValidationResult:
        """Validate data schema against metadata."""
        result = ValidationResult(is_valid=True)
        
        # Check for required columns
        expected_columns = {metadata.schema.name}
        
        # Add entity and timestamp columns if they should be present
        for col in data.columns:
            if col in ['entity_id', 'symbol', 'timestamp']:
                expected_columns.add(col)
        
        missing_columns = expected_columns - set(data.columns)
        if missing_columns:
            result.warnings.append(f"Missing expected columns: {missing_columns}")
        
        return result
    
    async def _validate_data_types(
        self,
        metadata: FeatureMetadata,
        data: pd.DataFrame
    ) -> ValidationResult:
        """Validate data types."""
        result = ValidationResult(is_valid=True)
        
        feature_column = metadata.schema.name
        if feature_column not in data.columns:
            result.errors.append(f"Feature column '{feature_column}' not found in data")
            return result
        
        series = data[feature_column]
        expected_type = metadata.schema.feature_type
        
        # Type-specific validation
        if expected_type == FeatureType.FLOAT:
            if not pd.api.types.is_numeric_dtype(series):
                result.errors.append(f"Expected numeric type for {feature_column}, got {series.dtype}")
        
        elif expected_type == FeatureType.INTEGER:
            if not pd.api.types.is_integer_dtype(series):
                if pd.api.types.is_numeric_dtype(series):
                    result.warnings.append(f"Float values for integer feature {feature_column}")
                else:
                    result.errors.append(f"Expected integer type for {feature_column}, got {series.dtype}")
        
        elif expected_type == FeatureType.STRING:
            if not pd.api.types.is_string_dtype(series) and not pd.api.types.is_object_dtype(series):
                result.errors.append(f"Expected string type for {feature_column}, got {series.dtype}")
        
        elif expected_type == FeatureType.BOOLEAN:
            if not pd.api.types.is_bool_dtype(series):
                result.errors.append(f"Expected boolean type for {feature_column}, got {series.dtype}")
        
        return result
    
    async def _validate_data_quality(
        self,
        metadata: FeatureMetadata,
        data: pd.DataFrame
    ) -> ValidationResult:
        """Validate data quality metrics."""
        result = ValidationResult(is_valid=True)
        
        feature_column = metadata.schema.name
        if feature_column not in data.columns:
            return result
        
        series = data[feature_column]
        total_count = len(series)
        
        if total_count == 0:
            result.errors.append("No data rows found")
            return result
        
        # Completeness check
        null_count = series.isnull().sum()
        completeness = (total_count - null_count) / total_count
        
        result.metadata['completeness'] = completeness
        result.metadata['null_count'] = int(null_count)
        result.metadata['total_count'] = total_count
        
        if not metadata.schema.nullable and null_count > 0:
            result.errors.append(f"Null values found in non-nullable feature: {null_count}")
        
        if completeness < self.quality_thresholds['min_completeness']:
            result.warnings.append(
                f"Low completeness: {completeness:.2%} < {self.quality_thresholds['min_completeness']:.2%}"
            )
        
        # For numeric data, additional quality checks
        if pd.api.types.is_numeric_dtype(series):
            numeric_series = series.dropna()
            
            if len(numeric_series) > 0:
                # Variance check
                variance = numeric_series.var()
                result.metadata['variance'] = float(variance)
                
                if variance < self.quality_thresholds['min_variance_threshold']:
                    result.warnings.append(f"Very low variance detected: {variance}")
                
                # Outlier detection (simple IQR method)
                if len(numeric_series) > 10:  # Need sufficient data for IQR
                    q1 = numeric_series.quantile(0.25)
                    q3 = numeric_series.quantile(0.75)
                    iqr = q3 - q1
                    
                    if iqr > 0:  # Avoid division by zero
                        lower_bound = q1 - 1.5 * iqr
                        upper_bound = q3 + 1.5 * iqr
                        
                        outliers = numeric_series[(numeric_series < lower_bound) | 
                                                 (numeric_series > upper_bound)]
                        outlier_ratio = len(outliers) / len(numeric_series)
                        
                        result.metadata['outlier_ratio'] = outlier_ratio
                        
                        if outlier_ratio > self.quality_thresholds['max_outlier_ratio']:
                            result.warnings.append(
                                f"High outlier ratio: {outlier_ratio:.2%} > "
                                f"{self.quality_thresholds['max_outlier_ratio']:.2%}"
                            )
        
        # Cardinality check (for potential categorical features)
        unique_count = series.nunique()
        cardinality_ratio = unique_count / total_count
        
        result.metadata['unique_count'] = int(unique_count)
        result.metadata['cardinality_ratio'] = cardinality_ratio
        
        if cardinality_ratio > self.quality_thresholds['max_cardinality_ratio']:
            result.warnings.append(
                f"High cardinality: {cardinality_ratio:.2%} unique values"
            )
        
        return result
    
    async def _validate_constraints(
        self,
        metadata: FeatureMetadata,
        data: pd.DataFrame
    ) -> ValidationResult:
        """Validate feature constraints."""
        result = ValidationResult(is_valid=True)
        
        feature_column = metadata.schema.name
        if feature_column not in data.columns or not metadata.schema.constraints:
            return result
        
        series = data[feature_column].dropna()
        constraints = metadata.schema.constraints
        
        # Numeric constraints
        if 'min_value' in constraints:
            min_val = constraints['min_value']
            violations = series[series < min_val]
            if len(violations) > 0:
                result.errors.append(
                    f"{len(violations)} values below minimum {min_val}"
                )
        
        if 'max_value' in constraints:
            max_val = constraints['max_value']
            violations = series[series > max_val]
            if len(violations) > 0:
                result.errors.append(
                    f"{len(violations)} values above maximum {max_val}"
                )
        
        # String constraints
        if 'max_length' in constraints and pd.api.types.is_string_dtype(series):
            max_len = constraints['max_length']
            violations = series[series.str.len() > max_len]
            if len(violations) > 0:
                result.errors.append(
                    f"{len(violations)} strings exceed maximum length {max_len}"
                )
        
        return result
    
    async def _apply_validation_rule(
        self,
        metadata: FeatureMetadata,
        data: pd.DataFrame,
        rule: FeatureValidationRule
    ) -> ValidationResult:
        """Apply a custom validation rule."""
        result = ValidationResult(is_valid=True)
        
        try:
            rule_type = rule.rule_type
            config = rule.rule_config
            
            if rule_type == "range_check":
                # Range validation
                feature_col = metadata.schema.name
                if feature_col in data.columns:
                    series = data[feature_col].dropna()
                    
                    min_val = config.get('min_value')
                    max_val = config.get('max_value')
                    
                    if min_val is not None:
                        violations = len(series[series < min_val])
                        if violations > 0:
                            result.errors.append(f"Range check failed: {violations} values < {min_val}")
                    
                    if max_val is not None:
                        violations = len(series[series > max_val])
                        if violations > 0:
                            result.errors.append(f"Range check failed: {violations} values > {max_val}")
            
            elif rule_type == "completeness_check":
                # Data completeness validation
                threshold = config.get('min_completeness', 0.95)
                feature_col = metadata.schema.name
                
                if feature_col in data.columns:
                    completeness = data[feature_col].notna().sum() / len(data)
                    if completeness < threshold:
                        result.errors.append(
                            f"Completeness check failed: {completeness:.2%} < {threshold:.2%}"
                        )
            
            elif rule_type == "pattern_check":
                # Pattern matching for string features
                pattern = config.get('pattern')
                feature_col = metadata.schema.name
                
                if pattern and feature_col in data.columns:
                    string_data = data[feature_col].dropna().astype(str)
                    matches = string_data.str.match(pattern)
                    violations = len(string_data[~matches])
                    
                    if violations > 0:
                        result.errors.append(
                            f"Pattern check failed: {violations} values don't match pattern"
                        )
            
            else:
                result.warnings.append(f"Unknown validation rule type: {rule_type}")
        
        except Exception as e:
            result.errors.append(f"Rule application error: {e}")
        
        return result