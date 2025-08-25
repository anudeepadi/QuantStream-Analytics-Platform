"""
Feature Metadata Management

Handles feature metadata, versioning, and schema definitions.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union
from enum import Enum
from dataclasses import dataclass, field
from pydantic import BaseModel, Field, validator
import json
import hashlib


class FeatureType(str, Enum):
    """Supported feature data types."""
    FLOAT = "float"
    INTEGER = "integer"
    STRING = "string"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    ARRAY = "array"
    OBJECT = "object"


class IndicatorCategory(str, Enum):
    """Technical indicator categories."""
    TREND = "trend"
    MOMENTUM = "momentum"
    VOLATILITY = "volatility"  
    VOLUME = "volume"
    COMPOSITE = "composite"
    CUSTOM = "custom"


@dataclass
class FeatureVersion:
    """Feature version information."""
    major: int
    minor: int
    patch: int
    
    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"
    
    def __eq__(self, other: 'FeatureVersion') -> bool:
        return (self.major, self.minor, self.patch) == (other.major, other.minor, other.patch)
    
    def __lt__(self, other: 'FeatureVersion') -> bool:
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)
    
    @classmethod
    def from_string(cls, version_str: str) -> 'FeatureVersion':
        """Parse version from string format 'major.minor.patch'."""
        parts = version_str.split('.')
        if len(parts) != 3:
            raise ValueError(f"Invalid version format: {version_str}")
        
        return cls(
            major=int(parts[0]),
            minor=int(parts[1]), 
            patch=int(parts[2])
        )
    
    def increment_patch(self) -> 'FeatureVersion':
        """Create new version with incremented patch number."""
        return FeatureVersion(self.major, self.minor, self.patch + 1)
    
    def increment_minor(self) -> 'FeatureVersion':
        """Create new version with incremented minor number."""
        return FeatureVersion(self.major, self.minor + 1, 0)
    
    def increment_major(self) -> 'FeatureVersion':
        """Create new version with incremented major number."""
        return FeatureVersion(self.major + 1, 0, 0)


class FeatureSchema(BaseModel):
    """Feature schema definition."""
    name: str = Field(..., description="Feature name")
    feature_type: FeatureType = Field(..., description="Feature data type")
    nullable: bool = Field(default=False, description="Whether feature can be null")
    default_value: Optional[Any] = Field(default=None, description="Default value for feature")
    description: str = Field(default="", description="Feature description")
    constraints: Dict[str, Any] = Field(default_factory=dict, description="Feature constraints")
    
    @validator('constraints')
    def validate_constraints(cls, v, values):
        """Validate feature constraints based on type."""
        feature_type = values.get('feature_type')
        if not feature_type:
            return v
            
        if feature_type in [FeatureType.FLOAT, FeatureType.INTEGER]:
            # Validate numeric constraints
            if 'min_value' in v and 'max_value' in v:
                if v['min_value'] > v['max_value']:
                    raise ValueError("min_value cannot be greater than max_value")
        
        return v


class FeatureMetadata(BaseModel):
    """Comprehensive feature metadata."""
    
    # Core identification
    feature_id: str = Field(..., description="Unique feature identifier")
    name: str = Field(..., description="Human-readable feature name")
    namespace: str = Field(default="default", description="Feature namespace/group")
    version: str = Field(..., description="Feature version (semantic)")
    
    # Feature definition
    schema: FeatureSchema = Field(..., description="Feature schema definition")
    category: IndicatorCategory = Field(..., description="Indicator category")
    tags: List[str] = Field(default_factory=list, description="Feature tags")
    
    # Technical configuration
    window_size: Optional[int] = Field(default=None, description="Lookback window size")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Calculation parameters")
    dependencies: List[str] = Field(default_factory=list, description="Dependent feature IDs")
    
    # Metadata
    description: str = Field(default="", description="Detailed feature description")
    calculation_logic: str = Field(default="", description="Calculation logic/formula")
    data_source: str = Field(default="", description="Source of input data")
    
    # Lifecycle information
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    created_by: str = Field(default="system", description="Creator identifier")
    
    # Status and quality
    is_active: bool = Field(default=True, description="Whether feature is active")
    quality_score: Optional[float] = Field(default=None, description="Feature quality score")
    usage_count: int = Field(default=0, description="Usage count")
    
    # Performance metadata  
    avg_computation_time_ms: Optional[float] = Field(default=None, description="Average computation time")
    storage_size_bytes: Optional[int] = Field(default=None, description="Storage size in bytes")
    
    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
    
    def get_version_object(self) -> FeatureVersion:
        """Get version as FeatureVersion object."""
        return FeatureVersion.from_string(self.version)
    
    def set_version(self, version: FeatureVersion) -> None:
        """Set version from FeatureVersion object."""
        self.version = str(version)
        self.updated_at = datetime.now(timezone.utc)
    
    def generate_signature(self) -> str:
        """Generate feature signature for change detection."""
        signature_data = {
            'schema': self.schema.dict(),
            'parameters': self.parameters,
            'dependencies': sorted(self.dependencies),
            'calculation_logic': self.calculation_logic
        }
        
        signature_str = json.dumps(signature_data, sort_keys=True)
        return hashlib.sha256(signature_str.encode()).hexdigest()
    
    def is_compatible_with(self, other: 'FeatureMetadata') -> bool:
        """Check if this feature is compatible with another version."""
        if self.feature_id != other.feature_id:
            return False
            
        # Check schema compatibility
        if self.schema.feature_type != other.schema.feature_type:
            return False
            
        # Check if nullable became non-nullable (breaking change)
        if other.schema.nullable and not self.schema.nullable:
            return False
            
        return True
    
    def should_increment_major(self, other: 'FeatureMetadata') -> bool:
        """Check if changes warrant a major version increment."""
        if not self.is_compatible_with(other):
            return True
            
        # Breaking changes that require major version bump
        breaking_changes = [
            self.schema.feature_type != other.schema.feature_type,
            len(self.dependencies) != len(other.dependencies),
            set(self.dependencies) != set(other.dependencies),
            self.calculation_logic != other.calculation_logic
        ]
        
        return any(breaking_changes)
    
    def should_increment_minor(self, other: 'FeatureMetadata') -> bool:
        """Check if changes warrant a minor version increment."""
        if self.should_increment_major(other):
            return False
            
        # Non-breaking feature additions
        feature_changes = [
            self.schema.description != other.schema.description,
            self.parameters != other.parameters,
            self.window_size != other.window_size,
            set(self.tags) != set(other.tags)
        ]
        
        return any(feature_changes)
    
    def update_usage_stats(self, computation_time_ms: Optional[float] = None) -> None:
        """Update feature usage statistics."""
        self.usage_count += 1
        self.updated_at = datetime.now(timezone.utc)
        
        if computation_time_ms is not None:
            if self.avg_computation_time_ms is None:
                self.avg_computation_time_ms = computation_time_ms
            else:
                # Exponential moving average
                alpha = 0.1
                self.avg_computation_time_ms = (
                    alpha * computation_time_ms + 
                    (1 - alpha) * self.avg_computation_time_ms
                )


class FeatureSet(BaseModel):
    """Collection of related features."""
    
    set_id: str = Field(..., description="Unique feature set identifier")
    name: str = Field(..., description="Feature set name")
    description: str = Field(default="", description="Feature set description")
    features: List[str] = Field(..., description="List of feature IDs")
    version: str = Field(..., description="Feature set version")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    tags: List[str] = Field(default_factory=list, description="Feature set tags")
    
    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
    
    def add_feature(self, feature_id: str) -> None:
        """Add feature to set."""
        if feature_id not in self.features:
            self.features.append(feature_id)
            self.updated_at = datetime.now(timezone.utc)
    
    def remove_feature(self, feature_id: str) -> None:
        """Remove feature from set."""
        if feature_id in self.features:
            self.features.remove(feature_id)
            self.updated_at = datetime.now(timezone.utc)


class FeatureValidationRule(BaseModel):
    """Feature validation rule definition."""
    
    rule_id: str = Field(..., description="Unique rule identifier")
    feature_id: str = Field(..., description="Target feature ID")
    rule_type: str = Field(..., description="Validation rule type")
    rule_config: Dict[str, Any] = Field(..., description="Rule configuration")
    severity: str = Field(default="error", description="Rule severity (error, warning, info)")
    is_active: bool = Field(default=True, description="Whether rule is active")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    class Config:
        """Pydantic configuration.""" 
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }