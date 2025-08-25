"""
Feature Drift Detection

Detects statistical drift in feature distributions for monitoring
data quality and model performance.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum
import pandas as pd
import numpy as np
from scipy import stats
import warnings

from ..store.feature_store import FeatureStore


logger = logging.getLogger(__name__)


class DriftMethod(str, Enum):
    """Available drift detection methods."""
    KOLMOGOROV_SMIRNOV = "ks_test"
    CHI_SQUARE = "chi_square"
    POPULATION_STABILITY_INDEX = "psi"
    JENSEN_SHANNON_DIVERGENCE = "js_divergence"
    WASSERSTEIN_DISTANCE = "wasserstein"


class DriftSeverity(str, Enum):
    """Drift severity levels."""
    NO_DRIFT = "no_drift"
    LOW = "low"
    MODERATE = "moderate" 
    HIGH = "high"
    SEVERE = "severe"


@dataclass
class DriftResult:
    """Result of drift detection analysis."""
    
    feature_id: str
    method: DriftMethod
    drift_score: float
    p_value: Optional[float]
    threshold: float
    severity: DriftSeverity
    has_drift: bool
    baseline_period: str
    comparison_period: str
    sample_sizes: Dict[str, int]
    metadata: Dict[str, Any]
    timestamp: datetime


class DriftDetector:
    """
    Feature drift detection system using statistical methods
    to identify changes in feature distributions over time.
    """
    
    def __init__(
        self,
        feature_store: FeatureStore,
        default_baseline_days: int = 30,
        default_comparison_days: int = 7
    ):
        self.feature_store = feature_store
        self.default_baseline_days = default_baseline_days
        self.default_comparison_days = default_comparison_days
        
        # Drift thresholds by severity
        self.severity_thresholds = {
            DriftMethod.KOLMOGOROV_SMIRNOV: {
                DriftSeverity.LOW: 0.1,
                DriftSeverity.MODERATE: 0.2,
                DriftSeverity.HIGH: 0.3,
                DriftSeverity.SEVERE: 0.5
            },
            DriftMethod.CHI_SQUARE: {
                DriftSeverity.LOW: 0.05,
                DriftSeverity.MODERATE: 0.01,
                DriftSeverity.HIGH: 0.001,
                DriftSeverity.SEVERE: 0.0001
            },
            DriftMethod.POPULATION_STABILITY_INDEX: {
                DriftSeverity.LOW: 0.1,
                DriftSeverity.MODERATE: 0.25,
                DriftSeverity.HIGH: 0.5,
                DriftSeverity.SEVERE: 1.0
            }
        }
        
        # Default significance levels
        self.significance_levels = {
            DriftMethod.KOLMOGOROV_SMIRNOV: 0.05,
            DriftMethod.CHI_SQUARE: 0.05,
            DriftMethod.POPULATION_STABILITY_INDEX: 0.1,
            DriftMethod.JENSEN_SHANNON_DIVERGENCE: 0.1,
            DriftMethod.WASSERSTEIN_DISTANCE: 0.05
        }
    
    async def detect_drift(
        self,
        feature_id: str,
        method: DriftMethod = DriftMethod.KOLMOGOROV_SMIRNOV,
        baseline_start: Optional[datetime] = None,
        baseline_end: Optional[datetime] = None,
        comparison_start: Optional[datetime] = None,
        comparison_end: Optional[datetime] = None,
        entities: Optional[List[str]] = None,
        significance_level: Optional[float] = None
    ) -> Optional[DriftResult]:
        """
        Detect drift for a specific feature.
        
        Args:
            feature_id: Feature to analyze
            method: Drift detection method
            baseline_start: Baseline period start
            baseline_end: Baseline period end
            comparison_start: Comparison period start
            comparison_end: Comparison period end
            entities: Specific entities to analyze
            significance_level: Statistical significance level
            
        Returns:
            Drift detection result
        """
        try:
            logger.info(f"Detecting drift for feature {feature_id} using {method}")
            
            # Set default time periods if not provided
            now = datetime.now(timezone.utc)
            
            if not comparison_end:
                comparison_end = now
            if not comparison_start:
                comparison_start = comparison_end - timedelta(days=self.default_comparison_days)
            
            if not baseline_end:
                baseline_end = comparison_start
            if not baseline_start:
                baseline_start = baseline_end - timedelta(days=self.default_baseline_days)
            
            # Get data for both periods
            baseline_data = await self._get_feature_data(
                feature_id, baseline_start, baseline_end, entities
            )
            comparison_data = await self._get_feature_data(
                feature_id, comparison_start, comparison_end, entities
            )
            
            if baseline_data is None or comparison_data is None:
                logger.warning(f"Insufficient data for drift detection: {feature_id}")
                return None
            
            if len(baseline_data) < 10 or len(comparison_data) < 10:
                logger.warning(f"Too few samples for reliable drift detection: {feature_id}")
                return None
            
            # Perform drift detection based on method
            if method == DriftMethod.KOLMOGOROV_SMIRNOV:
                result = self._ks_test(baseline_data, comparison_data)
            elif method == DriftMethod.CHI_SQUARE:
                result = self._chi_square_test(baseline_data, comparison_data)
            elif method == DriftMethod.POPULATION_STABILITY_INDEX:
                result = self._psi_test(baseline_data, comparison_data)
            elif method == DriftMethod.JENSEN_SHANNON_DIVERGENCE:
                result = self._js_divergence_test(baseline_data, comparison_data)
            elif method == DriftMethod.WASSERSTEIN_DISTANCE:
                result = self._wasserstein_test(baseline_data, comparison_data)
            else:
                raise ValueError(f"Unsupported drift detection method: {method}")
            
            # Create drift result
            threshold = significance_level or self.significance_levels.get(method, 0.05)
            severity = self._calculate_severity(method, result['score'], result.get('p_value'))
            has_drift = self._has_drift(method, result['score'], result.get('p_value'), threshold)
            
            drift_result = DriftResult(
                feature_id=feature_id,
                method=method,
                drift_score=result['score'],
                p_value=result.get('p_value'),
                threshold=threshold,
                severity=severity,
                has_drift=has_drift,
                baseline_period=f"{baseline_start.isoformat()} to {baseline_end.isoformat()}",
                comparison_period=f"{comparison_start.isoformat()} to {comparison_end.isoformat()}",
                sample_sizes={
                    'baseline': len(baseline_data),
                    'comparison': len(comparison_data)
                },
                metadata=result.get('metadata', {}),
                timestamp=datetime.now(timezone.utc)
            )
            
            logger.info(
                f"Drift detection completed: {feature_id}, "
                f"method={method}, score={result['score']:.4f}, "
                f"severity={severity}, has_drift={has_drift}"
            )
            
            return drift_result
            
        except Exception as e:
            logger.error(f"Error detecting drift for feature {feature_id}: {e}")
            return None
    
    async def detect_drift_batch(
        self,
        feature_ids: List[str],
        method: DriftMethod = DriftMethod.KOLMOGOROV_SMIRNOV,
        **kwargs
    ) -> Dict[str, Optional[DriftResult]]:
        """
        Detect drift for multiple features in batch.
        
        Args:
            feature_ids: List of features to analyze
            method: Drift detection method
            **kwargs: Additional arguments passed to detect_drift
            
        Returns:
            Dictionary mapping feature_id to drift result
        """
        results = {}
        
        for feature_id in feature_ids:
            try:
                result = await self.detect_drift(feature_id, method, **kwargs)
                results[feature_id] = result
                
            except Exception as e:
                logger.error(f"Error detecting drift for {feature_id}: {e}")
                results[feature_id] = None
        
        return results
    
    async def get_drift_report(
        self,
        feature_ids: Optional[List[str]] = None,
        time_window_days: int = 7,
        methods: Optional[List[DriftMethod]] = None
    ) -> Dict[str, Any]:
        """
        Generate comprehensive drift report.
        
        Args:
            feature_ids: Features to include (all if None)
            time_window_days: Time window for analysis
            methods: Drift methods to use
            
        Returns:
            Comprehensive drift report
        """
        try:
            if not feature_ids:
                # Get all active features from registry
                all_features = await self.feature_store.registry.list_features()
                feature_ids = [f.feature_id for f in all_features if f.is_active]
            
            if not methods:
                methods = [DriftMethod.KOLMOGOROV_SMIRNOV, DriftMethod.POPULATION_STABILITY_INDEX]
            
            # Analyze drift for all features and methods
            drift_results = {}
            
            for method in methods:
                method_results = await self.detect_drift_batch(
                    feature_ids=feature_ids,
                    method=method
                )
                drift_results[method.value] = method_results
            
            # Generate summary statistics
            summary = self._generate_drift_summary(drift_results)
            
            report = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'analysis_period_days': time_window_days,
                'methods_used': [m.value for m in methods],
                'features_analyzed': len(feature_ids),
                'drift_results': drift_results,
                'summary': summary,
                'recommendations': self._generate_drift_recommendations(summary)
            }
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating drift report: {e}")
            return {'error': str(e)}
    
    async def _get_feature_data(
        self,
        feature_id: str,
        start_time: datetime,
        end_time: datetime,
        entities: Optional[List[str]]
    ) -> Optional[np.ndarray]:
        """Get feature data for specified time period."""
        try:
            # Get feature data from storage
            feature_df = await self.feature_store.storage.read_features(
                feature_id=feature_id,
                start_time=start_time,
                end_time=end_time,
                entities=entities
            )
            
            if feature_df is None or feature_df.empty:
                return None
            
            # Extract numeric feature values
            numeric_cols = feature_df.select_dtypes(include=[np.number]).columns
            feature_cols = [col for col in numeric_cols 
                          if not col.startswith('_') and col not in ['timestamp']]
            
            if not feature_cols:
                logger.warning(f"No numeric columns found for feature {feature_id}")
                return None
            
            # Use the first numeric column as the feature value
            feature_values = feature_df[feature_cols[0]].dropna().values
            
            return feature_values if len(feature_values) > 0 else None
            
        except Exception as e:
            logger.error(f"Error getting feature data for {feature_id}: {e}")
            return None
    
    def _ks_test(self, baseline: np.ndarray, comparison: np.ndarray) -> Dict[str, Any]:
        """Kolmogorov-Smirnov test for distribution change."""
        try:
            statistic, p_value = stats.ks_2samp(baseline, comparison)
            
            return {
                'score': statistic,
                'p_value': p_value,
                'metadata': {
                    'test_type': 'two_sample_ks',
                    'baseline_mean': float(np.mean(baseline)),
                    'comparison_mean': float(np.mean(comparison)),
                    'baseline_std': float(np.std(baseline)),
                    'comparison_std': float(np.std(comparison))
                }
            }
            
        except Exception as e:
            logger.error(f"Error in KS test: {e}")
            return {'score': 0.0, 'p_value': 1.0, 'metadata': {'error': str(e)}}
    
    def _chi_square_test(self, baseline: np.ndarray, comparison: np.ndarray) -> Dict[str, Any]:
        """Chi-square test for categorical data drift."""
        try:
            # Bin the data for categorical comparison
            combined = np.concatenate([baseline, comparison])
            bins = np.histogram_bin_edges(combined, bins='auto')
            
            baseline_hist, _ = np.histogram(baseline, bins=bins)
            comparison_hist, _ = np.histogram(comparison, bins=bins)
            
            # Avoid zero frequencies
            baseline_hist = baseline_hist + 1e-6
            comparison_hist = comparison_hist + 1e-6
            
            # Chi-square test
            statistic, p_value = stats.chisquare(comparison_hist, baseline_hist)
            
            return {
                'score': statistic,
                'p_value': p_value,
                'metadata': {
                    'bins_used': len(bins) - 1,
                    'baseline_hist': baseline_hist.tolist(),
                    'comparison_hist': comparison_hist.tolist()
                }
            }
            
        except Exception as e:
            logger.error(f"Error in chi-square test: {e}")
            return {'score': 0.0, 'p_value': 1.0, 'metadata': {'error': str(e)}}
    
    def _psi_test(self, baseline: np.ndarray, comparison: np.ndarray) -> Dict[str, Any]:
        """Population Stability Index calculation."""
        try:
            # Create bins based on baseline data
            n_bins = min(10, len(np.unique(baseline)))
            bins = np.histogram_bin_edges(baseline, bins=n_bins)
            
            # Calculate frequencies
            baseline_freq, _ = np.histogram(baseline, bins=bins)
            comparison_freq, _ = np.histogram(comparison, bins=bins)
            
            # Convert to proportions
            baseline_prop = baseline_freq / len(baseline)
            comparison_prop = comparison_freq / len(comparison)
            
            # Avoid zero proportions
            baseline_prop = np.where(baseline_prop == 0, 1e-6, baseline_prop)
            comparison_prop = np.where(comparison_prop == 0, 1e-6, comparison_prop)
            
            # Calculate PSI
            psi = np.sum((comparison_prop - baseline_prop) * np.log(comparison_prop / baseline_prop))
            
            return {
                'score': abs(psi),
                'metadata': {
                    'psi_value': float(psi),
                    'bins_used': n_bins,
                    'baseline_prop': baseline_prop.tolist(),
                    'comparison_prop': comparison_prop.tolist()
                }
            }
            
        except Exception as e:
            logger.error(f"Error in PSI calculation: {e}")
            return {'score': 0.0, 'metadata': {'error': str(e)}}
    
    def _js_divergence_test(self, baseline: np.ndarray, comparison: np.ndarray) -> Dict[str, Any]:
        """Jensen-Shannon divergence calculation."""
        try:
            # Create histograms
            combined = np.concatenate([baseline, comparison])
            bins = np.histogram_bin_edges(combined, bins='auto')
            
            baseline_hist, _ = np.histogram(baseline, bins=bins, density=True)
            comparison_hist, _ = np.histogram(comparison, bins=bins, density=True)
            
            # Normalize to probabilities
            baseline_prob = baseline_hist / np.sum(baseline_hist)
            comparison_prob = comparison_hist / np.sum(comparison_hist)
            
            # Avoid zero probabilities
            baseline_prob = np.where(baseline_prob == 0, 1e-10, baseline_prob)
            comparison_prob = np.where(comparison_prob == 0, 1e-10, comparison_prob)
            
            # Calculate JS divergence
            m = 0.5 * (baseline_prob + comparison_prob)
            js_div = 0.5 * stats.entropy(baseline_prob, m) + 0.5 * stats.entropy(comparison_prob, m)
            js_distance = np.sqrt(js_div)
            
            return {
                'score': float(js_distance),
                'metadata': {
                    'js_divergence': float(js_div),
                    'bins_used': len(bins) - 1
                }
            }
            
        except Exception as e:
            logger.error(f"Error in JS divergence calculation: {e}")
            return {'score': 0.0, 'metadata': {'error': str(e)}}
    
    def _wasserstein_test(self, baseline: np.ndarray, comparison: np.ndarray) -> Dict[str, Any]:
        """Wasserstein (Earth Mover's) distance calculation."""
        try:
            distance = stats.wasserstein_distance(baseline, comparison)
            
            # Normalize by the range of the data for interpretability
            data_range = max(np.max(baseline), np.max(comparison)) - min(np.min(baseline), np.min(comparison))
            normalized_distance = distance / data_range if data_range > 0 else distance
            
            return {
                'score': float(normalized_distance),
                'metadata': {
                    'raw_distance': float(distance),
                    'data_range': float(data_range),
                    'baseline_range': [float(np.min(baseline)), float(np.max(baseline))],
                    'comparison_range': [float(np.min(comparison)), float(np.max(comparison))]
                }
            }
            
        except Exception as e:
            logger.error(f"Error in Wasserstein distance calculation: {e}")
            return {'score': 0.0, 'metadata': {'error': str(e)}}
    
    def _calculate_severity(
        self, 
        method: DriftMethod, 
        score: float, 
        p_value: Optional[float]
    ) -> DriftSeverity:
        """Calculate drift severity based on score and method."""
        try:
            if method not in self.severity_thresholds:
                return DriftSeverity.NO_DRIFT
            
            thresholds = self.severity_thresholds[method]
            
            if method == DriftMethod.CHI_SQUARE:
                # For p-value based tests, lower p-value means more severe drift
                if p_value is None:
                    return DriftSeverity.NO_DRIFT
                
                if p_value <= thresholds[DriftSeverity.SEVERE]:
                    return DriftSeverity.SEVERE
                elif p_value <= thresholds[DriftSeverity.HIGH]:
                    return DriftSeverity.HIGH
                elif p_value <= thresholds[DriftSeverity.MODERATE]:
                    return DriftSeverity.MODERATE
                elif p_value <= thresholds[DriftSeverity.LOW]:
                    return DriftSeverity.LOW
                else:
                    return DriftSeverity.NO_DRIFT
            
            else:
                # For score-based tests, higher score means more severe drift
                if score >= thresholds[DriftSeverity.SEVERE]:
                    return DriftSeverity.SEVERE
                elif score >= thresholds[DriftSeverity.HIGH]:
                    return DriftSeverity.HIGH
                elif score >= thresholds[DriftSeverity.MODERATE]:
                    return DriftSeverity.MODERATE
                elif score >= thresholds[DriftSeverity.LOW]:
                    return DriftSeverity.LOW
                else:
                    return DriftSeverity.NO_DRIFT
                    
        except Exception as e:
            logger.error(f"Error calculating severity: {e}")
            return DriftSeverity.NO_DRIFT
    
    def _has_drift(
        self, 
        method: DriftMethod, 
        score: float, 
        p_value: Optional[float], 
        threshold: float
    ) -> bool:
        """Determine if drift is present based on method and threshold."""
        try:
            if method in [DriftMethod.CHI_SQUARE, DriftMethod.KOLMOGOROV_SMIRNOV]:
                return p_value is not None and p_value < threshold
            else:
                return score > threshold
                
        except Exception as e:
            logger.error(f"Error determining drift presence: {e}")
            return False
    
    def _generate_drift_summary(self, drift_results: Dict[str, Dict[str, Optional[DriftResult]]]) -> Dict[str, Any]:
        """Generate summary statistics from drift results."""
        summary = {
            'total_features': 0,
            'features_with_drift': 0,
            'drift_by_severity': {severity.value: 0 for severity in DriftSeverity},
            'drift_by_method': {},
            'high_drift_features': []
        }
        
        all_feature_ids = set()
        
        for method, method_results in drift_results.items():
            method_stats = {'total': 0, 'drift': 0, 'no_drift': 0}
            
            for feature_id, result in method_results.items():
                if result is not None:
                    all_feature_ids.add(feature_id)
                    method_stats['total'] += 1
                    
                    if result.has_drift:
                        method_stats['drift'] += 1
                        summary['drift_by_severity'][result.severity.value] += 1
                        
                        if result.severity in [DriftSeverity.HIGH, DriftSeverity.SEVERE]:
                            if feature_id not in summary['high_drift_features']:
                                summary['high_drift_features'].append(feature_id)
                    else:
                        method_stats['no_drift'] += 1
            
            summary['drift_by_method'][method] = method_stats
        
        summary['total_features'] = len(all_feature_ids)
        summary['features_with_drift'] = len([
            fid for fid in all_feature_ids 
            if any(
                results.get(fid) and results[fid].has_drift 
                for results in drift_results.values()
            )
        ])
        
        return summary
    
    def _generate_drift_recommendations(self, summary: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on drift analysis."""
        recommendations = []
        
        if summary['features_with_drift'] > summary['total_features'] * 0.5:
            recommendations.append(
                "High number of features showing drift. Consider investigating data pipeline changes."
            )
        
        if len(summary['high_drift_features']) > 0:
            recommendations.append(
                f"Features with severe drift detected: {', '.join(summary['high_drift_features'][:5])}. "
                "Immediate attention required."
            )
        
        severe_count = summary['drift_by_severity'].get('severe', 0)
        if severe_count > 0:
            recommendations.append(
                f"{severe_count} features showing severe drift. Consider retraining models."
            )
        
        moderate_count = summary['drift_by_severity'].get('moderate', 0)
        if moderate_count > 5:
            recommendations.append(
                "Multiple features showing moderate drift. Monitor closely and prepare for model updates."
            )
        
        if not recommendations:
            recommendations.append("No significant drift detected. Continue regular monitoring.")
        
        return recommendations