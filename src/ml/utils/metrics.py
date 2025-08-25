"""
Custom Metrics for QuantStream Analytics Platform

This module provides custom metrics and evaluation functions for
anomaly detection models in financial time series data.
"""

import logging
from typing import Dict, List, Optional, Tuple, Union, Any
import numpy as np
import pandas as pd
from sklearn.metrics import (
    precision_score, recall_score, f1_score, roc_auc_score,
    average_precision_score, precision_recall_curve, roc_curve
)

logger = logging.getLogger(__name__)


class AnomalyDetectionMetrics:
    """Custom metrics for anomaly detection evaluation."""
    
    @staticmethod
    def detection_delay(
        y_true: np.ndarray,
        y_pred: np.ndarray,
        timestamps: Optional[np.ndarray] = None
    ) -> Dict[str, float]:
        """
        Calculate the average detection delay for anomalies.
        
        Args:
            y_true: True anomaly labels
            y_pred: Predicted anomaly labels
            timestamps: Optional timestamps for time-based delay calculation
            
        Returns:
            Dictionary with detection delay statistics
        """
        # Find true anomaly segments
        true_anomaly_starts = []
        in_anomaly = False
        
        for i, label in enumerate(y_true):
            if label == 1 and not in_anomaly:
                true_anomaly_starts.append(i)
                in_anomaly = True
            elif label == 0 and in_anomaly:
                in_anomaly = False
        
        if not true_anomaly_starts:
            return {'mean_delay': 0.0, 'median_delay': 0.0, 'max_delay': 0.0, 'detection_rate': 0.0}
        
        delays = []
        detected_anomalies = 0
        
        for start_idx in true_anomaly_starts:
            # Look for first detection after anomaly start
            detection_idx = None
            for i in range(start_idx, len(y_pred)):
                if y_pred[i] == 1:
                    detection_idx = i
                    break
                # Stop looking if we're past the anomaly end
                if i > start_idx and y_true[i] == 0:
                    break
            
            if detection_idx is not None:
                delay = detection_idx - start_idx
                delays.append(delay)
                detected_anomalies += 1
        
        if not delays:
            return {'mean_delay': float('inf'), 'median_delay': float('inf'), 
                   'max_delay': float('inf'), 'detection_rate': 0.0}
        
        return {
            'mean_delay': float(np.mean(delays)),
            'median_delay': float(np.median(delays)),
            'max_delay': float(np.max(delays)),
            'detection_rate': detected_anomalies / len(true_anomaly_starts)
        }
    
    @staticmethod
    def false_positive_rate(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """Calculate false positive rate."""
        tn = np.sum((y_true == 0) & (y_pred == 0))
        fp = np.sum((y_true == 0) & (y_pred == 1))
        return fp / (fp + tn) if (fp + tn) > 0 else 0.0
    
    @staticmethod
    def true_positive_rate(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """Calculate true positive rate (recall)."""
        return recall_score(y_true, y_pred, zero_division=0.0)
    
    @staticmethod
    def precision_at_k(
        y_true: np.ndarray,
        y_scores: np.ndarray,
        k: int
    ) -> float:
        """
        Calculate precision at k (top k predictions).
        
        Args:
            y_true: True labels
            y_scores: Prediction scores
            k: Number of top predictions to consider
            
        Returns:
            Precision at k
        """
        if k <= 0 or k > len(y_scores):
            k = len(y_scores)
        
        # Get indices of top k scores
        top_k_indices = np.argsort(y_scores)[-k:]
        
        # Calculate precision at k
        precision_at_k = np.mean(y_true[top_k_indices])
        return float(precision_at_k)
    
    @staticmethod
    def anomaly_coverage(
        y_true: np.ndarray,
        y_pred: np.ndarray,
        window_size: int = 1
    ) -> float:
        """
        Calculate anomaly coverage - percentage of true anomaly periods
        that have at least one detection within the window.
        
        Args:
            y_true: True anomaly labels
            y_pred: Predicted anomaly labels
            window_size: Size of detection window around true anomalies
            
        Returns:
            Coverage rate
        """
        # Find true anomaly indices
        true_anomaly_indices = np.where(y_true == 1)[0]
        
        if len(true_anomaly_indices) == 0:
            return 1.0  # Perfect coverage if no anomalies to detect
        
        covered_anomalies = 0
        
        for anomaly_idx in true_anomaly_indices:
            # Check window around the anomaly
            start_idx = max(0, anomaly_idx - window_size)
            end_idx = min(len(y_pred), anomaly_idx + window_size + 1)
            
            # Check if any prediction in the window is positive
            if np.any(y_pred[start_idx:end_idx] == 1):
                covered_anomalies += 1
        
        return covered_anomalies / len(true_anomaly_indices)


class FinancialMetrics:
    """Financial-specific metrics for anomaly detection."""
    
    @staticmethod
    def sharpe_ratio_improvement(
        returns: np.ndarray,
        anomaly_mask: np.ndarray,
        risk_free_rate: float = 0.02
    ) -> float:
        """
        Calculate improvement in Sharpe ratio from anomaly detection.
        
        Args:
            returns: Asset returns
            anomaly_mask: Boolean mask indicating detected anomalies
            risk_free_rate: Risk-free rate (annualized)
            
        Returns:
            Sharpe ratio improvement
        """
        if len(returns) != len(anomaly_mask):
            raise ValueError("Returns and anomaly mask must have same length")
        
        # Calculate original Sharpe ratio
        original_sharpe = (np.mean(returns) * 252 - risk_free_rate) / (np.std(returns) * np.sqrt(252))
        
        # Calculate Sharpe ratio excluding anomaly periods
        normal_returns = returns[~anomaly_mask]
        if len(normal_returns) == 0:
            return 0.0
        
        filtered_sharpe = (np.mean(normal_returns) * 252 - risk_free_rate) / (np.std(normal_returns) * np.sqrt(252))
        
        return filtered_sharpe - original_sharpe
    
    @staticmethod
    def maximum_drawdown_reduction(
        prices: np.ndarray,
        anomaly_mask: np.ndarray
    ) -> float:
        """
        Calculate reduction in maximum drawdown from anomaly detection.
        
        Args:
            prices: Asset prices
            anomaly_mask: Boolean mask indicating detected anomalies
            
        Returns:
            Maximum drawdown reduction (percentage points)
        """
        def calculate_max_drawdown(price_series):
            """Calculate maximum drawdown for a price series."""
            peak = np.maximum.accumulate(price_series)
            drawdown = (price_series - peak) / peak
            return np.min(drawdown)
        
        # Original maximum drawdown
        original_mdd = calculate_max_drawdown(prices)
        
        # Maximum drawdown excluding anomaly periods
        normal_prices = prices[~anomaly_mask]
        if len(normal_prices) == 0:
            return 0.0
        
        filtered_mdd = calculate_max_drawdown(normal_prices)
        
        return original_mdd - filtered_mdd  # Reduction (should be positive if beneficial)
    
    @staticmethod
    def volatility_reduction(
        returns: np.ndarray,
        anomaly_mask: np.ndarray
    ) -> float:
        """
        Calculate reduction in volatility from anomaly detection.
        
        Args:
            returns: Asset returns
            anomaly_mask: Boolean mask indicating detected anomalies
            
        Returns:
            Volatility reduction (percentage points)
        """
        original_vol = np.std(returns) * np.sqrt(252)  # Annualized volatility
        
        normal_returns = returns[~anomaly_mask]
        if len(normal_returns) == 0:
            return 0.0
        
        filtered_vol = np.std(normal_returns) * np.sqrt(252)
        
        return original_vol - filtered_vol


class ModelComparison:
    """Utilities for comparing multiple anomaly detection models."""
    
    @staticmethod
    def compare_models(
        models_predictions: Dict[str, Dict[str, np.ndarray]],
        y_true: np.ndarray,
        metrics: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Compare multiple models on various metrics.
        
        Args:
            models_predictions: Dict of {model_name: {'predictions': y_pred, 'scores': y_scores}}
            y_true: True labels
            metrics: List of metrics to compute
            
        Returns:
            Comparison dataframe
        """
        if metrics is None:
            metrics = ['precision', 'recall', 'f1_score', 'roc_auc', 'false_positive_rate']
        
        comparison_data = []
        
        for model_name, predictions in models_predictions.items():
            y_pred = predictions['predictions']
            y_scores = predictions.get('scores', y_pred)
            
            model_metrics = {'model': model_name}
            
            if 'precision' in metrics:
                model_metrics['precision'] = precision_score(y_true, y_pred, zero_division=0)
            
            if 'recall' in metrics:
                model_metrics['recall'] = recall_score(y_true, y_pred, zero_division=0)
            
            if 'f1_score' in metrics:
                model_metrics['f1_score'] = f1_score(y_true, y_pred, zero_division=0)
            
            if 'roc_auc' in metrics:
                try:
                    model_metrics['roc_auc'] = roc_auc_score(y_true, y_scores)
                except ValueError:
                    model_metrics['roc_auc'] = np.nan
            
            if 'false_positive_rate' in metrics:
                model_metrics['false_positive_rate'] = AnomalyDetectionMetrics.false_positive_rate(y_true, y_pred)
            
            if 'detection_delay' in metrics:
                delay_metrics = AnomalyDetectionMetrics.detection_delay(y_true, y_pred)
                model_metrics['mean_detection_delay'] = delay_metrics['mean_delay']
            
            comparison_data.append(model_metrics)
        
        return pd.DataFrame(comparison_data)
    
    @staticmethod
    def statistical_significance_test(
        model1_scores: np.ndarray,
        model2_scores: np.ndarray,
        test_type: str = 'wilcoxon'
    ) -> Dict[str, float]:
        """
        Test statistical significance between two model performances.
        
        Args:
            model1_scores: Performance scores for model 1
            model2_scores: Performance scores for model 2
            test_type: Type of statistical test
            
        Returns:
            Test results
        """
        from scipy import stats
        
        if test_type == 'wilcoxon':
            statistic, p_value = stats.wilcoxon(model1_scores, model2_scores)
        elif test_type == 'ttest':
            statistic, p_value = stats.ttest_rel(model1_scores, model2_scores)
        elif test_type == 'mannwhitney':
            statistic, p_value = stats.mannwhitneyu(model1_scores, model2_scores)
        else:
            raise ValueError(f"Unknown test type: {test_type}")
        
        return {
            'statistic': float(statistic),
            'p_value': float(p_value),
            'significant': p_value < 0.05
        }


class ThresholdOptimizer:
    """Utility for optimizing detection thresholds."""
    
    @staticmethod
    def optimize_threshold_f1(
        y_true: np.ndarray,
        y_scores: np.ndarray
    ) -> Dict[str, float]:
        """
        Find optimal threshold that maximizes F1 score.
        
        Args:
            y_true: True labels
            y_scores: Prediction scores
            
        Returns:
            Optimization results
        """
        precisions, recalls, thresholds = precision_recall_curve(y_true, y_scores)
        f1_scores = 2 * (precisions * recalls) / (precisions + recalls)
        f1_scores = np.nan_to_num(f1_scores)  # Handle division by zero
        
        best_idx = np.argmax(f1_scores)
        
        return {
            'optimal_threshold': float(thresholds[best_idx]),
            'best_f1_score': float(f1_scores[best_idx]),
            'precision_at_optimal': float(precisions[best_idx]),
            'recall_at_optimal': float(recalls[best_idx])
        }
    
    @staticmethod
    def optimize_threshold_custom(
        y_true: np.ndarray,
        y_scores: np.ndarray,
        metric_func: callable,
        maximize: bool = True
    ) -> Dict[str, float]:
        """
        Find optimal threshold for a custom metric.
        
        Args:
            y_true: True labels
            y_scores: Prediction scores
            metric_func: Custom metric function that takes (y_true, y_pred)
            maximize: Whether to maximize or minimize the metric
            
        Returns:
            Optimization results
        """
        # Create threshold candidates
        thresholds = np.percentile(y_scores, np.linspace(1, 99, 99))
        
        best_threshold = thresholds[0]
        best_score = float('-inf') if maximize else float('inf')
        
        for threshold in thresholds:
            y_pred = (y_scores >= threshold).astype(int)
            
            try:
                score = metric_func(y_true, y_pred)
                if maximize and score > best_score:
                    best_score = score
                    best_threshold = threshold
                elif not maximize and score < best_score:
                    best_score = score
                    best_threshold = threshold
            except:
                continue  # Skip invalid thresholds
        
        return {
            'optimal_threshold': float(best_threshold),
            'best_score': float(best_score)
        }
    
    @staticmethod
    def precision_recall_trade_off(
        y_true: np.ndarray,
        y_scores: np.ndarray,
        target_precision: Optional[float] = None,
        target_recall: Optional[float] = None
    ) -> Dict[str, float]:
        """
        Find threshold that achieves target precision or recall.
        
        Args:
            y_true: True labels
            y_scores: Prediction scores
            target_precision: Target precision (0-1)
            target_recall: Target recall (0-1)
            
        Returns:
            Threshold and achieved metrics
        """
        precisions, recalls, thresholds = precision_recall_curve(y_true, y_scores)
        
        if target_precision is not None:
            # Find threshold that achieves target precision
            valid_indices = precisions >= target_precision
            if not np.any(valid_indices):
                return {'error': 'Target precision not achievable'}
            
            best_idx = np.where(valid_indices)[0][-1]  # Highest recall among valid precisions
            
            return {
                'threshold': float(thresholds[best_idx]),
                'achieved_precision': float(precisions[best_idx]),
                'achieved_recall': float(recalls[best_idx])
            }
        
        elif target_recall is not None:
            # Find threshold that achieves target recall
            valid_indices = recalls >= target_recall
            if not np.any(valid_indices):
                return {'error': 'Target recall not achievable'}
            
            best_idx = np.where(valid_indices)[0][0]  # Highest precision among valid recalls
            
            return {
                'threshold': float(thresholds[best_idx]),
                'achieved_precision': float(precisions[best_idx]),
                'achieved_recall': float(recalls[best_idx])
            }
        
        else:
            raise ValueError("Must specify either target_precision or target_recall")


def comprehensive_evaluation(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    y_scores: np.ndarray,
    returns: Optional[np.ndarray] = None,
    prices: Optional[np.ndarray] = None
) -> Dict[str, Any]:
    """
    Perform comprehensive evaluation of anomaly detection model.
    
    Args:
        y_true: True anomaly labels
        y_pred: Predicted anomaly labels
        y_scores: Prediction scores
        returns: Optional asset returns for financial metrics
        prices: Optional asset prices for financial metrics
        
    Returns:
        Comprehensive evaluation results
    """
    results = {}
    
    # Basic classification metrics
    results['classification_metrics'] = {
        'precision': float(precision_score(y_true, y_pred, zero_division=0)),
        'recall': float(recall_score(y_true, y_pred, zero_division=0)),
        'f1_score': float(f1_score(y_true, y_pred, zero_division=0)),
    }
    
    # ROC-AUC if possible
    try:
        results['classification_metrics']['roc_auc'] = float(roc_auc_score(y_true, y_scores))
        results['classification_metrics']['average_precision'] = float(average_precision_score(y_true, y_scores))
    except ValueError:
        logger.warning("Could not calculate ROC-AUC (may be due to only one class in y_true)")
    
    # Anomaly detection specific metrics
    ad_metrics = AnomalyDetectionMetrics()
    results['anomaly_metrics'] = {
        'false_positive_rate': ad_metrics.false_positive_rate(y_true, y_pred),
        'detection_delay': ad_metrics.detection_delay(y_true, y_pred),
        'precision_at_10': ad_metrics.precision_at_k(y_true, y_scores, k=10),
        'anomaly_coverage': ad_metrics.anomaly_coverage(y_true, y_pred)
    }
    
    # Financial metrics if data is available
    if returns is not None:
        fin_metrics = FinancialMetrics()
        anomaly_mask = y_pred.astype(bool)
        
        results['financial_metrics'] = {
            'sharpe_ratio_improvement': fin_metrics.sharpe_ratio_improvement(returns, anomaly_mask),
            'volatility_reduction': fin_metrics.volatility_reduction(returns, anomaly_mask)
        }
        
        if prices is not None:
            results['financial_metrics']['max_drawdown_reduction'] = fin_metrics.maximum_drawdown_reduction(
                prices, anomaly_mask
            )
    
    # Threshold optimization
    threshold_opt = ThresholdOptimizer()
    results['threshold_optimization'] = threshold_opt.optimize_threshold_f1(y_true, y_scores)
    
    return results