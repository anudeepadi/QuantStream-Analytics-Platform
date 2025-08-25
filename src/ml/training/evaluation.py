"""
Model Evaluation Framework for Anomaly Detection

This module provides comprehensive evaluation capabilities including time-series
aware validation, backtesting, A/B testing, and statistical significance testing.
"""

import logging
import warnings
from typing import Any, Dict, List, Optional, Union, Tuple, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, average_precision_score, confusion_matrix,
    classification_report, precision_recall_curve, roc_curve
)
from sklearn.model_selection import TimeSeriesSplit
from scipy import stats
from scipy.stats import mannwhitneyu, wilcoxon, chi2_contingency

# Plotting imports
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    HAS_PLOTTING = True
except ImportError:
    HAS_PLOTTING = False

# Statistical tests
try:
    from statsmodels.stats.contingency_tables import mcnemar
    from statsmodels.stats.proportion import proportions_ztest
    HAS_STATSMODELS = True
except ImportError:
    HAS_STATSMODELS = False

from ..models.base_model import BaseAnomalyDetector

logger = logging.getLogger(__name__)


@dataclass
class EvaluationConfig:
    """Configuration for model evaluation."""
    metrics: List[str] = None
    confidence_level: float = 0.95
    bootstrap_samples: int = 1000
    time_series_aware: bool = True
    window_size: int = 30
    stride: int = 1
    min_anomaly_rate: float = 0.01
    max_anomaly_rate: float = 0.5
    
    def __post_init__(self):
        if self.metrics is None:
            self.metrics = [
                'accuracy', 'precision', 'recall', 'f1_score', 
                'roc_auc', 'average_precision', 'specificity', 'npv'
            ]


@dataclass
class BacktestConfig:
    """Configuration for backtesting."""
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    window_size: int = 30
    step_size: int = 1
    min_train_size: int = 100
    retrain_frequency: int = 10
    performance_threshold: float = 0.7
    drift_threshold: float = 0.1


class ModelEvaluator:
    """
    Comprehensive model evaluation framework for anomaly detection.
    """
    
    def __init__(self, config: Optional[EvaluationConfig] = None):
        """
        Initialize the evaluator.
        
        Args:
            config: Evaluation configuration
        """
        self.config = config or EvaluationConfig()
        self.evaluation_history = []
        self.comparison_results = {}
        
    def evaluate_single_model(
        self,
        model: BaseAnomalyDetector,
        X_test: np.ndarray,
        y_test: np.ndarray,
        sample_weights: Optional[np.ndarray] = None,
        return_predictions: bool = False
    ) -> Dict[str, Any]:
        """
        Comprehensive evaluation of a single model.
        
        Args:
            model: Trained anomaly detection model
            X_test: Test features
            y_test: Test labels
            sample_weights: Optional sample weights
            return_predictions: Whether to return predictions
            
        Returns:
            Evaluation results dictionary
        """
        logger.info(f"Evaluating model: {model.name}")
        
        # Get predictions
        y_pred = model.predict(X_test)
        y_scores = model.predict_proba(X_test)
        
        # Basic metrics
        metrics = self._calculate_metrics(y_test, y_pred, y_scores, sample_weights)
        
        # Confusion matrix analysis
        cm = confusion_matrix(y_test, y_pred)
        cm_analysis = self._analyze_confusion_matrix(cm)
        
        # Bootstrap confidence intervals
        ci_metrics = self._bootstrap_confidence_intervals(
            y_test, y_pred, y_scores, sample_weights
        )
        
        # Time-series specific metrics if applicable
        ts_metrics = {}
        if self.config.time_series_aware and len(X_test) > self.config.window_size:
            ts_metrics = self._time_series_metrics(y_test, y_pred, y_scores)
        
        # Detection delay analysis
        delay_metrics = self._detection_delay_analysis(y_test, y_pred)
        
        # Prepare results
        results = {
            'model_name': model.name,
            'model_type': model.model_type,
            'evaluation_timestamp': datetime.now().isoformat(),
            'test_samples': len(X_test),
            'anomaly_rate': np.mean(y_test),
            'predicted_anomaly_rate': np.mean(y_pred),
            'metrics': metrics,
            'confidence_intervals': ci_metrics,
            'confusion_matrix': cm.tolist(),
            'confusion_matrix_analysis': cm_analysis,
            'time_series_metrics': ts_metrics,
            'detection_delay_metrics': delay_metrics
        }
        
        if return_predictions:
            results['predictions'] = {
                'y_pred': y_pred,
                'y_scores': y_scores
            }
        
        # Store in history
        self.evaluation_history.append(results)
        
        logger.info(f"Evaluation completed. F1 Score: {metrics.get('f1_score', 'N/A'):.4f}")
        return results
    
    def _calculate_metrics(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        y_scores: np.ndarray,
        sample_weights: Optional[np.ndarray] = None
    ) -> Dict[str, float]:
        """Calculate comprehensive evaluation metrics."""
        metrics = {}
        
        # Basic classification metrics
        if 'accuracy' in self.config.metrics:
            metrics['accuracy'] = accuracy_score(y_true, y_pred, sample_weight=sample_weights)
        
        if 'precision' in self.config.metrics:
            metrics['precision'] = precision_score(y_true, y_pred, sample_weight=sample_weights, zero_division=0)
        
        if 'recall' in self.config.metrics:
            metrics['recall'] = recall_score(y_true, y_pred, sample_weight=sample_weights, zero_division=0)
        
        if 'f1_score' in self.config.metrics:
            metrics['f1_score'] = f1_score(y_true, y_pred, sample_weight=sample_weights, zero_division=0)
        
        # Area under curves
        try:
            if 'roc_auc' in self.config.metrics:
                metrics['roc_auc'] = roc_auc_score(y_true, y_scores, sample_weight=sample_weights)
        except ValueError:
            metrics['roc_auc'] = np.nan
        
        try:
            if 'average_precision' in self.config.metrics:
                metrics['average_precision'] = average_precision_score(y_true, y_scores, sample_weight=sample_weights)
        except ValueError:
            metrics['average_precision'] = np.nan
        
        # Additional metrics from confusion matrix
        cm = confusion_matrix(y_true, y_pred)
        if cm.shape == (2, 2):
            tn, fp, fn, tp = cm.ravel()
            
            if 'specificity' in self.config.metrics:
                metrics['specificity'] = tn / (tn + fp) if (tn + fp) > 0 else 0
            
            if 'npv' in self.config.metrics:  # Negative Predictive Value
                metrics['npv'] = tn / (tn + fn) if (tn + fn) > 0 else 0
            
            if 'false_positive_rate' in self.config.metrics:
                metrics['false_positive_rate'] = fp / (fp + tn) if (fp + tn) > 0 else 0
            
            if 'false_negative_rate' in self.config.metrics:
                metrics['false_negative_rate'] = fn / (fn + tp) if (fn + tp) > 0 else 0
        
        return metrics
    
    def _analyze_confusion_matrix(self, cm: np.ndarray) -> Dict[str, Any]:
        """Analyze confusion matrix for insights."""
        if cm.shape != (2, 2):
            return {'error': 'Only binary classification supported'}
        
        tn, fp, fn, tp = cm.ravel()
        total = tn + fp + fn + tp
        
        analysis = {
            'true_negatives': int(tn),
            'false_positives': int(fp),
            'false_negatives': int(fn),
            'true_positives': int(tp),
            'total_samples': int(total),
            'correct_predictions': int(tn + tp),
            'incorrect_predictions': int(fp + fn),
            'positive_samples': int(tp + fn),
            'negative_samples': int(tn + fp),
            'predicted_positives': int(tp + fp),
            'predicted_negatives': int(tn + fn)
        }
        
        return analysis
    
    def _bootstrap_confidence_intervals(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        y_scores: np.ndarray,
        sample_weights: Optional[np.ndarray] = None
    ) -> Dict[str, Tuple[float, float]]:
        """Calculate bootstrap confidence intervals for metrics."""
        n_samples = len(y_true)
        bootstrap_metrics = {metric: [] for metric in self.config.metrics}
        
        for _ in range(self.config.bootstrap_samples):
            # Bootstrap sample
            indices = np.random.choice(n_samples, n_samples, replace=True)
            y_true_boot = y_true[indices]
            y_pred_boot = y_pred[indices]
            y_scores_boot = y_scores[indices]
            weights_boot = sample_weights[indices] if sample_weights is not None else None
            
            # Calculate metrics for bootstrap sample
            try:
                boot_metrics = self._calculate_metrics(y_true_boot, y_pred_boot, y_scores_boot, weights_boot)
                for metric, value in boot_metrics.items():
                    if metric in bootstrap_metrics and not np.isnan(value):
                        bootstrap_metrics[metric].append(value)
            except:
                continue
        
        # Calculate confidence intervals
        alpha = 1 - self.config.confidence_level
        ci_results = {}
        
        for metric, values in bootstrap_metrics.items():
            if values:
                values = np.array(values)
                lower = np.percentile(values, 100 * alpha / 2)
                upper = np.percentile(values, 100 * (1 - alpha / 2))
                ci_results[metric] = (lower, upper)
        
        return ci_results
    
    def _time_series_metrics(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        y_scores: np.ndarray
    ) -> Dict[str, Any]:
        """Calculate time-series specific evaluation metrics."""
        metrics = {}
        
        # Rolling window evaluation
        window_metrics = []
        for i in range(len(y_true) - self.config.window_size + 1):
            if i % self.config.stride == 0:
                start_idx = i
                end_idx = i + self.config.window_size
                
                y_true_window = y_true[start_idx:end_idx]
                y_pred_window = y_pred[start_idx:end_idx]
                y_scores_window = y_scores[start_idx:end_idx]
                
                # Skip windows with no anomalies
                if np.sum(y_true_window) == 0:
                    continue
                
                window_f1 = f1_score(y_true_window, y_pred_window, zero_division=0)
                window_precision = precision_score(y_true_window, y_pred_window, zero_division=0)
                window_recall = recall_score(y_true_window, y_pred_window, zero_division=0)
                
                window_metrics.append({
                    'window_start': start_idx,
                    'f1_score': window_f1,
                    'precision': window_precision,
                    'recall': window_recall
                })
        
        if window_metrics:
            metrics['rolling_window_metrics'] = {
                'mean_f1': np.mean([w['f1_score'] for w in window_metrics]),
                'std_f1': np.std([w['f1_score'] for w in window_metrics]),
                'mean_precision': np.mean([w['precision'] for w in window_metrics]),
                'mean_recall': np.mean([w['recall'] for w in window_metrics]),
                'windows_evaluated': len(window_metrics)
            }
        
        # Temporal consistency
        metrics['temporal_consistency'] = self._calculate_temporal_consistency(y_pred)
        
        return metrics
    
    def _calculate_temporal_consistency(self, y_pred: np.ndarray) -> float:
        """Calculate temporal consistency of predictions."""
        if len(y_pred) < 2:
            return 1.0
        
        # Calculate the ratio of consecutive predictions that are the same
        same_consecutive = np.sum(y_pred[:-1] == y_pred[1:])
        total_transitions = len(y_pred) - 1
        
        return same_consecutive / total_transitions if total_transitions > 0 else 1.0
    
    def _detection_delay_analysis(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray
    ) -> Dict[str, Any]:
        """Analyze detection delay for anomalies."""
        delays = []
        
        # Find anomaly periods in ground truth
        anomaly_starts = []
        in_anomaly = False
        
        for i, label in enumerate(y_true):
            if label == 1 and not in_anomaly:
                anomaly_starts.append(i)
                in_anomaly = True
            elif label == 0:
                in_anomaly = False
        
        # For each anomaly start, find when it was first detected
        for start_idx in anomaly_starts:
            # Look for detection within reasonable window
            detection_window = min(start_idx + 50, len(y_pred))
            
            for detect_idx in range(start_idx, detection_window):
                if y_pred[detect_idx] == 1:
                    delay = detect_idx - start_idx
                    delays.append(delay)
                    break
        
        if delays:
            return {
                'mean_detection_delay': np.mean(delays),
                'median_detection_delay': np.median(delays),
                'max_detection_delay': np.max(delays),
                'detection_rate': len(delays) / len(anomaly_starts) if anomaly_starts else 0,
                'total_anomaly_periods': len(anomaly_starts)
            }
        else:
            return {
                'mean_detection_delay': np.inf,
                'median_detection_delay': np.inf,
                'max_detection_delay': np.inf,
                'detection_rate': 0.0,
                'total_anomaly_periods': len(anomaly_starts)
            }
    
    def compare_models(
        self,
        models: List[BaseAnomalyDetector],
        X_test: np.ndarray,
        y_test: np.ndarray,
        comparison_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Compare multiple models on the same test set.
        
        Args:
            models: List of trained models
            X_test: Test features
            y_test: Test labels
            comparison_name: Name for this comparison
            
        Returns:
            Comparison results
        """
        logger.info(f"Comparing {len(models)} models")
        
        comparison_name = comparison_name or f"comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Evaluate each model
        model_results = {}
        all_predictions = {}
        
        for model in models:
            try:
                result = self.evaluate_single_model(model, X_test, y_test, return_predictions=True)
                model_results[model.name] = result
                all_predictions[model.name] = result['predictions']
                logger.info(f"Evaluated {model.name}")
            except Exception as e:
                logger.error(f"Failed to evaluate {model.name}: {e}")
                continue
        
        if not model_results:
            logger.error("No models successfully evaluated")
            return {}
        
        # Statistical comparison
        statistical_tests = self._statistical_model_comparison(all_predictions, y_test)
        
        # Create comparison summary
        comparison_summary = self._create_comparison_summary(model_results)
        
        # Rank models
        model_rankings = self._rank_models(model_results)
        
        comparison_result = {
            'comparison_name': comparison_name,
            'timestamp': datetime.now().isoformat(),
            'models_compared': list(model_results.keys()),
            'test_samples': len(X_test),
            'anomaly_rate': np.mean(y_test),
            'model_results': {k: {key: val for key, val in v.items() if key != 'predictions'} 
                            for k, v in model_results.items()},
            'comparison_summary': comparison_summary,
            'model_rankings': model_rankings,
            'statistical_tests': statistical_tests
        }
        
        # Store comparison results
        self.comparison_results[comparison_name] = comparison_result
        
        logger.info(f"Model comparison completed. Best model: {model_rankings[0]['model_name']}")
        return comparison_result
    
    def _statistical_model_comparison(
        self,
        all_predictions: Dict[str, Dict[str, np.ndarray]],
        y_test: np.ndarray
    ) -> Dict[str, Any]:
        """Perform statistical tests to compare model performance."""
        tests = {}
        model_names = list(all_predictions.keys())
        
        if len(model_names) < 2:
            return tests
        
        # Pairwise McNemar tests for classification accuracy
        if HAS_STATSMODELS:
            mcnemar_results = {}
            for i in range(len(model_names)):
                for j in range(i + 1, len(model_names)):
                    model1, model2 = model_names[i], model_names[j]
                    
                    pred1 = all_predictions[model1]['y_pred']
                    pred2 = all_predictions[model2]['y_pred']
                    
                    # Create contingency table
                    correct1 = (pred1 == y_test)
                    correct2 = (pred2 == y_test)
                    
                    # McNemar's test contingency table
                    both_correct = np.sum(correct1 & correct2)
                    only1_correct = np.sum(correct1 & ~correct2)
                    only2_correct = np.sum(~correct1 & correct2)
                    both_wrong = np.sum(~correct1 & ~correct2)
                    
                    contingency_table = np.array([[both_correct, only2_correct],
                                                [only1_correct, both_wrong]])
                    
                    try:
                        result = mcnemar(contingency_table)
                        mcnemar_results[f"{model1}_vs_{model2}"] = {
                            'statistic': float(result.statistic),
                            'p_value': float(result.pvalue),
                            'significant': result.pvalue < 0.05
                        }
                    except Exception as e:
                        logger.warning(f"McNemar test failed for {model1} vs {model2}: {e}")
            
            tests['mcnemar_tests'] = mcnemar_results
        
        # Mann-Whitney U tests for score distributions
        mann_whitney_results = {}
        for i in range(len(model_names)):
            for j in range(i + 1, len(model_names)):
                model1, model2 = model_names[i], model_names[j]
                
                scores1 = all_predictions[model1]['y_scores']
                scores2 = all_predictions[model2]['y_scores']
                
                try:
                    statistic, p_value = mannwhitneyu(scores1, scores2, alternative='two-sided')
                    mann_whitney_results[f"{model1}_vs_{model2}"] = {
                        'statistic': float(statistic),
                        'p_value': float(p_value),
                        'significant': p_value < 0.05
                    }
                except Exception as e:
                    logger.warning(f"Mann-Whitney test failed for {model1} vs {model2}: {e}")
        
        tests['mann_whitney_tests'] = mann_whitney_results
        
        return tests
    
    def _create_comparison_summary(self, model_results: Dict[str, Dict]) -> pd.DataFrame:
        """Create summary table comparing model metrics."""
        summary_data = []
        
        for model_name, result in model_results.items():
            row = {'model_name': model_name}
            
            # Add main metrics
            metrics = result.get('metrics', {})
            for metric in ['accuracy', 'precision', 'recall', 'f1_score', 'roc_auc']:
                row[metric] = metrics.get(metric, np.nan)
            
            # Add confidence intervals
            ci = result.get('confidence_intervals', {})
            if 'f1_score' in ci:
                ci_low, ci_high = ci['f1_score']
                row['f1_score_ci'] = f"({ci_low:.3f}, {ci_high:.3f})"
            
            summary_data.append(row)
        
        return pd.DataFrame(summary_data)
    
    def _rank_models(self, model_results: Dict[str, Dict]) -> List[Dict[str, Any]]:
        """Rank models based on performance metrics."""
        rankings = []
        
        # Primary ranking metric (F1 score)
        for model_name, result in model_results.items():
            metrics = result.get('metrics', {})
            f1_score = metrics.get('f1_score', 0.0)
            
            rankings.append({
                'model_name': model_name,
                'f1_score': f1_score,
                'precision': metrics.get('precision', 0.0),
                'recall': metrics.get('recall', 0.0),
                'roc_auc': metrics.get('roc_auc', 0.0)
            })
        
        # Sort by F1 score descending
        rankings.sort(key=lambda x: x['f1_score'], reverse=True)
        
        # Add rank
        for i, ranking in enumerate(rankings):
            ranking['rank'] = i + 1
        
        return rankings
    
    def backtest_model(
        self,
        model: BaseAnomalyDetector,
        X: np.ndarray,
        y: np.ndarray,
        timestamps: Optional[np.ndarray] = None,
        config: Optional[BacktestConfig] = None
    ) -> Dict[str, Any]:
        """
        Perform time-series backtesting of the model.
        
        Args:
            model: Model to backtest
            X: Full dataset features
            y: Full dataset labels
            timestamps: Optional timestamps for data points
            config: Backtest configuration
            
        Returns:
            Backtesting results
        """
        config = config or BacktestConfig()
        logger.info(f"Starting backtest for {model.name}")
        
        if timestamps is None:
            timestamps = np.arange(len(X))
        
        backtest_results = []
        model_performance = []
        
        # Time series walk-forward validation
        for i in range(config.min_train_size, len(X) - config.window_size, config.step_size):
            train_end = i
            test_start = i
            test_end = min(i + config.window_size, len(X))
            
            # Training data (expanding window)
            X_train = X[:train_end]
            y_train = y[:train_end]
            
            # Test data
            X_test = X[test_start:test_end]
            y_test = y[test_start:test_end]
            
            try:
                # Retrain model if needed
                if i % config.retrain_frequency == 0:
                    logger.debug(f"Retraining model at step {i}")
                    model.fit(X_train, y_train)
                
                # Make predictions
                y_pred = model.predict(X_test)
                y_scores = model.predict_proba(X_test)
                
                # Evaluate
                step_metrics = self._calculate_metrics(y_test, y_pred, y_scores)
                
                # Store results
                step_result = {
                    'step': i,
                    'train_end': train_end,
                    'test_start': test_start,
                    'test_end': test_end,
                    'train_size': train_end,
                    'test_size': test_end - test_start,
                    'timestamp': timestamps[test_start] if timestamps is not None else test_start,
                    'metrics': step_metrics,
                    'anomaly_rate': np.mean(y_test),
                    'predicted_anomaly_rate': np.mean(y_pred)
                }
                
                backtest_results.append(step_result)
                
                # Track model performance
                f1_score = step_metrics.get('f1_score', 0.0)
                model_performance.append(f1_score)
                
                # Check for performance degradation
                if len(model_performance) > 10:  # Minimum window for trend analysis
                    recent_performance = np.mean(model_performance[-5:])
                    older_performance = np.mean(model_performance[-10:-5])
                    
                    if (older_performance - recent_performance) > config.drift_threshold:
                        logger.warning(f"Performance degradation detected at step {i}")
                        step_result['performance_alert'] = True
                
            except Exception as e:
                logger.error(f"Backtest failed at step {i}: {e}")
                continue
        
        # Analyze backtest results
        backtest_analysis = self._analyze_backtest_results(backtest_results, model_performance)
        
        final_results = {
            'model_name': model.name,
            'backtest_config': config.__dict__,
            'backtest_steps': len(backtest_results),
            'backtest_results': backtest_results,
            'analysis': backtest_analysis,
            'completed_timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Backtest completed. Average F1: {backtest_analysis.get('mean_f1', 'N/A'):.4f}")
        return final_results
    
    def _analyze_backtest_results(
        self,
        backtest_results: List[Dict],
        model_performance: List[float]
    ) -> Dict[str, Any]:
        """Analyze backtesting results for trends and insights."""
        if not backtest_results:
            return {}
        
        # Extract metrics over time
        f1_scores = [r['metrics'].get('f1_score', 0) for r in backtest_results]
        precisions = [r['metrics'].get('precision', 0) for r in backtest_results]
        recalls = [r['metrics'].get('recall', 0) for r in backtest_results]
        
        analysis = {
            'mean_f1': np.mean(f1_scores),
            'std_f1': np.std(f1_scores),
            'min_f1': np.min(f1_scores),
            'max_f1': np.max(f1_scores),
            'mean_precision': np.mean(precisions),
            'mean_recall': np.mean(recalls),
            'performance_trend': self._calculate_performance_trend(f1_scores),
            'stability_score': 1.0 - (np.std(f1_scores) / np.mean(f1_scores)) if np.mean(f1_scores) > 0 else 0,
            'performance_alerts': sum(1 for r in backtest_results if r.get('performance_alert', False))
        }
        
        return analysis
    
    def _calculate_performance_trend(self, performance_scores: List[float]) -> str:
        """Calculate performance trend over time."""
        if len(performance_scores) < 3:
            return "insufficient_data"
        
        # Simple linear regression to detect trend
        x = np.arange(len(performance_scores))
        y = np.array(performance_scores)
        
        # Calculate correlation coefficient
        correlation = np.corrcoef(x, y)[0, 1]
        
        if correlation > 0.1:
            return "improving"
        elif correlation < -0.1:
            return "declining"
        else:
            return "stable"
    
    def generate_evaluation_report(
        self,
        output_path: Optional[str] = None,
        include_plots: bool = True
    ) -> str:
        """Generate comprehensive evaluation report."""
        report_lines = []
        
        # Header
        report_lines.append("# Anomaly Detection Model Evaluation Report")
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")
        
        # Summary of evaluations
        if self.evaluation_history:
            report_lines.append("## Evaluation Summary")
            report_lines.append(f"Total evaluations performed: {len(self.evaluation_history)}")
            
            # Model performance summary
            for eval_result in self.evaluation_history[-5:]:  # Last 5 evaluations
                report_lines.append(f"### {eval_result['model_name']}")
                metrics = eval_result['metrics']
                report_lines.append(f"- F1 Score: {metrics.get('f1_score', 'N/A'):.4f}")
                report_lines.append(f"- Precision: {metrics.get('precision', 'N/A'):.4f}")
                report_lines.append(f"- Recall: {metrics.get('recall', 'N/A'):.4f}")
                report_lines.append("")
        
        # Comparison results
        if self.comparison_results:
            report_lines.append("## Model Comparisons")
            for comp_name, comp_result in self.comparison_results.items():
                report_lines.append(f"### {comp_name}")
                rankings = comp_result['model_rankings']
                for rank in rankings[:3]:  # Top 3 models
                    report_lines.append(f"{rank['rank']}. {rank['model_name']} - F1: {rank['f1_score']:.4f}")
                report_lines.append("")
        
        # Generate report text
        report_text = "\n".join(report_lines)
        
        # Save to file if path provided
        if output_path:
            with open(output_path, 'w') as f:
                f.write(report_text)
            logger.info(f"Evaluation report saved to {output_path}")
        
        return report_text