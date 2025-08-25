"""
Metrics Utilities

Track user activity and system metrics for monitoring and analytics.
"""

import streamlit as st
import time
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from prometheus_client import Counter, Histogram, Gauge, generate_latest
import threading
import queue
import requests

# Prometheus metrics
page_views = Counter('dashboard_page_views_total', 'Total page views', ['page', 'user_id'])
user_sessions = Counter('dashboard_user_sessions_total', 'Total user sessions', ['user_id', 'role'])
request_duration = Histogram('dashboard_request_duration_seconds', 'Request duration', ['endpoint'])
active_users = Gauge('dashboard_active_users', 'Number of active users')
component_load_time = Histogram('dashboard_component_load_seconds', 'Component load time', ['component'])
error_count = Counter('dashboard_errors_total', 'Total errors', ['error_type', 'component'])

class MetricsTracker:
    """Track dashboard metrics and user activity"""
    
    def __init__(self):
        self.metrics_queue = queue.Queue()
        self.session_start_time = time.time()
        self.initialize_session_tracking()
    
    def initialize_session_tracking(self):
        """Initialize session tracking"""
        
        if 'session_metrics' not in st.session_state:
            st.session_state.session_metrics = {
                'session_id': self.generate_session_id(),
                'start_time': datetime.now(),
                'page_views': 0,
                'components_viewed': set(),
                'actions_taken': [],
                'errors_encountered': []
            }
    
    def generate_session_id(self) -> str:
        """Generate unique session ID"""
        import uuid
        return str(uuid.uuid4())
    
    def track_page_view(self, page_name: str):
        """Track page view"""
        
        user_id = st.session_state.get('user_id', 'anonymous')
        
        # Update Prometheus metrics
        page_views.labels(page=page_name, user_id=user_id).inc()
        
        # Update session metrics
        st.session_state.session_metrics['page_views'] += 1
        st.session_state.session_metrics['components_viewed'].add(page_name)
        
        # Queue for batch processing
        self.queue_metric({
            'type': 'page_view',
            'page': page_name,
            'user_id': user_id,
            'timestamp': datetime.now().isoformat()
        })
    
    def track_component_load(self, component_name: str, load_time: float):
        """Track component load time"""
        
        component_load_time.labels(component=component_name).observe(load_time)
        
        self.queue_metric({
            'type': 'component_load',
            'component': component_name,
            'load_time': load_time,
            'timestamp': datetime.now().isoformat()
        })
    
    def track_user_action(self, action: str, details: Optional[Dict[str, Any]] = None):
        """Track user action"""
        
        action_data = {
            'action': action,
            'timestamp': datetime.now().isoformat(),
            'user_id': st.session_state.get('user_id', 'anonymous'),
            'details': details or {}
        }
        
        st.session_state.session_metrics['actions_taken'].append(action_data)
        
        self.queue_metric({
            'type': 'user_action',
            **action_data
        })
    
    def track_error(self, error_type: str, component: str, error_message: str):
        """Track error occurrence"""
        
        error_count.labels(error_type=error_type, component=component).inc()
        
        error_data = {
            'error_type': error_type,
            'component': component,
            'error_message': error_message,
            'timestamp': datetime.now().isoformat(),
            'user_id': st.session_state.get('user_id', 'anonymous')
        }
        
        st.session_state.session_metrics['errors_encountered'].append(error_data)
        
        self.queue_metric({
            'type': 'error',
            **error_data
        })
    
    def queue_metric(self, metric_data: Dict[str, Any]):
        """Queue metric for batch processing"""
        
        try:
            self.metrics_queue.put_nowait(metric_data)
        except queue.Full:
            # If queue is full, skip this metric
            pass
    
    def get_session_summary(self) -> Dict[str, Any]:
        """Get session summary"""
        
        session_metrics = st.session_state.get('session_metrics', {})
        current_time = datetime.now()
        start_time = session_metrics.get('start_time', current_time)
        
        session_duration = (current_time - start_time).total_seconds()
        
        return {
            'session_id': session_metrics.get('session_id'),
            'duration_seconds': session_duration,
            'duration_formatted': self.format_duration(session_duration),
            'page_views': session_metrics.get('page_views', 0),
            'components_viewed': len(session_metrics.get('components_viewed', set())),
            'actions_count': len(session_metrics.get('actions_taken', [])),
            'errors_count': len(session_metrics.get('errors_encountered', []))
        }
    
    def format_duration(self, seconds: float) -> str:
        """Format duration in human-readable format"""
        
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}h"
    
    def export_metrics(self) -> str:
        """Export Prometheus metrics"""
        return generate_latest().decode('utf-8')

# Global metrics tracker instance
metrics_tracker = MetricsTracker()

def track_user_activity():
    """Track user activity for current session"""
    
    # Track session start
    user_id = st.session_state.get('user_id', 'anonymous')
    user_role = st.session_state.get('user_role', 'unknown')
    
    # Update active users gauge
    active_users.set(len(get_active_users()))
    
    # Track session
    user_sessions.labels(user_id=user_id, role=user_role).inc()

def get_active_users() -> List[str]:
    """Get list of currently active users"""
    
    # In a real implementation, this would query active sessions from a database
    # For now, return mock data
    return ['admin', 'analyst', 'trader']

def track_component_performance(component_name: str):
    """Decorator to track component performance"""
    
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                load_time = time.time() - start_time
                metrics_tracker.track_component_load(component_name, load_time)
                return result
            except Exception as e:
                load_time = time.time() - start_time
                metrics_tracker.track_error('component_error', component_name, str(e))
                raise
        
        return wrapper
    return decorator

def send_metrics_to_endpoint(metrics_data: Dict[str, Any], endpoint_url: str):
    """Send metrics to external endpoint"""
    
    try:
        response = requests.post(
            endpoint_url,
            json=metrics_data,
            timeout=5,
            headers={'Content-Type': 'application/json'}
        )
        response.raise_for_status()
    except requests.RequestException as e:
        # Log error but don't fail the application
        print(f"Failed to send metrics: {e}")

def render_metrics_dashboard():
    """Render metrics dashboard for administrators"""
    
    if not st.session_state.get('user_role') == 'administrator':
        st.error("Access denied. Administrator role required.")
        return
    
    st.subheader("📊 Dashboard Metrics")
    
    # Session summary
    session_summary = metrics_tracker.get_session_summary()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Session Duration", session_summary['duration_formatted'])
    
    with col2:
        st.metric("Page Views", session_summary['page_views'])
    
    with col3:
        st.metric("Components Viewed", session_summary['components_viewed'])
    
    with col4:
        st.metric("User Actions", session_summary['actions_count'])
    
    # Recent activity
    st.subheader("Recent Activity")
    
    session_metrics = st.session_state.get('session_metrics', {})
    recent_actions = session_metrics.get('actions_taken', [])[-10:]  # Last 10 actions
    
    if recent_actions:
        actions_df = pd.DataFrame([
            {
                'Time': action['timestamp'],
                'Action': action['action'],
                'User': action['user_id'],
                'Details': str(action.get('details', {}))
            }
            for action in recent_actions
        ])
        
        st.dataframe(actions_df, hide_index=True, use_container_width=True)
    else:
        st.info("No recent activity to display")
    
    # Error log
    if session_metrics.get('errors_encountered'):
        st.subheader("⚠️ Errors")
        
        errors_df = pd.DataFrame([
            {
                'Time': error['timestamp'],
                'Type': error['error_type'],
                'Component': error['component'],
                'Message': error['error_message']
            }
            for error in session_metrics['errors_encountered']
        ])
        
        st.dataframe(errors_df, hide_index=True, use_container_width=True)
    
    # Export options
    st.subheader("Export Options")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📥 Export Session Data"):
            session_data = json.dumps(session_metrics, indent=2, default=str)
            st.download_button(
                label="Download Session Data",
                data=session_data,
                file_name=f"session_data_{session_summary['session_id']}.json",
                mime="application/json"
            )
    
    with col2:
        if st.button("📊 Export Prometheus Metrics"):
            prometheus_data = metrics_tracker.export_metrics()
            st.download_button(
                label="Download Metrics",
                data=prometheus_data,
                file_name="dashboard_metrics.txt",
                mime="text/plain"
            )

class PerformanceMonitor:
    """Monitor dashboard performance"""
    
    def __init__(self):
        self.start_time = time.time()
        self.checkpoints = {}
    
    def checkpoint(self, name: str):
        """Record a performance checkpoint"""
        self.checkpoints[name] = time.time() - self.start_time
    
    def get_timing_report(self) -> Dict[str, float]:
        """Get timing report"""
        return self.checkpoints.copy()
    
    def render_performance_info(self):
        """Render performance information"""
        
        total_time = time.time() - self.start_time
        
        with st.expander("⚡ Performance Info"):
            st.metric("Total Load Time", f"{total_time:.2f}s")
            
            if self.checkpoints:
                checkpoint_df = pd.DataFrame([
                    {'Checkpoint': name, 'Time (s)': f"{time_val:.3f}"}
                    for name, time_val in self.checkpoints.items()
                ])
                
                st.dataframe(checkpoint_df, hide_index=True, use_container_width=True)

# Global performance monitor
performance_monitor = PerformanceMonitor()

def log_user_interaction(interaction_type: str, details: Dict[str, Any] = None):
    """Log user interaction for analytics"""
    
    metrics_tracker.track_user_action(interaction_type, details)
    
    # Also log to console in debug mode
    if st.session_state.get('debug_mode', False):
        print(f"User Interaction: {interaction_type} - {details}")

import pandas as pd

def get_usage_analytics() -> Dict[str, Any]:
    """Get usage analytics data"""
    
    # In a real implementation, this would query a database
    # For now, return mock analytics data
    
    return {
        'daily_active_users': 150,
        'weekly_active_users': 450,
        'monthly_active_users': 1200,
        'avg_session_duration': 1800,  # seconds
        'bounce_rate': 0.25,
        'most_viewed_components': [
            'Market Data', 'Technical Analysis', 'Portfolio', 'Alerts'
        ],
        'peak_usage_hours': [9, 10, 11, 14, 15, 16],
        'error_rate': 0.02,
        'performance_score': 0.95
    }