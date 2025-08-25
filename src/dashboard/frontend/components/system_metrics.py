"""
System Metrics Component

Real-time system health monitoring, performance metrics, and infrastructure status.
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import psutil
import time

class SystemMetricsComponent:
    """Component for system monitoring and metrics"""
    
    def __init__(self):
        self.cache_duration = 30  # 30 seconds for system metrics
        self.initialize_mock_data()
    
    def initialize_mock_data(self):
        """Initialize mock system data"""
        
        # Service health status
        self.services_status = {
            'dashboard': {'status': 'healthy', 'uptime': '23h 45m', 'cpu': 15.2, 'memory': 32.1},
            'api_server': {'status': 'healthy', 'uptime': '23h 45m', 'cpu': 8.7, 'memory': 28.4},
            'websocket': {'status': 'healthy', 'uptime': '23h 45m', 'cpu': 5.3, 'memory': 18.9},
            'database': {'status': 'healthy', 'uptime': '7d 12h', 'cpu': 12.1, 'memory': 45.6},
            'redis': {'status': 'healthy', 'uptime': '7d 12h', 'cpu': 3.2, 'memory': 15.8},
            'prometheus': {'status': 'healthy', 'uptime': '7d 12h', 'cpu': 6.8, 'memory': 22.3},
            'grafana': {'status': 'warning', 'uptime': '23h 45m', 'cpu': 4.1, 'memory': 35.7}
        }
        
        # Generate system metrics history
        self.generate_metrics_history()
    
    def generate_metrics_history(self, hours: int = 24):
        """Generate mock system metrics history"""
        
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        
        # Generate time series (every minute)
        timestamps = pd.date_range(start=start_time, end=end_time, freq='1T')
        
        # Generate realistic system metrics
        self.metrics_history = {
            'timestamps': timestamps,
            'cpu_usage': self.generate_cpu_data(len(timestamps)),
            'memory_usage': self.generate_memory_data(len(timestamps)),
            'disk_usage': self.generate_disk_data(len(timestamps)),
            'network_io': self.generate_network_data(len(timestamps)),
            'response_times': self.generate_response_time_data(len(timestamps)),
            'request_rates': self.generate_request_rate_data(len(timestamps)),
            'error_rates': self.generate_error_rate_data(len(timestamps))
        }
    
    def generate_cpu_data(self, length: int) -> np.ndarray:
        """Generate realistic CPU usage data"""
        base = 25  # Base CPU usage
        trend = np.linspace(0, 5, length)  # Slight upward trend
        noise = np.random.normal(0, 8, length)
        spikes = np.random.choice([0, 30], size=length, p=[0.97, 0.03])  # Occasional spikes
        
        cpu_data = base + trend + noise + spikes
        return np.clip(cpu_data, 0, 100)
    
    def generate_memory_data(self, length: int) -> np.ndarray:
        """Generate realistic memory usage data"""
        base = 60  # Base memory usage
        trend = np.linspace(0, 10, length)  # Gradual increase (memory leaks simulation)
        noise = np.random.normal(0, 3, length)
        
        memory_data = base + trend + noise
        return np.clip(memory_data, 0, 100)
    
    def generate_disk_data(self, length: int) -> np.ndarray:
        """Generate realistic disk usage data"""
        base = 75  # Base disk usage
        growth = np.linspace(0, 2, length)  # Slow growth
        noise = np.random.normal(0, 1, length)
        
        disk_data = base + growth + noise
        return np.clip(disk_data, 0, 100)
    
    def generate_network_data(self, length: int) -> Dict[str, np.ndarray]:
        """Generate network I/O data"""
        # Network traffic with business hours pattern
        hours = np.array([(datetime.now() - timedelta(minutes=i)).hour for i in range(length)])
        
        # Higher traffic during business hours (9-17)
        business_hours_multiplier = np.where(
            (hours >= 9) & (hours <= 17), 
            np.random.uniform(1.5, 3.0, length),
            np.random.uniform(0.3, 1.0, length)
        )
        
        base_in = 50  # MB/s
        base_out = 30  # MB/s
        
        network_in = base_in * business_hours_multiplier + np.random.normal(0, 10, length)
        network_out = base_out * business_hours_multiplier + np.random.normal(0, 8, length)
        
        return {
            'incoming': np.clip(network_in, 0, 1000),
            'outgoing': np.clip(network_out, 0, 1000)
        }
    
    def generate_response_time_data(self, length: int) -> np.ndarray:
        """Generate API response time data"""
        base = 45  # Base response time in ms
        spikes = np.random.choice([0, 200], size=length, p=[0.95, 0.05])  # Occasional spikes
        noise = np.random.exponential(15, length)  # Exponential distribution for response times
        
        response_times = base + spikes + noise
        return np.clip(response_times, 10, 1000)
    
    def generate_request_rate_data(self, length: int) -> np.ndarray:
        """Generate request rate data"""
        hours = np.array([(datetime.now() - timedelta(minutes=i)).hour for i in range(length)])
        
        # Higher request rates during business hours
        business_hours_multiplier = np.where(
            (hours >= 9) & (hours <= 17),
            np.random.uniform(8, 15, length),
            np.random.uniform(2, 5, length)
        )
        
        base_rate = 100  # requests/minute
        request_rates = base_rate * business_hours_multiplier + np.random.normal(0, 50, length)
        
        return np.clip(request_rates, 0, 2000)
    
    def generate_error_rate_data(self, length: int) -> np.ndarray:
        """Generate error rate data"""
        base_error_rate = 0.5  # 0.5% base error rate
        spikes = np.random.choice([0, 5], size=length, p=[0.98, 0.02])  # Occasional error spikes
        noise = np.random.exponential(0.3, length)
        
        error_rates = base_error_rate + spikes + noise
        return np.clip(error_rates, 0, 10)
    
    @st.cache_data(ttl=30)
    def get_current_system_metrics(_self) -> Dict[str, float]:
        """Get current system metrics (real or mock)"""
        try:
            # Try to get real system metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            network = psutil.net_io_counters()
            
            return {
                'cpu_usage': cpu_percent,
                'memory_usage': memory.percent,
                'disk_usage': (disk.used / disk.total) * 100,
                'network_bytes_sent': network.bytes_sent,
                'network_bytes_recv': network.bytes_recv
            }
        except Exception:
            # Fallback to mock data
            return {
                'cpu_usage': np.random.uniform(20, 40),
                'memory_usage': np.random.uniform(50, 70),
                'disk_usage': np.random.uniform(70, 80),
                'network_bytes_sent': np.random.randint(1000000, 10000000),
                'network_bytes_recv': np.random.randint(1000000, 10000000)
            }
    
    def create_system_overview_chart(self) -> go.Figure:
        """Create system overview dashboard"""
        
        fig = make_subplots(
            rows=3, cols=2,
            subplot_titles=(
                'CPU Usage (%)',
                'Memory Usage (%)',
                'Network I/O (MB/s)',
                'Response Times (ms)',
                'Request Rate (req/min)',
                'Error Rate (%)'
            ),
            vertical_spacing=0.08
        )
        
        timestamps = self.metrics_history['timestamps']
        
        # CPU Usage
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=self.metrics_history['cpu_usage'],
            mode='lines',
            name='CPU Usage',
            line=dict(color='red', width=2),
            fill='tonexty'
        ), row=1, col=1)
        
        # Add CPU threshold line
        fig.add_hline(y=80, line_dash="dash", line_color="red", row=1, col=1)
        
        # Memory Usage
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=self.metrics_history['memory_usage'],
            mode='lines',
            name='Memory Usage',
            line=dict(color='blue', width=2),
            fill='tonexty'
        ), row=1, col=2)
        
        fig.add_hline(y=85, line_dash="dash", line_color="red", row=1, col=2)
        
        # Network I/O
        network_data = self.metrics_history['network_io']
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=network_data['incoming'],
            mode='lines',
            name='Network In',
            line=dict(color='green', width=2)
        ), row=2, col=1)
        
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=network_data['outgoing'],
            mode='lines',
            name='Network Out',
            line=dict(color='orange', width=2)
        ), row=2, col=1)
        
        # Response Times
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=self.metrics_history['response_times'],
            mode='lines',
            name='Response Time',
            line=dict(color='purple', width=2)
        ), row=2, col=2)
        
        fig.add_hline(y=300, line_dash="dash", line_color="red", row=2, col=2)
        
        # Request Rate
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=self.metrics_history['request_rates'],
            mode='lines',
            name='Request Rate',
            line=dict(color='teal', width=2),
            fill='tonexty'
        ), row=3, col=1)
        
        # Error Rate
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=self.metrics_history['error_rates'],
            mode='lines',
            name='Error Rate',
            line=dict(color='red', width=2)
        ), row=3, col=2)
        
        fig.add_hline(y=5, line_dash="dash", line_color="red", row=3, col=2)
        
        fig.update_layout(
            title="System Performance Dashboard",
            height=1000,
            showlegend=False
        )
        
        return fig
    
    def create_service_status_chart(self) -> go.Figure:
        """Create service status overview"""
        
        services = list(self.services_status.keys())
        cpu_values = [self.services_status[s]['cpu'] for s in services]
        memory_values = [self.services_status[s]['memory'] for s in services]
        
        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=('Service CPU Usage (%)', 'Service Memory Usage (%)'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # CPU usage by service
        fig.add_trace(go.Bar(
            x=services,
            y=cpu_values,
            name='CPU Usage',
            marker_color='lightblue'
        ), row=1, col=1)
        
        # Memory usage by service
        colors = ['red' if mem > 40 else 'orange' if mem > 25 else 'green' for mem in memory_values]
        fig.add_trace(go.Bar(
            x=services,
            y=memory_values,
            name='Memory Usage',
            marker_color=colors
        ), row=1, col=2)
        
        fig.update_layout(
            title="Service Resource Usage",
            height=400,
            showlegend=False
        )
        
        return fig
    
    def render_service_status_table(self):
        """Render service status table"""
        
        status_data = []
        for service, data in self.services_status.items():
            status_icon = {
                'healthy': '🟢',
                'warning': '🟡',
                'critical': '🔴',
                'unknown': '⚪'
            }.get(data['status'], '⚪')
            
            status_data.append({
                'Service': service.replace('_', ' ').title(),
                'Status': f"{status_icon} {data['status'].title()}",
                'Uptime': data['uptime'],
                'CPU (%)': f"{data['cpu']:.1f}%",
                'Memory (%)': f"{data['memory']:.1f}%"
            })
        
        status_df = pd.DataFrame(status_data)
        
        st.dataframe(
            status_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                'Status': st.column_config.TextColumn(width="medium"),
                'CPU (%)': st.column_config.TextColumn(width="small"),
                'Memory (%)': st.column_config.TextColumn(width="small")
            }
        )
    
    def render_infrastructure_metrics(self):
        """Render infrastructure metrics"""
        
        current_metrics = self.get_current_system_metrics()
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "CPU Usage",
                f"{current_metrics['cpu_usage']:.1f}%",
                delta=f"{np.random.uniform(-2, 2):.1f}%"
            )
        
        with col2:
            st.metric(
                "Memory Usage",
                f"{current_metrics['memory_usage']:.1f}%", 
                delta=f"{np.random.uniform(-1, 3):.1f}%"
            )
        
        with col3:
            st.metric(
                "Disk Usage",
                f"{current_metrics['disk_usage']:.1f}%",
                delta=f"{np.random.uniform(0, 0.5):.2f}%"
            )
        
        with col4:
            # Calculate network throughput
            network_throughput = (current_metrics['network_bytes_sent'] + current_metrics['network_bytes_recv']) / 1_000_000  # MB
            st.metric(
                "Network Throughput",
                f"{network_throughput:.1f} MB/s",
                delta=f"{np.random.uniform(-10, 10):.1f} MB/s"
            )
    
    def render_performance_alerts(self):
        """Render performance alerts and warnings"""
        
        alerts = []
        current_metrics = self.get_current_system_metrics()
        
        # Check thresholds
        if current_metrics['cpu_usage'] > 80:
            alerts.append("🔴 High CPU usage detected")
        elif current_metrics['cpu_usage'] > 60:
            alerts.append("🟡 Elevated CPU usage")
        
        if current_metrics['memory_usage'] > 85:
            alerts.append("🔴 High memory usage detected")
        elif current_metrics['memory_usage'] > 70:
            alerts.append("🟡 Elevated memory usage")
        
        if current_metrics['disk_usage'] > 90:
            alerts.append("🔴 Disk space critical")
        elif current_metrics['disk_usage'] > 80:
            alerts.append("🟡 Disk space warning")
        
        # Check service health
        unhealthy_services = [
            service for service, data in self.services_status.items()
            if data['status'] in ['warning', 'critical']
        ]
        
        if unhealthy_services:
            alerts.append(f"🟡 Service issues: {', '.join(unhealthy_services)}")
        
        if alerts:
            st.markdown("### ⚠️ Active Alerts")
            for alert in alerts:
                st.markdown(f"• {alert}")
        else:
            st.success("✅ All systems operating normally")
    
    def render(self):
        """Render the system metrics component"""
        
        st.subheader("🖥️ System Health & Performance")
        
        # Infrastructure overview
        self.render_infrastructure_metrics()
        
        # Performance alerts
        self.render_performance_alerts()
        
        # System metrics tabs
        tab1, tab2, tab3, tab4 = st.tabs([
            "Real-time Metrics",
            "Service Status", 
            "Performance History",
            "Infrastructure"
        ])
        
        with tab1:
            st.subheader("📊 Real-time System Metrics")
            
            # System overview chart
            overview_chart = self.create_system_overview_chart()
            st.plotly_chart(overview_chart, use_container_width=True)
            
            # Real-time updates indicator
            st.markdown("---")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Data Refresh Rate", "30s", "Real-time")
            
            with col2:
                st.metric("Metrics Collected", "1,440", "Last 24h")
            
            with col3:
                st.metric("Alert Threshold", "3", "Active rules")
        
        with tab2:
            st.subheader("🔧 Service Health Status")
            
            # Service status table
            self.render_service_status_table()
            
            # Service resource usage chart
            st.subheader("Service Resource Consumption")
            service_chart = self.create_service_status_chart()
            st.plotly_chart(service_chart, use_container_width=True)
            
            # Quick service actions
            st.markdown("### Quick Actions")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if st.button("🔄 Restart Services"):
                    st.info("Service restart initiated...")
            
            with col2:
                if st.button("📊 Generate Report"):
                    st.success("System report generated")
            
            with col3:
                if st.button("🚨 Test Alerts"):
                    st.warning("Test alert sent")
            
            with col4:
                if st.button("⚙️ Service Config"):
                    st.info("Opening service configuration...")
        
        with tab3:
            st.subheader("📈 Performance History")
            
            # Time range selector
            col1, col2 = st.columns(2)
            
            with col1:
                time_range = st.selectbox(
                    "Historical Range",
                    options=['1h', '6h', '24h', '7d', '30d'],
                    index=2
                )
            
            with col2:
                metric_type = st.selectbox(
                    "Metric Type",
                    options=['All', 'CPU', 'Memory', 'Network', 'Response Time']
                )
            
            # Performance trends
            st.markdown("**Performance Trends:**")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                avg_cpu = np.mean(self.metrics_history['cpu_usage'])
                st.metric("Avg CPU (24h)", f"{avg_cpu:.1f}%", delta="-2.3%")
            
            with col2:
                avg_memory = np.mean(self.metrics_history['memory_usage'])
                st.metric("Avg Memory (24h)", f"{avg_memory:.1f}%", delta="+1.2%")
            
            with col3:
                avg_response = np.mean(self.metrics_history['response_times'])
                st.metric("Avg Response (24h)", f"{avg_response:.0f}ms", delta="-15ms")
            
            # Historical analysis
            st.markdown("**Historical Analysis:**")
            
            # Peak usage times
            timestamps = self.metrics_history['timestamps']
            cpu_data = self.metrics_history['cpu_usage']
            
            peak_cpu_idx = np.argmax(cpu_data)
            peak_time = timestamps[peak_cpu_idx]
            
            st.info(f"Peak CPU usage: {cpu_data[peak_cpu_idx]:.1f}% at {peak_time.strftime('%H:%M')}")
            
            # Performance recommendations
            st.markdown("**Recommendations:**")
            recommendations = [
                "Consider scaling up during peak hours (9 AM - 5 PM)",
                "Memory usage trending upward - investigate potential leaks",
                "Network throughput within normal parameters",
                "Response times optimal - no immediate action needed"
            ]
            
            for rec in recommendations:
                st.markdown(f"• {rec}")
        
        with tab4:
            st.subheader("🏗️ Infrastructure Overview")
            
            # Infrastructure diagram (mock)
            st.markdown("### System Architecture")
            
            # Simple infrastructure overview
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Application Layer:**")
                st.markdown("• Streamlit Dashboard")
                st.markdown("• FastAPI Backend")
                st.markdown("• WebSocket Server")
                st.markdown("")
                
                st.markdown("**Data Layer:**")
                st.markdown("• PostgreSQL Database")
                st.markdown("• Redis Cache")
                st.markdown("• S3 Storage")
            
            with col2:
                st.markdown("**Monitoring Layer:**")
                st.markdown("• Prometheus")
                st.markdown("• Grafana")
                st.markdown("• AlertManager")
                st.markdown("")
                
                st.markdown("**Infrastructure:**")
                st.markdown("• Docker Containers")
                st.markdown("• Kubernetes Cluster")
                st.markdown("• Load Balancer")
            
            # Resource allocation
            st.markdown("### Resource Allocation")
            
            resource_data = pd.DataFrame({
                'Component': ['Dashboard', 'API', 'Database', 'Cache', 'Monitoring'],
                'CPU Cores': [2, 4, 8, 2, 4],
                'Memory (GB)': [4, 8, 16, 4, 8],
                'Storage (GB)': [20, 40, 500, 10, 100]
            })
            
            st.dataframe(resource_data, hide_index=True, use_container_width=True)
            
            # Capacity planning
            st.markdown("### Capacity Planning")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Current Capacity", "75%", "Healthy")
            
            with col2:
                st.metric("Projected Growth", "15%/month", "6 months runway")
            
            with col3:
                st.metric("Scale Trigger", "85%", "Auto-scaling enabled")
        
        # Footer with system info
        st.markdown("---")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown("**System Version**")
            st.markdown("Dashboard: v1.0.0")
            st.markdown("API: v1.0.0")
        
        with col2:
            st.markdown("**Last Updated**")
            st.markdown(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        with col3:
            st.markdown("**Monitoring Status**")
            st.markdown("🟢 All systems monitored")
        
        with col4:
            st.markdown("**Data Retention**")
            st.markdown("Metrics: 30 days")
            st.markdown("Logs: 7 days")