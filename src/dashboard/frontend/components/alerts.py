"""
Alerts Component

Real-time alert management, notifications, and anomaly detection system.
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import json

class AlertsComponent:
    """Component for managing alerts and notifications"""
    
    def __init__(self):
        self.alert_types = {
            'price': 'Price Alert',
            'volume': 'Volume Alert', 
            'technical': 'Technical Indicator',
            'anomaly': 'Anomaly Detection',
            'portfolio': 'Portfolio Alert',
            'system': 'System Alert'
        }
        
        self.severity_levels = {
            'low': {'color': 'green', 'icon': '🟢'},
            'medium': {'color': 'orange', 'icon': '🟡'},
            'high': {'color': 'red', 'icon': '🔴'},
            'critical': {'color': 'purple', 'icon': '🚨'}
        }
        
        self.initialize_mock_alerts()
    
    def initialize_mock_alerts(self):
        """Initialize mock alert data for demonstration"""
        
        self.mock_active_alerts = [
            {
                'id': 'ALT001',
                'type': 'price',
                'symbol': 'AAPL',
                'title': 'Price Target Reached',
                'message': 'AAPL has reached your target price of $180.00',
                'severity': 'medium',
                'timestamp': datetime.now() - timedelta(minutes=5),
                'status': 'active',
                'conditions': {'price': 180.00, 'condition': 'above'},
                'actions_taken': []
            },
            {
                'id': 'ALT002', 
                'type': 'volume',
                'symbol': 'TSLA',
                'title': 'Unusual Volume Activity',
                'message': 'TSLA volume is 300% above average',
                'severity': 'high',
                'timestamp': datetime.now() - timedelta(minutes=12),
                'status': 'active',
                'conditions': {'volume_multiplier': 3.0},
                'actions_taken': ['email_sent']
            },
            {
                'id': 'ALT003',
                'type': 'technical',
                'symbol': 'GOOGL',
                'title': 'RSI Oversold Signal',
                'message': 'GOOGL RSI has dropped below 30 (currently 28.5)',
                'severity': 'medium',
                'timestamp': datetime.now() - timedelta(minutes=8),
                'status': 'active',
                'conditions': {'rsi': 28.5, 'threshold': 30},
                'actions_taken': ['dashboard_notification']
            },
            {
                'id': 'ALT004',
                'type': 'anomaly',
                'symbol': 'MSFT',
                'title': 'Price Anomaly Detected',
                'message': 'MSFT price movement outside normal range',
                'severity': 'high',
                'timestamp': datetime.now() - timedelta(minutes=3),
                'status': 'active',
                'conditions': {'z_score': 3.2, 'threshold': 3.0},
                'actions_taken': ['alert_generated']
            },
            {
                'id': 'ALT005',
                'type': 'portfolio',
                'symbol': 'PORTFOLIO',
                'title': 'Position Size Limit',
                'message': 'AAPL position exceeds 25% portfolio allocation',
                'severity': 'low',
                'timestamp': datetime.now() - timedelta(minutes=15),
                'status': 'acknowledged',
                'conditions': {'allocation': 26.8, 'limit': 25.0},
                'actions_taken': ['email_sent', 'dashboard_notification']
            },
            {
                'id': 'ALT006',
                'type': 'system',
                'symbol': 'SYSTEM',
                'title': 'High API Latency',
                'message': 'Data feed latency above 100ms threshold',
                'severity': 'critical',
                'timestamp': datetime.now() - timedelta(minutes=1),
                'status': 'active',
                'conditions': {'latency': 145, 'threshold': 100},
                'actions_taken': ['escalation', 'slack_notification']
            }
        ]
        
        self.mock_alert_history = self.generate_alert_history()
    
    def generate_alert_history(self, days: int = 7) -> List[Dict]:
        """Generate mock alert history"""
        
        history = []
        end_date = datetime.now()
        
        # Generate alerts over the past week
        for day in range(days):
            date = end_date - timedelta(days=day)
            
            # Generate 3-8 alerts per day
            num_alerts = np.random.randint(3, 9)
            
            for i in range(num_alerts):
                alert_time = date.replace(
                    hour=np.random.randint(9, 16),
                    minute=np.random.randint(0, 60),
                    second=np.random.randint(0, 60)
                )
                
                alert_type = np.random.choice(list(self.alert_types.keys()))
                symbol = np.random.choice(['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'SYSTEM', 'PORTFOLIO'])
                severity = np.random.choice(['low', 'medium', 'high'], p=[0.4, 0.4, 0.2])
                
                history.append({
                    'id': f'HIST_{len(history):04d}',
                    'type': alert_type,
                    'symbol': symbol,
                    'title': f'{self.alert_types[alert_type]} - {symbol}',
                    'message': f'Generated alert for {symbol}',
                    'severity': severity,
                    'timestamp': alert_time,
                    'status': 'resolved',
                    'resolution_time': alert_time + timedelta(minutes=np.random.randint(5, 120))
                })
        
        return sorted(history, key=lambda x: x['timestamp'], reverse=True)
    
    def create_alerts_overview_chart(self) -> go.Figure:
        """Create alerts overview dashboard"""
        
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Alerts by Type',
                'Alerts by Severity', 
                'Alert Trends (7 Days)',
                'Response Times'
            ),
            specs=[
                [{"type": "pie"}, {"type": "bar"}],
                [{"type": "scatter"}, {"type": "histogram"}]
            ]
        )
        
        # Alerts by type
        all_alerts = self.mock_active_alerts + self.mock_alert_history[-50:]  # Last 50 historical
        type_counts = pd.Series([alert['type'] for alert in all_alerts]).value_counts()
        
        fig.add_trace(go.Pie(
            labels=[self.alert_types[t] for t in type_counts.index],
            values=type_counts.values,
            name="Alert Types"
        ), row=1, col=1)
        
        # Alerts by severity
        severity_counts = pd.Series([alert['severity'] for alert in all_alerts]).value_counts()
        severity_colors = [self.severity_levels[s]['color'] for s in severity_counts.index]
        
        fig.add_trace(go.Bar(
            x=[s.title() for s in severity_counts.index],
            y=severity_counts.values,
            marker_color=severity_colors,
            name="Severity"
        ), row=1, col=2)
        
        # Alert trends over time
        historical_df = pd.DataFrame(self.mock_alert_history)
        if not historical_df.empty:
            daily_counts = historical_df.groupby(historical_df['timestamp'].dt.date).size()
            
            fig.add_trace(go.Scatter(
                x=daily_counts.index,
                y=daily_counts.values,
                mode='lines+markers',
                name='Daily Alerts',
                line=dict(color='blue', width=2)
            ), row=2, col=1)
        
        # Response times (mock data)
        response_times = [np.random.exponential(30) for _ in range(100)]  # Minutes
        
        fig.add_trace(go.Histogram(
            x=response_times,
            nbinsx=20,
            name='Response Time Distribution',
            marker_color='lightblue'
        ), row=2, col=2)
        
        fig.update_layout(
            title="Alerts Management Dashboard",
            height=800,
            showlegend=True
        )
        
        return fig
    
    def create_alert_timeline(self) -> go.Figure:
        """Create alert timeline visualization"""
        
        all_alerts = sorted(
            self.mock_active_alerts + self.mock_alert_history[-20:],
            key=lambda x: x['timestamp'],
            reverse=True
        )[:50]  # Last 50 alerts
        
        fig = go.Figure()
        
        y_positions = list(range(len(all_alerts)))
        colors = [self.severity_levels[alert['severity']]['color'] for alert in all_alerts]
        
        # Create timeline
        fig.add_trace(go.Scatter(
            x=[alert['timestamp'] for alert in all_alerts],
            y=y_positions,
            mode='markers+text',
            marker=dict(
                size=12,
                color=colors,
                symbol='circle',
                line=dict(width=2, color='white')
            ),
            text=[f"{alert['symbol']} - {alert['title']}" for alert in all_alerts],
            textposition="middle right",
            name='Alerts'
        ))
        
        # Add status indicators
        active_alerts = [i for i, alert in enumerate(all_alerts) if alert['status'] == 'active']
        if active_alerts:
            fig.add_trace(go.Scatter(
                x=[all_alerts[i]['timestamp'] for i in active_alerts],
                y=[i for i in active_alerts],
                mode='markers',
                marker=dict(
                    size=20,
                    symbol='circle-open',
                    line=dict(width=3, color='red')
                ),
                name='Active Alerts',
                showlegend=True
            ))
        
        fig.update_layout(
            title="Alert Timeline",
            xaxis_title="Time",
            yaxis_title="Alert Sequence",
            height=600,
            showlegend=True,
            yaxis=dict(showticklabels=False)
        )
        
        return fig
    
    def create_alert_configuration_form(self):
        """Create alert configuration interface"""
        
        st.subheader("🔧 Create New Alert")
        
        with st.form("new_alert_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                alert_type = st.selectbox(
                    "Alert Type",
                    options=list(self.alert_types.keys()),
                    format_func=lambda x: self.alert_types[x]
                )
                
                symbol = st.text_input("Symbol", placeholder="AAPL, GOOGL, etc.")
                
                severity = st.selectbox(
                    "Severity Level",
                    options=['low', 'medium', 'high', 'critical']
                )
            
            with col2:
                if alert_type == 'price':
                    price_condition = st.selectbox("Condition", ["above", "below", "crosses"])
                    price_value = st.number_input("Price Target", min_value=0.01, value=100.00)
                
                elif alert_type == 'volume':
                    volume_condition = st.selectbox("Condition", ["above_average", "below_average", "spike"])
                    volume_multiplier = st.number_input("Volume Multiplier", min_value=1.0, value=2.0)
                
                elif alert_type == 'technical':
                    indicator = st.selectbox("Indicator", ["RSI", "MACD", "Bollinger Bands", "Moving Average"])
                    threshold = st.number_input("Threshold", value=70.0)
                
                elif alert_type == 'portfolio':
                    metric = st.selectbox("Metric", ["allocation", "return", "drawdown", "risk"])
                    limit = st.number_input("Limit", value=25.0)
            
            # Notification settings
            st.markdown("**Notification Channels:**")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                email_notify = st.checkbox("Email", value=True)
            with col2:
                slack_notify = st.checkbox("Slack")
            with col3:
                webhook_notify = st.checkbox("Webhook")
            
            message = st.text_area("Custom Message (optional)")
            
            submitted = st.form_submit_button("Create Alert")
            
            if submitted:
                st.success(f"✅ Alert created for {symbol} - {self.alert_types[alert_type]}")
                st.balloons()
    
    def render_active_alerts_table(self):
        """Render active alerts table"""
        
        if not self.mock_active_alerts:
            st.info("No active alerts at this time.")
            return
        
        # Prepare data for display
        alerts_data = []
        for alert in self.mock_active_alerts:
            alerts_data.append({
                'Severity': self.severity_levels[alert['severity']]['icon'],
                'Type': self.alert_types[alert['type']],
                'Symbol': alert['symbol'],
                'Title': alert['title'],
                'Message': alert['message'],
                'Time': alert['timestamp'].strftime('%H:%M:%S'),
                'Status': alert['status'].title(),
                'Actions': len(alert['actions_taken'])
            })
        
        alerts_df = pd.DataFrame(alerts_data)
        
        # Display with formatting
        st.dataframe(
            alerts_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                'Severity': st.column_config.TextColumn(width="small"),
                'Type': st.column_config.TextColumn(width="medium"),
                'Symbol': st.column_config.TextColumn(width="small"),
                'Message': st.column_config.TextColumn(width="large"),
                'Actions': st.column_config.NumberColumn(width="small")
            }
        )
        
        # Alert actions
        st.markdown("### Quick Actions")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("🔕 Acknowledge All"):
                st.success("All alerts acknowledged")
        
        with col2:
            if st.button("📧 Send Summary"):
                st.success("Alert summary sent via email")
        
        with col3:
            if st.button("🔄 Refresh Alerts"):
                st.rerun()
        
        with col4:
            if st.button("⚙️ Manage Rules"):
                st.info("Opening alert management...")
    
    def render_alert_statistics(self):
        """Render alert statistics"""
        
        # Current statistics
        active_count = len([a for a in self.mock_active_alerts if a['status'] == 'active'])
        acknowledged_count = len([a for a in self.mock_active_alerts if a['status'] == 'acknowledged'])
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Active Alerts", active_count, delta="+2 from last hour")
        
        with col2:
            st.metric("Acknowledged", acknowledged_count, delta="-1 from last hour")
        
        with col3:
            critical_count = len([a for a in self.mock_active_alerts if a['severity'] == 'critical'])
            st.metric("Critical Alerts", critical_count, delta="0 from last hour")
        
        with col4:
            avg_response_time = 15  # Mock average response time in minutes
            st.metric("Avg Response Time", f"{avg_response_time}m", delta="-3m improvement")
    
    def render(self):
        """Render the alerts component"""
        
        st.subheader("🚨 Alerts & Notifications")
        
        # Alert statistics
        self.render_alert_statistics()
        
        # Alert management tabs
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "Active Alerts", 
            "Alert History", 
            "Create Alert", 
            "Analytics", 
            "Settings"
        ])
        
        with tab1:
            st.subheader("🔴 Active Alerts")
            
            # Filter controls
            col1, col2, col3 = st.columns(3)
            
            with col1:
                severity_filter = st.selectbox(
                    "Filter by Severity",
                    options=['All'] + list(self.severity_levels.keys()),
                    key="severity_filter"
                )
            
            with col2:
                type_filter = st.selectbox(
                    "Filter by Type",
                    options=['All'] + list(self.alert_types.keys()),
                    format_func=lambda x: x if x == 'All' else self.alert_types[x],
                    key="type_filter"
                )
            
            with col3:
                symbol_filter = st.text_input("Filter by Symbol", key="symbol_filter")
            
            # Filter alerts based on selections
            filtered_alerts = self.mock_active_alerts.copy()
            
            if severity_filter != 'All':
                filtered_alerts = [a for a in filtered_alerts if a['severity'] == severity_filter]
            
            if type_filter != 'All':
                filtered_alerts = [a for a in filtered_alerts if a['type'] == type_filter]
            
            if symbol_filter:
                filtered_alerts = [a for a in filtered_alerts 
                                 if symbol_filter.upper() in a['symbol'].upper()]
            
            # Display filtered alerts
            if filtered_alerts:
                self.mock_active_alerts = filtered_alerts  # Temporarily update for display
                self.render_active_alerts_table()
            else:
                st.info("No alerts match the current filters.")
        
        with tab2:
            st.subheader("📚 Alert History")
            
            # Time range selector
            col1, col2 = st.columns(2)
            
            with col1:
                days_back = st.selectbox("Show alerts from", [1, 3, 7, 14, 30], index=2)
            
            with col2:
                show_resolved = st.checkbox("Include resolved alerts", value=True)
            
            # Alert timeline
            st.subheader("Alert Timeline")
            timeline_chart = self.create_alert_timeline()
            st.plotly_chart(timeline_chart, use_container_width=True)
            
            # Historical data table
            st.subheader("Historical Alerts")
            
            if self.mock_alert_history:
                history_data = []
                for alert in self.mock_alert_history[:50]:  # Show last 50
                    if alert['timestamp'] >= datetime.now() - timedelta(days=days_back):
                        history_data.append({
                            'Date': alert['timestamp'].strftime('%Y-%m-%d'),
                            'Time': alert['timestamp'].strftime('%H:%M:%S'),
                            'Type': self.alert_types[alert['type']],
                            'Symbol': alert['symbol'],
                            'Severity': alert['severity'].title(),
                            'Status': alert['status'].title(),
                            'Resolution Time': f"{int((alert['resolution_time'] - alert['timestamp']).total_seconds() / 60)}m"
                        })
                
                if history_data:
                    history_df = pd.DataFrame(history_data)
                    st.dataframe(history_df, hide_index=True, use_container_width=True)
                else:
                    st.info(f"No historical alerts found for the last {days_back} days.")
        
        with tab3:
            self.create_alert_configuration_form()
        
        with tab4:
            st.subheader("📊 Alert Analytics")
            
            # Overview charts
            overview_chart = self.create_alerts_overview_chart()
            st.plotly_chart(overview_chart, use_container_width=True)
            
            # Performance metrics
            st.subheader("Alert Performance Metrics")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Alert frequency
                st.markdown("**Alert Frequency (Last 7 Days)**")
                
                daily_counts = pd.Series([
                    np.random.randint(5, 15) for _ in range(7)
                ], index=pd.date_range(end=datetime.now(), periods=7, freq='D'))
                
                fig = px.bar(
                    x=daily_counts.index,
                    y=daily_counts.values,
                    title="Daily Alert Count"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Response efficiency
                st.markdown("**Response Efficiency**")
                
                efficiency_metrics = pd.DataFrame({
                    'Metric': ['Avg Response Time', 'Alert Accuracy', 'False Positive Rate', 'Resolution Rate'],
                    'Value': ['15 minutes', '94.2%', '3.1%', '98.7%'],
                    'Target': ['< 20 minutes', '> 90%', '< 5%', '> 95%'],
                    'Status': ['✅', '✅', '✅', '✅']
                })
                
                st.dataframe(efficiency_metrics, hide_index=True, use_container_width=True)
        
        with tab5:
            st.subheader("⚙️ Alert Settings")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Global Settings**")
                
                global_enabled = st.checkbox("Enable all alerts", value=True)
                quiet_hours_enabled = st.checkbox("Enable quiet hours")
                
                if quiet_hours_enabled:
                    start_time = st.time_input("Quiet hours start", value=datetime.strptime("22:00", "%H:%M").time())
                    end_time = st.time_input("Quiet hours end", value=datetime.strptime("08:00", "%H:%M").time())
                
                batch_notifications = st.checkbox("Batch notifications", value=True)
                max_alerts_per_hour = st.slider("Max alerts per hour", 1, 100, 20)
            
            with col2:
                st.markdown("**Notification Channels**")
                
                email_settings = {
                    'enabled': st.checkbox("Email notifications", value=True),
                    'address': st.text_input("Email address", value="admin@quantstream.ai"),
                    'severity': st.multiselect(
                        "Email severity levels",
                        options=['low', 'medium', 'high', 'critical'],
                        default=['high', 'critical']
                    )
                }
                
                slack_settings = {
                    'enabled': st.checkbox("Slack notifications"),
                    'webhook': st.text_input("Slack webhook URL"),
                    'channel': st.text_input("Slack channel", value="#alerts")
                }
                
                webhook_settings = {
                    'enabled': st.checkbox("Custom webhook"),
                    'url': st.text_input("Webhook URL"),
                    'headers': st.text_area("Custom headers (JSON)")
                }
            
            # Save settings
            if st.button("💾 Save Settings"):
                st.success("Alert settings saved successfully!")
        
        # Real-time alert status
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Alert System Status**")
            st.success("🟢 Online")
        
        with col2:
            st.markdown("**Last Alert**")
            if self.mock_active_alerts:
                last_alert = max(self.mock_active_alerts, key=lambda x: x['timestamp'])
                st.info(f"{last_alert['timestamp'].strftime('%H:%M:%S')} - {last_alert['symbol']}")
        
        with col3:
            st.markdown("**Next Check**")
            next_check = datetime.now() + timedelta(seconds=30)
            st.info(f"{next_check.strftime('%H:%M:%S')}")