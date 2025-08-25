"""
Dashboard Sidebar Component

Provides navigation, settings, and quick controls for the dashboard.
"""

import streamlit as st
from datetime import datetime, timedelta
from typing import List, Dict, Any

def create_sidebar() -> None:
    """Create and render the main dashboard sidebar"""
    
    with st.sidebar:
        # Dashboard branding
        st.image("https://via.placeholder.com/200x80/1f77b4/white?text=QuantStream", width=200)
        st.markdown("---")
        
        # User info section
        if st.session_state.get('authenticated', False):
            st.markdown("### 👤 User Info")
            st.markdown(f"**User:** {st.session_state.get('user_id', 'Guest')}")
            st.markdown(f"**Role:** Admin")
            st.markdown(f"**Session:** {datetime.now().strftime('%H:%M:%S')}")
            
            if st.button("🚪 Logout", key="logout_btn"):
                st.session_state.authenticated = False
                st.session_state.user_id = None
                st.rerun()
        
        st.markdown("---")
        
        # Symbol selection
        st.markdown("### 📊 Market Symbols")
        available_symbols = [
            'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 
            'META', 'NFLX', 'NVDA', 'AMD', 'INTC'
        ]
        
        selected_symbols = st.multiselect(
            "Select symbols to monitor:",
            options=available_symbols,
            default=st.session_state.get('selected_symbols', ['AAPL', 'GOOGL', 'MSFT']),
            key="symbol_selector"
        )
        
        if selected_symbols != st.session_state.get('selected_symbols', []):
            st.session_state.selected_symbols = selected_symbols
        
        st.markdown("---")
        
        # Time range selection
        st.markdown("### ⏱️ Time Range")
        time_ranges = {
            '5M': '5 Minutes',
            '15M': '15 Minutes', 
            '1H': '1 Hour',
            '4H': '4 Hours',
            '1D': '1 Day',
            '1W': '1 Week',
            '1M': '1 Month'
        }
        
        selected_time_range = st.selectbox(
            "Chart time range:",
            options=list(time_ranges.keys()),
            format_func=lambda x: time_ranges[x],
            index=list(time_ranges.keys()).index(st.session_state.get('time_range', '1D')),
            key="time_range_selector"
        )
        
        if selected_time_range != st.session_state.get('time_range'):
            st.session_state.time_range = selected_time_range
        
        st.markdown("---")
        
        # Dashboard layout options
        st.markdown("### 🖥️ Layout")
        layout_options = {
            'standard': 'Standard View',
            'compact': 'Compact View',
            'detailed': 'Detailed View',
            'mobile': 'Mobile Optimized'
        }
        
        selected_layout = st.selectbox(
            "Dashboard layout:",
            options=list(layout_options.keys()),
            format_func=lambda x: layout_options[x],
            index=list(layout_options.keys()).index(st.session_state.get('dashboard_layout', 'standard')),
            key="layout_selector"
        )
        
        if selected_layout != st.session_state.get('dashboard_layout'):
            st.session_state.dashboard_layout = selected_layout
        
        st.markdown("---")
        
        # Technical indicators settings
        st.markdown("### 📈 Indicators")
        
        # Bollinger Bands settings
        with st.expander("Bollinger Bands"):
            bb_enabled = st.checkbox("Enable Bollinger Bands", value=True, key="bb_enabled")
            bb_period = st.slider("Period", 10, 50, 20, key="bb_period")
            bb_std = st.slider("Standard Deviation", 1.0, 3.0, 2.0, step=0.1, key="bb_std")
        
        # RSI settings
        with st.expander("RSI"):
            rsi_enabled = st.checkbox("Enable RSI", value=True, key="rsi_enabled")
            rsi_period = st.slider("Period", 5, 30, 14, key="rsi_period")
            rsi_overbought = st.slider("Overbought Level", 60, 90, 70, key="rsi_overbought")
            rsi_oversold = st.slider("Oversold Level", 10, 40, 30, key="rsi_oversold")
        
        # MACD settings
        with st.expander("MACD"):
            macd_enabled = st.checkbox("Enable MACD", value=True, key="macd_enabled")
            macd_fast = st.slider("Fast Period", 5, 20, 12, key="macd_fast")
            macd_slow = st.slider("Slow Period", 20, 35, 26, key="macd_slow")
            macd_signal = st.slider("Signal Period", 5, 15, 9, key="macd_signal")
        
        st.markdown("---")
        
        # Alert settings
        st.markdown("### 🚨 Alert Settings")
        
        alert_settings = {
            'price_alerts': st.checkbox("Price Alerts", value=True, key="price_alerts"),
            'volume_alerts': st.checkbox("Volume Alerts", value=True, key="volume_alerts"),
            'technical_alerts': st.checkbox("Technical Indicator Alerts", value=True, key="tech_alerts"),
            'anomaly_alerts': st.checkbox("Anomaly Detection", value=True, key="anomaly_alerts")
        }
        
        # Alert channels
        st.markdown("**Notification Channels:**")
        notification_channels = {
            'email': st.checkbox("Email", value=True, key="email_notifications"),
            'slack': st.checkbox("Slack", value=False, key="slack_notifications"),
            'webhook': st.checkbox("Webhook", value=False, key="webhook_notifications")
        }
        
        st.markdown("---")
        
        # System status
        st.markdown("### ⚡ System Status")
        
        # Mock system status - in production this would be real data
        status_metrics = {
            "API": "🟢 Online",
            "WebSocket": "🟢 Connected", 
            "Database": "🟢 Healthy",
            "Cache": "🟢 Active",
            "Alerts": "🟢 Active"
        }
        
        for service, status in status_metrics.items():
            st.markdown(f"**{service}:** {status}")
        
        # Performance metrics
        st.markdown("**Performance:**")
        st.metric("Latency", "45ms", "-5ms")
        st.metric("Uptime", "99.98%", "+0.02%")
        
        st.markdown("---")
        
        # Quick actions
        st.markdown("### ⚡ Quick Actions")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📥 Export Data", key="export_data"):
                st.success("Export started...")
        
        with col2:
            if st.button("🔄 Reset View", key="reset_view"):
                # Reset to defaults
                st.session_state.selected_symbols = ['AAPL', 'GOOGL', 'MSFT']
                st.session_state.time_range = '1D'
                st.session_state.dashboard_layout = 'standard'
                st.success("View reset!")
                st.rerun()
        
        # Help and documentation
        st.markdown("---")
        st.markdown("### 📚 Help & Support")
        
        if st.button("📖 Documentation", key="docs"):
            st.info("Opening documentation...")
        
        if st.button("🐛 Report Issue", key="report"):
            st.info("Opening issue tracker...")
        
        if st.button("💬 Get Help", key="help"):
            st.info("Opening support chat...")
        
        # Version info
        st.markdown("---")
        st.markdown("**Version:** 1.0.0")
        st.markdown("**Build:** 2024.01.23")
        st.markdown("**© 2024 QuantStream Analytics**")