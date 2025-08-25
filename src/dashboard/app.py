"""
QuantStream Analytics Dashboard - Main Streamlit Application

This is the main entry point for the QuantStream real-time analytics dashboard.
It provides live market data visualization, technical indicators, anomaly alerts,
and portfolio performance tracking.
"""

import streamlit as st
import asyncio
import threading
from datetime import datetime, timedelta
import sys
import os

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

# Import dashboard components
from src.dashboard.frontend.components.sidebar import create_sidebar
from src.dashboard.frontend.components.market_data import MarketDataComponent
from src.dashboard.frontend.components.technical_indicators import TechnicalIndicatorsComponent
from src.dashboard.frontend.components.portfolio import PortfolioComponent
from src.dashboard.frontend.components.alerts import AlertsComponent
from src.dashboard.frontend.components.system_metrics import SystemMetricsComponent
from src.dashboard.frontend.utils.config import load_config
from src.dashboard.frontend.utils.auth import check_authentication
from src.dashboard.frontend.utils.metrics import track_user_activity

# Page configuration
st.set_page_config(
    page_title="QuantStream Analytics",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://quantstream.ai/help',
        'Report a bug': 'https://quantstream.ai/bug-report',
        'About': "# QuantStream Analytics Dashboard\n\nReal-time financial data analysis and monitoring platform."
    }
)

# Load configuration
config = load_config()

def initialize_session_state():
    """Initialize session state variables"""
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    
    if 'user_id' not in st.session_state:
        st.session_state.user_id = None
    
    if 'last_refresh' not in st.session_state:
        st.session_state.last_refresh = datetime.now()
    
    if 'selected_symbols' not in st.session_state:
        st.session_state.selected_symbols = ['AAPL', 'GOOGL', 'MSFT']
    
    if 'time_range' not in st.session_state:
        st.session_state.time_range = '1D'
    
    if 'dashboard_layout' not in st.session_state:
        st.session_state.dashboard_layout = 'standard'
    
    if 'auto_refresh' not in st.session_state:
        st.session_state.auto_refresh = True

def main():
    """Main dashboard application"""
    initialize_session_state()
    
    # Authentication check
    if not check_authentication():
        st.stop()
    
    # Track user activity for monitoring
    track_user_activity()
    
    # Create sidebar
    create_sidebar()
    
    # Main dashboard header
    st.title("🚀 QuantStream Analytics Dashboard")
    st.markdown("### Real-time Financial Data Analysis & Monitoring")
    
    # Display last refresh time
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.markdown(f"**Last Updated:** {st.session_state.last_refresh.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    with col2:
        if st.button("🔄 Refresh Now"):
            st.session_state.last_refresh = datetime.now()
            st.rerun()
    with col3:
        auto_refresh = st.checkbox("Auto Refresh", value=st.session_state.auto_refresh)
        if auto_refresh != st.session_state.auto_refresh:
            st.session_state.auto_refresh = auto_refresh
    
    # Main dashboard tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Market Data", 
        "📈 Technical Analysis", 
        "💼 Portfolio", 
        "🚨 Alerts", 
        "🖥️ System Metrics"
    ])
    
    with tab1:
        st.header("Live Market Data")
        market_component = MarketDataComponent()
        market_component.render()
    
    with tab2:
        st.header("Technical Indicators")
        indicators_component = TechnicalIndicatorsComponent()
        indicators_component.render()
    
    with tab3:
        st.header("Portfolio Performance")
        portfolio_component = PortfolioComponent()
        portfolio_component.render()
    
    with tab4:
        st.header("Alerts & Notifications")
        alerts_component = AlertsComponent()
        alerts_component.render()
    
    with tab5:
        st.header("System Metrics")
        metrics_component = SystemMetricsComponent()
        metrics_component.render()
    
    # Auto-refresh functionality
    if st.session_state.auto_refresh:
        refresh_interval = config.get('dashboard', {}).get('refresh_rate', 1000)
        
        # Create a placeholder for auto-refresh timer
        with st.empty():
            st.markdown(f"⏱️ Next auto-refresh in {refresh_interval/1000:.0f} seconds")
        
        # Auto-refresh using st.rerun() (new Streamlit approach)
        import time
        time.sleep(refresh_interval / 1000)
        st.rerun()

# Footer
def render_footer():
    """Render dashboard footer"""
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**QuantStream Analytics Platform**")
        st.markdown("Version 1.0.0")
    
    with col2:
        st.markdown("**System Status**")
        # This will be populated with actual system status
        st.markdown("🟢 All Systems Operational")
    
    with col3:
        st.markdown("**Support**")
        st.markdown("[Documentation](https://quantstream.ai/docs) | [Support](https://quantstream.ai/support)")

if __name__ == "__main__":
    try:
        main()
        render_footer()
    except Exception as e:
        st.error(f"Dashboard Error: {str(e)}")
        st.markdown("Please contact system administrator if this issue persists.")
        
        # Log error for monitoring
        import logging
        logging.error(f"Dashboard error: {str(e)}", exc_info=True)