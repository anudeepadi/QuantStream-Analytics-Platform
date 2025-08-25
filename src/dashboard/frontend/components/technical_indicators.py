"""
Technical Indicators Component

Advanced technical analysis with Bollinger Bands, RSI, MACD, and other indicators.
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import yfinance as yf
from typing import Dict, List, Tuple, Optional
import talib

class TechnicalIndicatorsComponent:
    """Component for technical analysis and indicators"""
    
    def __init__(self):
        self.cache_duration = 60
    
    def calculate_bollinger_bands(self, data: pd.DataFrame, period: int = 20, std_dev: float = 2.0) -> Dict[str, pd.Series]:
        """Calculate Bollinger Bands"""
        if len(data) < period:
            # Return empty series if not enough data
            return {
                'upper': pd.Series([np.nan] * len(data)),
                'middle': pd.Series([np.nan] * len(data)),
                'lower': pd.Series([np.nan] * len(data))
            }
        
        rolling_mean = data['Close'].rolling(window=period).mean()
        rolling_std = data['Close'].rolling(window=period).std()
        
        upper_band = rolling_mean + (rolling_std * std_dev)
        lower_band = rolling_mean - (rolling_std * std_dev)
        
        return {
            'upper': upper_band,
            'middle': rolling_mean,
            'lower': lower_band
        }
    
    def calculate_rsi(self, data: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Relative Strength Index"""
        if len(data) < period + 1:
            return pd.Series([np.nan] * len(data))
        
        delta = data['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def calculate_macd(self, data: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> Dict[str, pd.Series]:
        """Calculate MACD (Moving Average Convergence Divergence)"""
        if len(data) < slow:
            return {
                'macd': pd.Series([np.nan] * len(data)),
                'signal': pd.Series([np.nan] * len(data)),
                'histogram': pd.Series([np.nan] * len(data))
            }
        
        ema_fast = data['Close'].ewm(span=fast).mean()
        ema_slow = data['Close'].ewm(span=slow).mean()
        
        macd = ema_fast - ema_slow
        signal_line = macd.ewm(span=signal).mean()
        histogram = macd - signal_line
        
        return {
            'macd': macd,
            'signal': signal_line,
            'histogram': histogram
        }
    
    def calculate_moving_averages(self, data: pd.DataFrame) -> Dict[str, pd.Series]:
        """Calculate various moving averages"""
        return {
            'SMA_20': data['Close'].rolling(window=20).mean(),
            'SMA_50': data['Close'].rolling(window=50).mean(),
            'EMA_20': data['Close'].ewm(span=20).mean(),
            'EMA_50': data['Close'].ewm(span=50).mean()
        }
    
    def calculate_stochastic(self, data: pd.DataFrame, k_period: int = 14, d_period: int = 3) -> Dict[str, pd.Series]:
        """Calculate Stochastic Oscillator"""
        if len(data) < k_period:
            return {
                'k': pd.Series([np.nan] * len(data)),
                'd': pd.Series([np.nan] * len(data))
            }
        
        low_min = data['Low'].rolling(window=k_period).min()
        high_max = data['High'].rolling(window=k_period).max()
        
        k_percent = 100 * ((data['Close'] - low_min) / (high_max - low_min))
        d_percent = k_percent.rolling(window=d_period).mean()
        
        return {
            'k': k_percent,
            'd': d_percent
        }
    
    def detect_patterns(self, data: pd.DataFrame) -> Dict[str, List[int]]:
        """Detect common technical patterns"""
        patterns = {
            'support_resistance': [],
            'breakouts': [],
            'divergences': []
        }
        
        if len(data) < 50:
            return patterns
        
        # Simple pattern detection (can be enhanced with more sophisticated algorithms)
        
        # Support and resistance levels
        highs = data['High'].rolling(window=20).max()
        lows = data['Low'].rolling(window=20).min()
        
        for i in range(20, len(data) - 20):
            if data['High'].iloc[i] == highs.iloc[i]:
                patterns['support_resistance'].append(i)
            if data['Low'].iloc[i] == lows.iloc[i]:
                patterns['support_resistance'].append(i)
        
        # Breakout detection
        bb_data = self.calculate_bollinger_bands(data)
        for i in range(len(data)):
            if not pd.isna(bb_data['upper'].iloc[i]):
                if data['Close'].iloc[i] > bb_data['upper'].iloc[i]:
                    patterns['breakouts'].append(i)
                elif data['Close'].iloc[i] < bb_data['lower'].iloc[i]:
                    patterns['breakouts'].append(i)
        
        return patterns
    
    def create_indicators_chart(self, data: pd.DataFrame, symbol: str, indicators: Dict[str, bool]) -> go.Figure:
        """Create comprehensive technical indicators chart"""
        
        # Create subplots
        subplot_count = 1 + sum([
            indicators.get('rsi_enabled', False),
            indicators.get('macd_enabled', False)
        ])
        
        row_heights = [0.6] + [0.2] * (subplot_count - 1)
        
        fig = make_subplots(
            rows=subplot_count,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            row_heights=row_heights,
            subplot_titles=[f'{symbol} Technical Analysis'] + ['RSI'] * indicators.get('rsi_enabled', False) + ['MACD'] * indicators.get('macd_enabled', False)
        )
        
        # Main price chart with candlesticks
        candlestick = go.Candlestick(
            x=data['Datetime'],
            open=data['Open'],
            high=data['High'],
            low=data['Low'],
            close=data['Close'],
            name=symbol,
            increasing_line_color='#00d4aa',
            decreasing_line_color='#ff6b6b'
        )
        fig.add_trace(candlestick, row=1, col=1)
        
        # Add Bollinger Bands
        if indicators.get('bb_enabled', False):
            bb_period = st.session_state.get('bb_period', 20)
            bb_std = st.session_state.get('bb_std', 2.0)
            bb_data = self.calculate_bollinger_bands(data, bb_period, bb_std)
            
            fig.add_trace(go.Scatter(
                x=data['Datetime'],
                y=bb_data['upper'],
                mode='lines',
                name='BB Upper',
                line=dict(color='rgba(128,128,128,0.5)', width=1)
            ), row=1, col=1)
            
            fig.add_trace(go.Scatter(
                x=data['Datetime'],
                y=bb_data['middle'],
                mode='lines',
                name='BB Middle',
                line=dict(color='orange', width=1)
            ), row=1, col=1)
            
            fig.add_trace(go.Scatter(
                x=data['Datetime'],
                y=bb_data['lower'],
                mode='lines',
                name='BB Lower',
                line=dict(color='rgba(128,128,128,0.5)', width=1),
                fill='tonexty',
                fillcolor='rgba(128,128,128,0.1)'
            ), row=1, col=1)
        
        # Add Moving Averages
        ma_data = self.calculate_moving_averages(data)
        fig.add_trace(go.Scatter(
            x=data['Datetime'],
            y=ma_data['SMA_20'],
            mode='lines',
            name='SMA 20',
            line=dict(color='blue', width=1)
        ), row=1, col=1)
        
        fig.add_trace(go.Scatter(
            x=data['Datetime'],
            y=ma_data['SMA_50'],
            mode='lines',
            name='SMA 50',
            line=dict(color='red', width=1)
        ), row=1, col=1)
        
        current_row = 2
        
        # RSI subplot
        if indicators.get('rsi_enabled', False):
            rsi_period = st.session_state.get('rsi_period', 14)
            rsi_data = self.calculate_rsi(data, rsi_period)
            
            fig.add_trace(go.Scatter(
                x=data['Datetime'],
                y=rsi_data,
                mode='lines',
                name='RSI',
                line=dict(color='purple', width=2)
            ), row=current_row, col=1)
            
            # Add RSI levels
            overbought = st.session_state.get('rsi_overbought', 70)
            oversold = st.session_state.get('rsi_oversold', 30)
            
            fig.add_hline(y=overbought, line_dash="dash", line_color="red", row=current_row, col=1)
            fig.add_hline(y=oversold, line_dash="dash", line_color="green", row=current_row, col=1)
            fig.add_hline(y=50, line_dash="dot", line_color="gray", row=current_row, col=1)
            
            fig.update_yaxes(range=[0, 100], title_text="RSI", row=current_row, col=1)
            current_row += 1
        
        # MACD subplot
        if indicators.get('macd_enabled', False):
            macd_fast = st.session_state.get('macd_fast', 12)
            macd_slow = st.session_state.get('macd_slow', 26)
            macd_signal = st.session_state.get('macd_signal', 9)
            macd_data = self.calculate_macd(data, macd_fast, macd_slow, macd_signal)
            
            fig.add_trace(go.Scatter(
                x=data['Datetime'],
                y=macd_data['macd'],
                mode='lines',
                name='MACD',
                line=dict(color='blue', width=2)
            ), row=current_row, col=1)
            
            fig.add_trace(go.Scatter(
                x=data['Datetime'],
                y=macd_data['signal'],
                mode='lines',
                name='Signal',
                line=dict(color='red', width=2)
            ), row=current_row, col=1)
            
            # MACD histogram
            colors = ['green' if h >= 0 else 'red' for h in macd_data['histogram']]
            fig.add_trace(go.Bar(
                x=data['Datetime'],
                y=macd_data['histogram'],
                name='Histogram',
                marker_color=colors,
                opacity=0.6
            ), row=current_row, col=1)
            
            fig.update_yaxes(title_text="MACD", row=current_row, col=1)
        
        # Update layout
        fig.update_layout(
            title=f'{symbol} - Technical Analysis Dashboard',
            template='plotly_white',
            height=800,
            showlegend=True,
            xaxis_rangeslider_visible=False
        )
        
        return fig
    
    def create_indicators_summary(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Create summary table of technical indicators"""
        if data.empty:
            return pd.DataFrame()
        
        latest = data.iloc[-1]
        
        # Calculate all indicators
        bb_data = self.calculate_bollinger_bands(data)
        rsi_data = self.calculate_rsi(data)
        macd_data = self.calculate_macd(data)
        ma_data = self.calculate_moving_averages(data)
        stoch_data = self.calculate_stochastic(data)
        
        # Create summary
        indicators_summary = []
        
        # Bollinger Bands
        if not pd.isna(bb_data['upper'].iloc[-1]):
            bb_position = "Above Upper" if latest['Close'] > bb_data['upper'].iloc[-1] else \
                         "Below Lower" if latest['Close'] < bb_data['lower'].iloc[-1] else \
                         "Inside Bands"
            indicators_summary.append({
                'Indicator': 'Bollinger Bands',
                'Value': f"{bb_position}",
                'Signal': '🔴 Overbought' if bb_position == 'Above Upper' else \
                         '🟢 Oversold' if bb_position == 'Below Lower' else \
                         '🟡 Neutral'
            })
        
        # RSI
        if not pd.isna(rsi_data.iloc[-1]):
            rsi_value = rsi_data.iloc[-1]
            rsi_signal = '🔴 Overbought' if rsi_value > 70 else \
                        '🟢 Oversold' if rsi_value < 30 else \
                        '🟡 Neutral'
            indicators_summary.append({
                'Indicator': 'RSI (14)',
                'Value': f"{rsi_value:.2f}",
                'Signal': rsi_signal
            })
        
        # MACD
        if not pd.isna(macd_data['macd'].iloc[-1]):
            macd_value = macd_data['macd'].iloc[-1]
            signal_value = macd_data['signal'].iloc[-1]
            macd_signal = '🟢 Bullish' if macd_value > signal_value else '🔴 Bearish'
            indicators_summary.append({
                'Indicator': 'MACD',
                'Value': f"{macd_value:.4f}",
                'Signal': macd_signal
            })
        
        # Moving Averages
        if not pd.isna(ma_data['SMA_20'].iloc[-1]):
            sma20_signal = '🟢 Above' if latest['Close'] > ma_data['SMA_20'].iloc[-1] else '🔴 Below'
            indicators_summary.append({
                'Indicator': 'SMA 20',
                'Value': f"${ma_data['SMA_20'].iloc[-1]:.2f}",
                'Signal': sma20_signal
            })
        
        if not pd.isna(ma_data['SMA_50'].iloc[-1]):
            sma50_signal = '🟢 Above' if latest['Close'] > ma_data['SMA_50'].iloc[-1] else '🔴 Below'
            indicators_summary.append({
                'Indicator': 'SMA 50',
                'Value': f"${ma_data['SMA_50'].iloc[-1]:.2f}",
                'Signal': sma50_signal
            })
        
        # Stochastic
        if not pd.isna(stoch_data['k'].iloc[-1]):
            stoch_k = stoch_data['k'].iloc[-1]
            stoch_signal = '🔴 Overbought' if stoch_k > 80 else \
                          '🟢 Oversold' if stoch_k < 20 else \
                          '🟡 Neutral'
            indicators_summary.append({
                'Indicator': 'Stochastic %K',
                'Value': f"{stoch_k:.2f}",
                'Signal': stoch_signal
            })
        
        return pd.DataFrame(indicators_summary)
    
    def render(self):
        """Render the technical indicators component"""
        
        selected_symbols = st.session_state.get('selected_symbols', ['AAPL'])
        time_range = st.session_state.get('time_range', '1D')
        
        if not selected_symbols:
            st.warning("Please select symbols from the sidebar to view technical analysis.")
            return
        
        # Symbol selector
        selected_symbol = st.selectbox(
            "Select symbol for technical analysis:",
            options=selected_symbols,
            key="tech_analysis_symbol"
        )
        
        if selected_symbol:
            # Get indicator settings from session state
            indicators = {
                'bb_enabled': st.session_state.get('bb_enabled', True),
                'rsi_enabled': st.session_state.get('rsi_enabled', True),
                'macd_enabled': st.session_state.get('macd_enabled', True)
            }
            
            # Fetch market data (reusing from market data component)
            from src.dashboard.frontend.components.market_data import MarketDataComponent
            market_component = MarketDataComponent()
            
            with st.spinner(f"Analyzing {selected_symbol}..."):
                market_data = market_component.fetch_market_data(selected_symbol, time_range)
                
                if not market_data.empty:
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        # Technical indicators chart
                        chart = self.create_indicators_chart(market_data, selected_symbol, indicators)
                        st.plotly_chart(chart, use_container_width=True)
                    
                    with col2:
                        # Indicators summary
                        st.subheader("Indicators Summary")
                        summary_df = self.create_indicators_summary(market_data, selected_symbol)
                        
                        if not summary_df.empty:
                            st.dataframe(summary_df, hide_index=True, use_container_width=True)
                        
                        # Pattern detection
                        st.subheader("Pattern Detection")
                        patterns = self.detect_patterns(market_data)
                        
                        st.metric("Support/Resistance Levels", len(patterns['support_resistance']))
                        st.metric("Breakout Signals", len(patterns['breakouts']))
                        st.metric("Divergences", len(patterns['divergences']))
                        
                        # Trading signals
                        st.subheader("Trading Signals")
                        
                        # Calculate overall sentiment
                        bullish_signals = 0
                        bearish_signals = 0
                        total_signals = 0
                        
                        if not summary_df.empty:
                            for _, row in summary_df.iterrows():
                                if '🟢' in row['Signal']:
                                    bullish_signals += 1
                                elif '🔴' in row['Signal']:
                                    bearish_signals += 1
                                total_signals += 1
                        
                        if total_signals > 0:
                            bullish_pct = (bullish_signals / total_signals) * 100
                            
                            if bullish_pct > 60:
                                st.success("🟢 Overall: BULLISH")
                            elif bullish_pct < 40:
                                st.error("🔴 Overall: BEARISH")
                            else:
                                st.warning("🟡 Overall: NEUTRAL")
                        
                        # Risk metrics
                        st.subheader("Risk Metrics")
                        
                        if len(market_data) > 20:
                            volatility = market_data['Close'].pct_change().std() * np.sqrt(252) * 100
                            st.metric("Volatility (Annualized)", f"{volatility:.2f}%")
                        
                        # Volume analysis
                        avg_volume = market_data['Volume'].mean()
                        latest_volume = market_data['Volume'].iloc[-1]
                        volume_ratio = latest_volume / avg_volume if avg_volume > 0 else 1
                        
                        st.metric(
                            "Volume vs Average",
                            f"{volume_ratio:.2f}x",
                            delta="High" if volume_ratio > 1.5 else "Normal" if volume_ratio > 0.5 else "Low"
                        )
                
                else:
                    st.error(f"No data available for {selected_symbol}")
        
        # Technical analysis tools
        st.markdown("---")
        st.subheader("🛠️ Analysis Tools")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🔍 Pattern Scanner"):
                st.info("Scanning for technical patterns...")
                # This would trigger advanced pattern detection
        
        with col2:
            if st.button("📊 Correlation Analysis"):
                st.info("Analyzing correlations...")
                # This would show correlation with other symbols/indices
        
        with col3:
            if st.button("⚡ Real-time Alerts"):
                st.info("Setting up alerts...")
                # This would configure real-time technical alerts