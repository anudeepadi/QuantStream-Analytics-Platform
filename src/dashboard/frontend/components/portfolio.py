"""
Portfolio Component

Portfolio performance tracking, P&L analysis, and position management.
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import yfinance as yf

class PortfolioComponent:
    """Component for portfolio tracking and analysis"""
    
    def __init__(self):
        self.cache_duration = 300  # 5 minutes for portfolio data
        self.initialize_mock_portfolio()
    
    def initialize_mock_portfolio(self):
        """Initialize mock portfolio data for demonstration"""
        # In production, this would connect to a real portfolio management system
        self.mock_positions = {
            'AAPL': {'shares': 100, 'avg_cost': 150.00, 'entry_date': '2024-01-15'},
            'GOOGL': {'shares': 50, 'avg_cost': 2800.00, 'entry_date': '2024-01-20'},
            'MSFT': {'shares': 75, 'avg_cost': 380.00, 'entry_date': '2024-01-10'},
            'AMZN': {'shares': 30, 'avg_cost': 3200.00, 'entry_date': '2024-02-01'},
            'TSLA': {'shares': 25, 'avg_cost': 220.00, 'entry_date': '2024-01-25'}
        }
        
        self.mock_cash_balance = 25000.00
        self.mock_initial_capital = 200000.00
    
    @st.cache_data(ttl=300)
    def get_current_prices(_self, symbols: List[str]) -> Dict[str, float]:
        """Get current prices for portfolio symbols"""
        prices = {}
        
        for symbol in symbols:
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period="1d", interval="1m")
                
                if not hist.empty:
                    prices[symbol] = hist['Close'].iloc[-1]
                else:
                    # Generate mock price if data unavailable
                    base_prices = {'AAPL': 180, 'GOOGL': 2900, 'MSFT': 420, 'AMZN': 3400, 'TSLA': 200}
                    base_price = base_prices.get(symbol, 100)
                    # Add some random variation
                    prices[symbol] = base_price * np.random.uniform(0.95, 1.05)
                    
            except Exception:
                # Fallback prices
                base_prices = {'AAPL': 180, 'GOOGL': 2900, 'MSFT': 420, 'AMZN': 3400, 'TSLA': 200}
                prices[symbol] = base_prices.get(symbol, 100)
        
        return prices
    
    def calculate_portfolio_metrics(self) -> Dict[str, any]:
        """Calculate comprehensive portfolio metrics"""
        symbols = list(self.mock_positions.keys())
        current_prices = self.get_current_prices(symbols)
        
        positions_data = []
        total_market_value = 0
        total_cost_basis = 0
        total_unrealized_pnl = 0
        
        for symbol, position in self.mock_positions.items():
            current_price = current_prices.get(symbol, position['avg_cost'])
            shares = position['shares']
            avg_cost = position['avg_cost']
            
            market_value = shares * current_price
            cost_basis = shares * avg_cost
            unrealized_pnl = market_value - cost_basis
            unrealized_pnl_pct = (unrealized_pnl / cost_basis) * 100 if cost_basis > 0 else 0
            
            positions_data.append({
                'Symbol': symbol,
                'Shares': shares,
                'Avg Cost': avg_cost,
                'Current Price': current_price,
                'Market Value': market_value,
                'Cost Basis': cost_basis,
                'Unrealized P&L': unrealized_pnl,
                'Unrealized P&L %': unrealized_pnl_pct,
                'Entry Date': position['entry_date']
            })
            
            total_market_value += market_value
            total_cost_basis += cost_basis
            total_unrealized_pnl += unrealized_pnl
        
        portfolio_value = total_market_value + self.mock_cash_balance
        total_return = portfolio_value - self.mock_initial_capital
        total_return_pct = (total_return / self.mock_initial_capital) * 100
        
        return {
            'positions': pd.DataFrame(positions_data),
            'portfolio_value': portfolio_value,
            'cash_balance': self.mock_cash_balance,
            'total_market_value': total_market_value,
            'total_cost_basis': total_cost_basis,
            'total_unrealized_pnl': total_unrealized_pnl,
            'total_return': total_return,
            'total_return_pct': total_return_pct,
            'initial_capital': self.mock_initial_capital
        }
    
    def generate_portfolio_history(self, days: int = 30) -> pd.DataFrame:
        """Generate mock portfolio history for demonstration"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # Generate realistic portfolio value progression
        initial_value = self.mock_initial_capital
        daily_returns = np.random.normal(0.001, 0.02, len(dates))  # ~0.1% daily return with 2% volatility
        
        portfolio_values = []
        current_value = initial_value
        
        for daily_return in daily_returns:
            current_value *= (1 + daily_return)
            portfolio_values.append(current_value)
        
        # Generate component breakdown
        cash_values = [self.mock_cash_balance] * len(dates)
        equity_values = [pv - self.mock_cash_balance for pv in portfolio_values]
        
        return pd.DataFrame({
            'Date': dates,
            'Portfolio Value': portfolio_values,
            'Equity Value': equity_values,
            'Cash Value': cash_values,
            'Daily Return': [0] + [portfolio_values[i] / portfolio_values[i-1] - 1 for i in range(1, len(portfolio_values))]
        })
    
    def create_portfolio_overview_chart(self, metrics: Dict[str, any]) -> go.Figure:
        """Create portfolio overview chart"""
        
        # Pie chart for allocation
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Portfolio Allocation', 'P&L by Position', 'Asset Distribution', 'Performance Metrics'),
            specs=[[{"type": "pie"}, {"type": "bar"}], 
                   [{"type": "pie"}, {"type": "scatter"}]]
        )
        
        positions_df = metrics['positions']
        
        # Portfolio allocation pie chart
        fig.add_trace(go.Pie(
            labels=positions_df['Symbol'].tolist() + ['Cash'],
            values=positions_df['Market Value'].tolist() + [metrics['cash_balance']],
            name="Allocation",
            hole=0.3
        ), row=1, col=1)
        
        # P&L bar chart
        colors = ['green' if pnl > 0 else 'red' for pnl in positions_df['Unrealized P&L']]
        fig.add_trace(go.Bar(
            x=positions_df['Symbol'],
            y=positions_df['Unrealized P&L'],
            marker_color=colors,
            name="P&L"
        ), row=1, col=2)
        
        # Asset type distribution
        # For simplicity, categorize all as "Equities"
        fig.add_trace(go.Pie(
            labels=['Equities', 'Cash'],
            values=[metrics['total_market_value'], metrics['cash_balance']],
            name="Asset Types"
        ), row=2, col=1)
        
        # Portfolio performance over time
        history_df = self.generate_portfolio_history()
        fig.add_trace(go.Scatter(
            x=history_df['Date'],
            y=history_df['Portfolio Value'],
            mode='lines',
            name='Portfolio Value',
            line=dict(color='blue', width=2)
        ), row=2, col=2)
        
        fig.update_layout(
            title="Portfolio Overview Dashboard",
            height=800,
            showlegend=True
        )
        
        return fig
    
    def create_performance_chart(self) -> go.Figure:
        """Create detailed performance chart"""
        
        history_df = self.generate_portfolio_history()
        
        fig = make_subplots(
            rows=3, cols=1,
            subplot_titles=('Portfolio Value Over Time', 'Daily Returns', 'Cumulative Returns'),
            vertical_spacing=0.08,
            row_heights=[0.5, 0.25, 0.25]
        )
        
        # Portfolio value
        fig.add_trace(go.Scatter(
            x=history_df['Date'],
            y=history_df['Portfolio Value'],
            mode='lines',
            name='Portfolio Value',
            line=dict(color='blue', width=2),
            fill='tonexty'
        ), row=1, col=1)
        
        # Add benchmark (S&P 500 proxy)
        benchmark_returns = np.random.normal(0.0005, 0.015, len(history_df))
        benchmark_value = self.mock_initial_capital * np.cumprod(1 + benchmark_returns)
        
        fig.add_trace(go.Scatter(
            x=history_df['Date'],
            y=benchmark_value,
            mode='lines',
            name='S&P 500 (Benchmark)',
            line=dict(color='gray', width=1, dash='dash')
        ), row=1, col=1)
        
        # Daily returns
        colors = ['green' if ret > 0 else 'red' for ret in history_df['Daily Return']]
        fig.add_trace(go.Bar(
            x=history_df['Date'],
            y=history_df['Daily Return'] * 100,
            marker_color=colors,
            name='Daily Returns (%)',
            opacity=0.7
        ), row=2, col=1)
        
        # Cumulative returns
        cumulative_returns = (history_df['Portfolio Value'] / self.mock_initial_capital - 1) * 100
        cumulative_benchmark = (benchmark_value / self.mock_initial_capital - 1) * 100
        
        fig.add_trace(go.Scatter(
            x=history_df['Date'],
            y=cumulative_returns,
            mode='lines',
            name='Portfolio Cumulative Return (%)',
            line=dict(color='blue', width=2)
        ), row=3, col=1)
        
        fig.add_trace(go.Scatter(
            x=history_df['Date'],
            y=cumulative_benchmark,
            mode='lines',
            name='Benchmark Cumulative Return (%)',
            line=dict(color='gray', width=1, dash='dash')
        ), row=3, col=1)
        
        fig.update_layout(
            title="Portfolio Performance Analysis",
            height=900,
            showlegend=True
        )
        
        # Update y-axis labels
        fig.update_yaxes(title_text="Value ($)", row=1, col=1)
        fig.update_yaxes(title_text="Daily Return (%)", row=2, col=1)
        fig.update_yaxes(title_text="Cumulative Return (%)", row=3, col=1)
        
        return fig
    
    def calculate_risk_metrics(self, history_df: pd.DataFrame) -> Dict[str, float]:
        """Calculate portfolio risk metrics"""
        
        daily_returns = history_df['Daily Return'].dropna()
        
        if len(daily_returns) < 2:
            return {}
        
        # Annualized metrics
        annual_return = daily_returns.mean() * 252
        annual_volatility = daily_returns.std() * np.sqrt(252)
        
        # Sharpe ratio (assuming 2% risk-free rate)
        risk_free_rate = 0.02
        sharpe_ratio = (annual_return - risk_free_rate) / annual_volatility if annual_volatility > 0 else 0
        
        # Maximum drawdown
        cumulative_returns = (1 + daily_returns).cumprod()
        rolling_max = cumulative_returns.expanding().max()
        drawdown = (cumulative_returns - rolling_max) / rolling_max
        max_drawdown = drawdown.min()
        
        # Value at Risk (95% confidence)
        var_95 = np.percentile(daily_returns, 5)
        
        # Sortino ratio (using downside deviation)
        downside_returns = daily_returns[daily_returns < 0]
        downside_deviation = downside_returns.std() * np.sqrt(252) if len(downside_returns) > 0 else 0
        sortino_ratio = (annual_return - risk_free_rate) / downside_deviation if downside_deviation > 0 else 0
        
        return {
            'annual_return': annual_return * 100,
            'annual_volatility': annual_volatility * 100,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown * 100,
            'var_95': var_95 * 100,
            'sortino_ratio': sortino_ratio
        }
    
    def render(self):
        """Render the portfolio component"""
        
        st.subheader("💼 Portfolio Overview")
        
        # Calculate portfolio metrics
        with st.spinner("Calculating portfolio metrics..."):
            metrics = self.calculate_portfolio_metrics()
            history_df = self.generate_portfolio_history()
            risk_metrics = self.calculate_risk_metrics(history_df)
        
        # Key portfolio metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Portfolio Value",
                f"${metrics['portfolio_value']:,.2f}",
                f"${metrics['total_return']:,.2f}"
            )
        
        with col2:
            st.metric(
                "Total Return",
                f"{metrics['total_return_pct']:.2f}%",
                "Since Inception"
            )
        
        with col3:
            st.metric(
                "Unrealized P&L",
                f"${metrics['total_unrealized_pnl']:,.2f}",
                f"{(metrics['total_unrealized_pnl']/metrics['total_cost_basis']*100):.2f}%"
            )
        
        with col4:
            st.metric(
                "Cash Balance",
                f"${metrics['cash_balance']:,.2f}",
                f"{(metrics['cash_balance']/metrics['portfolio_value']*100):.1f}% allocation"
            )
        
        # Portfolio tabs
        tab1, tab2, tab3, tab4 = st.tabs(["Holdings", "Performance", "Risk Analysis", "Transactions"])
        
        with tab1:
            st.subheader("📊 Current Holdings")
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Holdings table
                positions_df = metrics['positions'].copy()
                
                # Format currency columns
                for col in ['Avg Cost', 'Current Price', 'Market Value', 'Cost Basis', 'Unrealized P&L']:
                    positions_df[col] = positions_df[col].apply(lambda x: f"${x:,.2f}")
                
                positions_df['Unrealized P&L %'] = positions_df['Unrealized P&L %'].apply(lambda x: f"{x:.2f}%")
                
                st.dataframe(
                    positions_df,
                    use_container_width=True,
                    hide_index=True
                )
            
            with col2:
                # Portfolio composition
                overview_chart = self.create_portfolio_overview_chart(metrics)
                st.plotly_chart(overview_chart, use_container_width=True)
        
        with tab2:
            st.subheader("📈 Performance Analysis")
            
            # Performance chart
            performance_chart = self.create_performance_chart()
            st.plotly_chart(performance_chart, use_container_width=True)
            
            # Performance metrics
            st.subheader("Performance Metrics")
            
            if risk_metrics:
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Annual Return", f"{risk_metrics['annual_return']:.2f}%")
                    st.metric("Sharpe Ratio", f"{risk_metrics['sharpe_ratio']:.2f}")
                
                with col2:
                    st.metric("Annual Volatility", f"{risk_metrics['annual_volatility']:.2f}%")
                    st.metric("Sortino Ratio", f"{risk_metrics['sortino_ratio']:.2f}")
                
                with col3:
                    st.metric("Max Drawdown", f"{risk_metrics['max_drawdown']:.2f}%")
                    st.metric("Value at Risk (95%)", f"{risk_metrics['var_95']:.2f}%")
        
        with tab3:
            st.subheader("⚠️ Risk Analysis")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### Position Concentration")
                
                # Calculate position weights
                total_equity = metrics['total_market_value']
                position_weights = []
                
                for _, position in metrics['positions'].iterrows():
                    weight = (position['Market Value'] / total_equity) * 100
                    position_weights.append({
                        'Symbol': position['Symbol'],
                        'Weight': f"{weight:.1f}%",
                        'Risk Level': 'High' if weight > 25 else 'Medium' if weight > 15 else 'Low'
                    })
                
                weight_df = pd.DataFrame(position_weights)
                st.dataframe(weight_df, hide_index=True, use_container_width=True)
                
                # Risk warnings
                max_weight = max([float(w['Weight'].replace('%', '')) for w in position_weights])
                if max_weight > 30:
                    st.warning("⚠️ High concentration risk detected! Consider diversifying.")
                elif max_weight > 20:
                    st.info("ℹ️ Moderate concentration. Monitor position sizes.")
                else:
                    st.success("✅ Well-diversified portfolio.")
            
            with col2:
                st.markdown("### Risk Metrics Summary")
                
                if risk_metrics:
                    risk_summary = pd.DataFrame({
                        'Metric': [
                            'Portfolio Beta',
                            'Correlation to Market',
                            'Tracking Error',
                            'Information Ratio',
                            'Calmar Ratio'
                        ],
                        'Value': [
                            '1.15',  # Mock values
                            '0.85',
                            '8.2%',
                            '0.45',
                            '1.8'
                        ],
                        'Benchmark': [
                            '1.00',
                            '1.00',
                            '0.0%',
                            '0.00',
                            'N/A'
                        ]
                    })
                    
                    st.dataframe(risk_summary, hide_index=True, use_container_width=True)
                
                # Risk score
                st.markdown("### Risk Score")
                risk_score = np.random.randint(3, 8)  # Mock risk score
                risk_level = "Low" if risk_score <= 3 else "Medium" if risk_score <= 6 else "High"
                risk_color = "green" if risk_level == "Low" else "orange" if risk_level == "Medium" else "red"
                
                st.markdown(f"**Risk Level:** <span style='color:{risk_color}'>{risk_level}</span> ({risk_score}/10)", unsafe_allow_html=True)
                st.progress(risk_score / 10)
        
        with tab4:
            st.subheader("💱 Recent Transactions")
            
            # Mock transaction history
            transactions = pd.DataFrame({
                'Date': ['2024-01-25', '2024-01-20', '2024-01-15', '2024-01-10'],
                'Symbol': ['TSLA', 'GOOGL', 'AAPL', 'MSFT'],
                'Type': ['BUY', 'BUY', 'BUY', 'BUY'],
                'Quantity': [25, 50, 100, 75],
                'Price': [220.00, 2800.00, 150.00, 380.00],
                'Total': [5500.00, 140000.00, 15000.00, 28500.00],
                'Status': ['Settled', 'Settled', 'Settled', 'Settled']
            })
            
            # Format currency columns
            transactions['Price'] = transactions['Price'].apply(lambda x: f"${x:.2f}")
            transactions['Total'] = transactions['Total'].apply(lambda x: f"${x:,.2f}")
            
            st.dataframe(transactions, hide_index=True, use_container_width=True)
            
            # Transaction controls
            st.markdown("### Quick Actions")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("📊 Export Transactions"):
                    st.success("Transaction export started...")
            
            with col2:
                if st.button("📈 Generate Tax Report"):
                    st.success("Tax report generation started...")
            
            with col3:
                if st.button("📧 Email Summary"):
                    st.success("Portfolio summary sent!")
        
        # Portfolio alerts
        st.markdown("---")
        st.subheader("🚨 Portfolio Alerts")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Active Alerts:**")
            alerts = [
                "🟡 AAPL position up 15% - Consider profit taking",
                "🔴 TSLA showing high volatility - Review risk",
                "🟢 Cash allocation optimal for current market"
            ]
            
            for alert in alerts:
                st.markdown(f"• {alert}")
        
        with col2:
            st.markdown("**Recommendations:**")
            recommendations = [
                "Consider rebalancing overweight positions",
                "Review stop-loss orders for high-risk positions", 
                "Evaluate dividend opportunities for cash allocation"
            ]
            
            for rec in recommendations:
                st.markdown(f"• {rec}")