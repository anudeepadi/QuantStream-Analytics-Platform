"""
Portfolio Models

Pydantic models for portfolio-related API requests and responses.
"""

from pydantic import BaseModel, Field
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from decimal import Decimal
from enum import Enum

class TransactionType(str, Enum):
    """Transaction types"""
    BUY = "BUY"
    SELL = "SELL"
    DIVIDEND = "DIVIDEND"
    SPLIT = "SPLIT"
    TRANSFER = "TRANSFER"

class HoldingPeriod(str, Enum):
    """Tax holding periods"""
    SHORT_TERM = "short_term"
    LONG_TERM = "long_term"

class PortfolioPosition(BaseModel):
    """Individual portfolio position"""
    symbol: str = Field(..., description="Stock symbol")
    shares: float = Field(..., description="Number of shares")
    avg_cost: float = Field(..., description="Average cost basis per share")
    current_price: float = Field(..., description="Current market price")
    market_value: float = Field(..., description="Current market value")
    cost_basis: float = Field(..., description="Total cost basis")
    unrealized_pnl: float = Field(..., description="Unrealized profit/loss")
    unrealized_pnl_percent: float = Field(..., description="Unrealized P&L percentage")
    entry_date: date = Field(..., description="Position entry date")
    last_updated: datetime = Field(..., description="Last price update")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat()
        }

class Transaction(BaseModel):
    """Portfolio transaction record"""
    id: str = Field(..., description="Transaction ID")
    symbol: str = Field(..., description="Stock symbol")
    transaction_type: TransactionType = Field(..., description="Transaction type")
    quantity: float = Field(..., description="Number of shares")
    price: float = Field(..., description="Price per share")
    total_amount: float = Field(..., description="Total transaction amount")
    transaction_date: datetime = Field(..., description="Transaction date")
    fees: Optional[float] = Field(None, description="Transaction fees")
    status: str = Field(default="pending", description="Transaction status")
    notes: Optional[str] = Field(None, description="Transaction notes")

class PortfolioSummary(BaseModel):
    """Portfolio summary information"""
    portfolio_value: float = Field(..., description="Total portfolio value")
    cash_balance: float = Field(..., description="Cash balance")
    total_market_value: float = Field(..., description="Total market value of positions")
    total_cost_basis: float = Field(..., description="Total cost basis")
    total_unrealized_pnl: float = Field(..., description="Total unrealized P&L")
    total_return: float = Field(..., description="Total return")
    total_return_percent: float = Field(..., description="Total return percentage")
    day_change: float = Field(..., description="Daily change in value")
    day_change_percent: float = Field(..., description="Daily change percentage")
    positions_count: int = Field(..., description="Number of positions")
    last_updated: datetime = Field(..., description="Last update timestamp")

class PerformanceMetrics(BaseModel):
    """Portfolio performance metrics"""
    total_return_percent: float = Field(..., description="Total return percentage")
    annualized_return: float = Field(..., description="Annualized return")
    volatility: float = Field(..., description="Portfolio volatility")
    sharpe_ratio: float = Field(..., description="Sharpe ratio")
    max_drawdown: float = Field(..., description="Maximum drawdown")
    beta: float = Field(..., description="Portfolio beta")
    alpha: float = Field(..., description="Portfolio alpha")
    win_rate: float = Field(..., description="Win rate percentage")
    profit_factor: float = Field(..., description="Profit factor")
    calmar_ratio: float = Field(..., description="Calmar ratio")
    sortino_ratio: float = Field(..., description="Sortino ratio")
    var_95: float = Field(..., description="Value at Risk (95%)")
    period_days: int = Field(..., description="Analysis period in days")
    last_calculated: datetime = Field(..., description="Last calculation timestamp")

class AllocationBreakdown(BaseModel):
    """Portfolio allocation breakdown"""
    by_sector: Dict[str, float] = Field(..., description="Allocation by sector")
    by_position: List[Dict[str, Any]] = Field(..., description="Allocation by position")
    cash_allocation: float = Field(..., description="Cash allocation percentage")
    last_updated: datetime = Field(..., description="Last update timestamp")

class RiskMetrics(BaseModel):
    """Portfolio risk metrics"""
    portfolio_beta: float = Field(..., description="Portfolio beta")
    correlation_to_spy: float = Field(..., description="Correlation to S&P 500")
    tracking_error: float = Field(..., description="Tracking error")
    information_ratio: float = Field(..., description="Information ratio")
    concentration_risk: Dict[str, float] = Field(..., description="Concentration risk metrics")
    sector_exposure: Dict[str, float] = Field(..., description="Sector exposure")
    geographic_exposure: Dict[str, float] = Field(..., description="Geographic exposure")
    risk_score: float = Field(..., description="Overall risk score (1-10)")
    risk_level: str = Field(..., description="Risk level description")
    last_calculated: datetime = Field(..., description="Last calculation timestamp")

class RebalanceRecommendation(BaseModel):
    """Portfolio rebalance recommendation"""
    symbol: str = Field(..., description="Stock symbol")
    action: str = Field(..., description="Recommended action (BUY/SELL)")
    current_weight: float = Field(..., description="Current portfolio weight")
    target_weight: float = Field(..., description="Target portfolio weight")
    shares_to_trade: float = Field(..., description="Shares to trade")
    estimated_value: float = Field(..., description="Estimated trade value")

class RebalanceResponse(BaseModel):
    """Rebalance response"""
    recommendations: List[RebalanceRecommendation] = Field(..., description="Rebalance recommendations")
    estimated_cost: float = Field(..., description="Estimated transaction costs")
    generated_at: datetime = Field(..., description="Generation timestamp")

class DividendInfo(BaseModel):
    """Dividend information"""
    symbol: str = Field(..., description="Stock symbol")
    ex_dividend_date: date = Field(..., description="Ex-dividend date")
    payment_date: date = Field(..., description="Payment date")
    dividend_per_share: float = Field(..., description="Dividend per share")
    estimated_payment: float = Field(..., description="Estimated total payment")

class DividendSummary(BaseModel):
    """Dividend summary"""
    annual_dividend_income: float = Field(..., description="Annual dividend income")
    dividend_yield: float = Field(..., description="Portfolio dividend yield")
    upcoming_dividends: List[DividendInfo] = Field(..., description="Upcoming dividends")
    dividend_history: List[Dict[str, Any]] = Field(..., description="Dividend history")
    last_updated: datetime = Field(..., description="Last update timestamp")

class TaxLot(BaseModel):
    """Tax lot information"""
    symbol: str = Field(..., description="Stock symbol")
    acquisition_date: date = Field(..., description="Acquisition date")
    shares: float = Field(..., description="Number of shares")
    cost_basis: float = Field(..., description="Cost basis per share")
    current_price: float = Field(..., description="Current price")
    unrealized_gain: float = Field(..., description="Unrealized gain/loss")
    holding_period: HoldingPeriod = Field(..., description="Holding period classification")

class TaxSummary(BaseModel):
    """Tax summary information"""
    total_unrealized_gains: float = Field(..., description="Total unrealized gains")
    total_unrealized_losses: float = Field(..., description="Total unrealized losses")
    net_unrealized_gain: float = Field(..., description="Net unrealized gain")
    estimated_tax_liability: float = Field(..., description="Estimated tax liability")

class TaxReport(BaseModel):
    """Tax report"""
    tax_lots: List[TaxLot] = Field(..., description="Tax lots")
    tax_summary: TaxSummary = Field(..., description="Tax summary")
    last_updated: datetime = Field(..., description="Last update timestamp")

class ExportRequest(BaseModel):
    """Portfolio export request"""
    format: str = Field(..., description="Export format (csv, json, xlsx)")
    include_transactions: bool = Field(default=True, description="Include transaction history")
    include_performance: bool = Field(default=True, description="Include performance metrics")
    date_range: Optional[Dict[str, date]] = Field(None, description="Date range for export")

class ExportResponse(BaseModel):
    """Portfolio export response"""
    export_url: str = Field(..., description="Download URL")
    expires_at: float = Field(..., description="Expiration timestamp")
    format: str = Field(..., description="Export format")
    generated_at: datetime = Field(..., description="Generation timestamp")

class PortfolioResponse(BaseModel):
    """Portfolio API response wrapper"""
    data: Any = Field(..., description="Response data")
    summary: Optional[PortfolioSummary] = Field(None, description="Portfolio summary")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    source: str = Field(default="quantstream", description="Data source")