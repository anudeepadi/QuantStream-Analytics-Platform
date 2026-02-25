"""
Portfolio API Endpoints

REST API endpoints for portfolio management and analysis.
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import List, Optional, Dict, Any
from datetime import datetime, date
import logging
import random

from ...models.portfolio import (
    PortfolioResponse,
    PortfolioPosition,
    Transaction,
    PortfolioSummary,
    PerformanceMetrics
)
from ...services.database_service import DatabaseService
from ...services.redis_service import RedisService

router = APIRouter()
logger = logging.getLogger(__name__)

_db_service = None

def set_services(db_svc):
    """Called from main.py lifespan to inject initialized DB service."""
    global _db_service
    _db_service = db_svc

# Mock portfolio data for demonstration
MOCK_POSITIONS = {
    "AAPL": {"shares": 100, "avg_cost": 150.00, "entry_date": "2024-01-15"},
    "GOOGL": {"shares": 50, "avg_cost": 2800.00, "entry_date": "2024-01-20"},
    "MSFT": {"shares": 75, "avg_cost": 380.00, "entry_date": "2024-01-10"},
    "AMZN": {"shares": 30, "avg_cost": 3200.00, "entry_date": "2024-02-01"},
    "TSLA": {"shares": 25, "avg_cost": 220.00, "entry_date": "2024-01-25"}
}

NAME_MAP = {
    "AAPL": "Apple Inc.",
    "GOOGL": "Alphabet Inc.",
    "MSFT": "Microsoft Corp.",
    "AMZN": "Amazon.com Inc.",
    "TSLA": "Tesla Inc.",
}

SECTOR_MAP = {
    "AAPL": "Technology",
    "GOOGL": "Communication Services",
    "MSFT": "Technology",
    "AMZN": "Consumer Discretionary",
    "TSLA": "Consumer Discretionary",
}

@router.get("/positions")
async def get_portfolio_positions(user_id: str = "demo_user"):
    """Get all portfolio positions"""
    
    try:
        positions = []
        
        # In production, would fetch current market prices
        mock_current_prices = {
            "AAPL": 180.50,
            "GOOGL": 2950.00,
            "MSFT": 420.75,
            "AMZN": 3400.25,
            "TSLA": 195.80
        }
        
        # First pass: compute total market value for weight calculation
        position_data = []
        total_market_value = 0.0
        
        for symbol, data in MOCK_POSITIONS.items():
            current_price = mock_current_prices.get(symbol, data["avg_cost"])
            market_value = data["shares"] * current_price
            cost_basis = data["shares"] * data["avg_cost"]
            unrealized_pnl = market_value - cost_basis
            total_market_value += market_value
            position_data.append({
                "symbol": symbol,
                "current_price": current_price,
                "market_value": market_value,
                "cost_basis": cost_basis,
                "unrealized_pnl": unrealized_pnl,
                "shares": data["shares"],
                "avg_cost": data["avg_cost"],
            })
        
        # Second pass: build response dicts with weight
        for p in position_data:
            weight = round((p["market_value"] / total_market_value) * 100, 2) if total_market_value else 0.0
            positions.append({
                "symbol": p["symbol"],
                "name": NAME_MAP.get(p["symbol"], p["symbol"]),
                "quantity": p["shares"],
                "avg_cost": p["avg_cost"],
                "current_price": p["current_price"],
                "market_value": round(p["market_value"], 2),
                "unrealized_pnl": round(p["unrealized_pnl"], 2),
                "unrealized_pnl_percent": round((p["unrealized_pnl"] / p["cost_basis"]) * 100, 2) if p["cost_basis"] else 0.0,
                "weight": weight,
                "sector": SECTOR_MAP.get(p["symbol"], "Other"),
            })
        
        return positions
        
    except Exception as e:
        logger.error(f"Error fetching portfolio positions: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching positions: {str(e)}")

@router.get("/summary")
async def get_portfolio_summary(user_id: str = "demo_user"):
    """Get portfolio summary"""
    
    try:
        positions = await get_portfolio_positions(user_id)
        
        total_market_value = sum(p["market_value"] for p in positions)
        total_cost = sum(p["quantity"] * p["avg_cost"] for p in positions)
        total_pnl = total_market_value - total_cost
        total_pnl_percent = round((total_pnl / total_cost) * 100, 2) if total_cost else 0.0
        cash = 25000.00
        buying_power = cash * 2  # Mock 2x margin
        daily_pnl = 1250.75
        daily_pnl_percent = round((daily_pnl / total_market_value) * 100, 2) if total_market_value else 0.0
        
        return {
            "total_value": round(total_market_value + cash, 2),
            "total_cost": round(total_cost, 2),
            "total_pnl": round(total_pnl, 2),
            "total_pnl_percent": total_pnl_percent,
            "daily_pnl": daily_pnl,
            "daily_pnl_percent": daily_pnl_percent,
            "cash": cash,
            "buying_power": buying_power,
        }
        
    except Exception as e:
        logger.error(f"Error calculating portfolio summary: {e}")
        raise HTTPException(status_code=500, detail=f"Error calculating summary: {str(e)}")

@router.get("/performance")
async def get_performance_metrics(
    user_id: str = "demo_user",
    days: int = 30
):
    """Get portfolio performance metrics"""
    
    try:
        periods = [
            {"period": "1D", "return_percent": 0.85, "benchmark_return_percent": 0.42, "alpha": 0.43, "sharpe_ratio": 1.2, "max_drawdown": -0.3, "volatility": 12.5},
            {"period": "1W", "return_percent": 2.15, "benchmark_return_percent": 1.10, "alpha": 1.05, "sharpe_ratio": 1.05, "max_drawdown": -1.2, "volatility": 14.8},
            {"period": "1M", "return_percent": 5.40, "benchmark_return_percent": 3.20, "alpha": 2.20, "sharpe_ratio": 0.95, "max_drawdown": -3.5, "volatility": 18.2},
            {"period": "3M", "return_percent": 12.30, "benchmark_return_percent": 8.50, "alpha": 3.80, "sharpe_ratio": 0.88, "max_drawdown": -6.1, "volatility": 20.5},
            {"period": "YTD", "return_percent": 15.75, "benchmark_return_percent": 10.20, "alpha": 5.55, "sharpe_ratio": 0.81, "max_drawdown": -8.3, "volatility": 22.5},
        ]
        
        return periods
        
    except Exception as e:
        logger.error(f"Error calculating performance metrics: {e}")
        raise HTTPException(status_code=500, detail=f"Error calculating metrics: {str(e)}")

@router.get("/transactions", response_model=List[Transaction])
async def get_transactions(
    user_id: str = "demo_user",
    limit: int = 50,
    offset: int = 0
):
    """Get transaction history"""
    
    try:
        # Mock transaction data
        mock_transactions = [
            {
                "id": "txn_001",
                "symbol": "TSLA",
                "transaction_type": "BUY",
                "quantity": 25.0,
                "price": 220.00,
                "total_amount": 5500.00,
                "transaction_date": "2024-01-25T10:30:00",
                "fees": 2.95
            },
            {
                "id": "txn_002",
                "symbol": "GOOGL",
                "transaction_type": "BUY",
                "quantity": 50.0,
                "price": 2800.00,
                "total_amount": 140000.00,
                "transaction_date": "2024-01-20T14:15:00",
                "fees": 7.50
            },
            {
                "id": "txn_003",
                "symbol": "AAPL",
                "transaction_type": "BUY",
                "quantity": 100.0,
                "price": 150.00,
                "total_amount": 15000.00,
                "transaction_date": "2024-01-15T09:45:00",
                "fees": 4.95
            },
            {
                "id": "txn_004",
                "symbol": "MSFT",
                "transaction_type": "BUY",
                "quantity": 75.0,
                "price": 380.00,
                "total_amount": 28500.00,
                "transaction_date": "2024-01-10T11:20:00",
                "fees": 6.25
            }
        ]
        
        transactions = []
        for txn_data in mock_transactions[offset:offset + limit]:
            transaction = Transaction(
                id=txn_data["id"],
                symbol=txn_data["symbol"],
                transaction_type=txn_data["transaction_type"],
                quantity=txn_data["quantity"],
                price=txn_data["price"],
                total_amount=txn_data["total_amount"],
                transaction_date=datetime.fromisoformat(txn_data["transaction_date"]),
                fees=txn_data["fees"],
                status="settled"
            )
            transactions.append(transaction)
        
        return transactions
        
    except Exception as e:
        logger.error(f"Error fetching transactions: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching transactions: {str(e)}")

@router.get("/allocation")
async def get_portfolio_allocation(user_id: str = "demo_user"):
    """Get portfolio allocation breakdown"""
    
    try:
        positions = await get_portfolio_positions(user_id)
        
        # Aggregate market value by sector
        sector_values = {}
        total_value = sum(p["market_value"] for p in positions)
        
        for p in positions:
            sector = p["sector"]
            sector_values[sector] = sector_values.get(sector, 0.0) + p["market_value"]
        
        colors = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899"]
        
        allocation = []
        for i, (sector, value) in enumerate(sector_values.items()):
            percentage = round((value / total_value) * 100, 2) if total_value else 0.0
            allocation.append({
                "category": sector,
                "value": round(value, 2),
                "percentage": percentage,
                "color": colors[i % len(colors)],
            })
        
        return allocation
        
    except Exception as e:
        logger.error(f"Error calculating allocation: {e}")
        raise HTTPException(status_code=500, detail=f"Error calculating allocation: {str(e)}")

@router.get("/activity")
async def get_recent_activity(user_id: str = "demo_user"):
    """Get recent portfolio activity"""
    
    try:
        from datetime import timedelta
        
        now = datetime.now()
        
        activities = [
            {
                "id": "act_001",
                "type": "buy",
                "symbol": "AAPL",
                "description": "Bought 10 shares of AAPL",
                "amount": 1805.00,
                "timestamp": (now - timedelta(hours=2)).isoformat(),
            },
            {
                "id": "act_002",
                "type": "sell",
                "symbol": "TSLA",
                "description": "Sold 5 shares of TSLA",
                "amount": 979.00,
                "timestamp": (now - timedelta(hours=5)).isoformat(),
            },
            {
                "id": "act_003",
                "type": "dividend",
                "symbol": "MSFT",
                "description": "Dividend payment from MSFT",
                "amount": 56.25,
                "timestamp": (now - timedelta(days=1)).isoformat(),
            },
            {
                "id": "act_004",
                "type": "deposit",
                "symbol": "",
                "description": "Cash deposit",
                "amount": 5000.00,
                "timestamp": (now - timedelta(days=2)).isoformat(),
            },
            {
                "id": "act_005",
                "type": "buy",
                "symbol": "GOOGL",
                "description": "Bought 3 shares of GOOGL",
                "amount": 8850.00,
                "timestamp": (now - timedelta(days=3)).isoformat(),
            },
            {
                "id": "act_006",
                "type": "withdrawal",
                "symbol": "",
                "description": "Cash withdrawal",
                "amount": 2000.00,
                "timestamp": (now - timedelta(days=4)).isoformat(),
            },
        ]
        
        return activities
        
    except Exception as e:
        logger.error(f"Error fetching recent activity: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching activity: {str(e)}")


@router.get("/risk-metrics")
async def get_risk_metrics(user_id: str = "demo_user"):
    """Get portfolio risk metrics"""
    
    try:
        return {
            "portfolio_beta": 1.15,
            "correlation_to_spy": 0.85,
            "tracking_error": 8.2,
            "information_ratio": 0.45,
            "concentration_risk": {
                "largest_position_percent": 28.5,
                "top_5_positions_percent": 88.2,
                "herfindahl_index": 0.22
            },
            "sector_exposure": {
                "technology": 75.5,
                "consumer_discretionary": 15.2,
                "communication_services": 9.3
            },
            "geographic_exposure": {
                "united_states": 100.0
            },
            "risk_score": 7.2,  # Scale of 1-10
            "risk_level": "Medium-High",
            "last_calculated": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error calculating risk metrics: {e}")
        raise HTTPException(status_code=500, detail=f"Error calculating risk: {str(e)}")

@router.post("/rebalance")
async def create_rebalance_recommendation(
    user_id: str = "demo_user",
    target_allocation: Optional[Dict[str, float]] = None
):
    """Create portfolio rebalance recommendations"""
    
    try:
        positions = await get_portfolio_positions(user_id)
        
        # Mock rebalancing logic
        recommendations = []
        
        for pos in positions:
            # Simple rebalancing example
            current_weight = 20.0  # Mock current weight
            target_weight = target_allocation.get(pos.symbol, 20.0) if target_allocation else 20.0
            
            if abs(current_weight - target_weight) > 2.0:  # 2% threshold
                action = "SELL" if current_weight > target_weight else "BUY"
                shares_to_trade = abs(current_weight - target_weight) * 10  # Mock calculation
                
                recommendations.append({
                    "symbol": pos.symbol,
                    "action": action,
                    "current_weight": current_weight,
                    "target_weight": target_weight,
                    "shares_to_trade": shares_to_trade,
                    "estimated_value": shares_to_trade * pos.current_price
                })
        
        return {
            "recommendations": recommendations,
            "estimated_cost": sum(r.get("estimated_value", 0) for r in recommendations) * 0.001,  # Mock cost
            "generated_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error creating rebalance recommendation: {e}")
        raise HTTPException(status_code=500, detail=f"Error creating recommendation: {str(e)}")

@router.get("/dividends")
async def get_dividend_information(user_id: str = "demo_user"):
    """Get dividend information for portfolio"""
    
    try:
        # Mock dividend data
        return {
            "annual_dividend_income": 2450.80,
            "dividend_yield": 1.85,
            "upcoming_dividends": [
                {
                    "symbol": "AAPL",
                    "ex_dividend_date": "2024-02-15",
                    "payment_date": "2024-02-22",
                    "dividend_per_share": 0.24,
                    "estimated_payment": 24.00
                },
                {
                    "symbol": "MSFT",
                    "ex_dividend_date": "2024-02-20",
                    "payment_date": "2024-02-28",
                    "dividend_per_share": 0.75,
                    "estimated_payment": 56.25
                }
            ],
            "dividend_history": [
                {
                    "date": "2024-01-15",
                    "symbol": "AAPL",
                    "amount": 24.00
                },
                {
                    "date": "2024-01-10",
                    "symbol": "MSFT",
                    "amount": 56.25
                }
            ],
            "last_updated": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error fetching dividend information: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching dividends: {str(e)}")

@router.get("/tax-lots")
async def get_tax_lots(user_id: str = "demo_user"):
    """Get tax lot information for portfolio positions"""
    
    try:
        # Mock tax lot data
        return {
            "tax_lots": [
                {
                    "symbol": "AAPL",
                    "acquisition_date": "2024-01-15",
                    "shares": 100.0,
                    "cost_basis": 150.00,
                    "current_price": 180.50,
                    "unrealized_gain": 3050.00,
                    "holding_period": "short_term"
                },
                {
                    "symbol": "GOOGL",
                    "acquisition_date": "2024-01-20",
                    "shares": 50.0,
                    "cost_basis": 2800.00,
                    "current_price": 2950.00,
                    "unrealized_gain": 7500.00,
                    "holding_period": "short_term"
                }
            ],
            "tax_summary": {
                "total_unrealized_gains": 15750.00,
                "total_unrealized_losses": -850.00,
                "net_unrealized_gain": 14900.00,
                "estimated_tax_liability": 3725.00  # Assuming 25% rate
            },
            "last_updated": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error fetching tax lots: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching tax lots: {str(e)}")

@router.get("/export/{format}")
async def export_portfolio_data(
    format: str,
    user_id: str = "demo_user",
    include_transactions: bool = True,
    include_performance: bool = True
):
    """Export portfolio data in various formats"""
    
    if format.lower() not in ["csv", "json", "xlsx"]:
        raise HTTPException(status_code=400, detail="Unsupported export format")
    
    try:
        # Mock export functionality
        return {
            "export_url": f"/downloads/portfolio_{user_id}_{datetime.now().strftime('%Y%m%d')}.{format.lower()}",
            "expires_at": (datetime.now().timestamp() + 3600),  # 1 hour
            "format": format.lower(),
            "generated_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error exporting portfolio data: {e}")
        raise HTTPException(status_code=500, detail=f"Error exporting data: {str(e)}")