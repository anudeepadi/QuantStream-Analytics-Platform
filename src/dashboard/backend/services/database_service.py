"""
Database Service

Handles database connections and operations for the dashboard backend.
"""

import asyncio
import logging
from typing import Optional, Dict, Any, List
import asyncpg
import os
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

class DatabaseService:
    """Database service for PostgreSQL operations"""
    
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self.connection_string = self._build_connection_string()
    
    def _build_connection_string(self) -> str:
        """Build database connection string from environment variables"""
        
        db_url = os.getenv("DATABASE_URL")
        if db_url:
            return db_url
        
        # Build from individual components
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        database = os.getenv("DB_NAME", "quantstream")
        user = os.getenv("DB_USER", "postgres")
        password = os.getenv("DB_PASSWORD", "postgres")
        
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    async def initialize(self):
        """Initialize database connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                self.connection_string,
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            
            # Run initial setup
            await self._create_tables()
            
            logger.info("Database connection pool initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    async def close(self):
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")
    
    async def health_check(self) -> bool:
        """Check database connectivity"""
        try:
            if not self.pool:
                return False
            
            async with self.pool.acquire() as connection:
                await connection.fetchval("SELECT 1")
            
            return True
            
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    @asynccontextmanager
    async def get_connection(self):
        """Get database connection from pool"""
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        
        async with self.pool.acquire() as connection:
            yield connection
    
    async def _create_tables(self):
        """Create necessary database tables"""
        
        tables_sql = """
        -- Users table
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            role VARCHAR(20) NOT NULL DEFAULT 'viewer',
            full_name VARCHAR(100),
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login TIMESTAMP
        );
        
        -- Market data cache table
        CREATE TABLE IF NOT EXISTS market_data_cache (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            open_price DECIMAL(10, 4),
            high_price DECIMAL(10, 4),
            low_price DECIMAL(10, 4),
            close_price DECIMAL(10, 4),
            volume BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp)
        );
        
        -- Portfolio positions table
        CREATE TABLE IF NOT EXISTS portfolio_positions (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            symbol VARCHAR(10) NOT NULL,
            shares DECIMAL(12, 6) NOT NULL,
            avg_cost DECIMAL(10, 4) NOT NULL,
            entry_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, symbol)
        );
        
        -- Transactions table
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            symbol VARCHAR(10) NOT NULL,
            transaction_type VARCHAR(10) NOT NULL, -- BUY, SELL
            quantity DECIMAL(12, 6) NOT NULL,
            price DECIMAL(10, 4) NOT NULL,
            total_amount DECIMAL(15, 2) NOT NULL,
            transaction_date TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Alerts table
        CREATE TABLE IF NOT EXISTS alerts (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            alert_type VARCHAR(20) NOT NULL, -- price, volume, technical, etc.
            symbol VARCHAR(10),
            conditions JSONB NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            triggered_at TIMESTAMP,
            message TEXT
        );
        
        -- Alert history table
        CREATE TABLE IF NOT EXISTS alert_history (
            id SERIAL PRIMARY KEY,
            alert_id INTEGER REFERENCES alerts(id),
            user_id INTEGER REFERENCES users(id),
            triggered_at TIMESTAMP NOT NULL,
            message TEXT,
            severity VARCHAR(10) DEFAULT 'medium',
            actions_taken JSONB
        );
        
        -- System metrics table
        CREATE TABLE IF NOT EXISTS system_metrics (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            metric_name VARCHAR(50) NOT NULL,
            metric_value DECIMAL(10, 4) NOT NULL,
            tags JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- User sessions table
        CREATE TABLE IF NOT EXISTS user_sessions (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            session_token VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            ip_address INET,
            user_agent TEXT
        );
        
        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_market_data_symbol_timestamp ON market_data_cache(symbol, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_portfolio_positions_user_symbol ON portfolio_positions(user_id, symbol);
        CREATE INDEX IF NOT EXISTS idx_transactions_user_date ON transactions(user_id, transaction_date DESC);
        CREATE INDEX IF NOT EXISTS idx_alerts_user_active ON alerts(user_id, is_active);
        CREATE INDEX IF NOT EXISTS idx_alert_history_triggered ON alert_history(triggered_at DESC);
        CREATE INDEX IF NOT EXISTS idx_system_metrics_timestamp ON system_metrics(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_user_sessions_token ON user_sessions(session_token);
        """
        
        async with self.get_connection() as conn:
            await conn.execute(tables_sql)
        
        logger.info("Database tables created/verified")
    
    # User operations
    async def get_user(self, username: str) -> Optional[Dict[str, Any]]:
        """Get user by username"""
        
        async with self.get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM users WHERE username = $1 AND is_active = TRUE",
                username
            )
            
            return dict(row) if row else None
    
    async def create_user(self, user_data: Dict[str, Any]) -> int:
        """Create new user"""
        
        async with self.get_connection() as conn:
            user_id = await conn.fetchval(
                """
                INSERT INTO users (username, email, password_hash, role, full_name)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id
                """,
                user_data["username"],
                user_data["email"],
                user_data["password_hash"],
                user_data.get("role", "viewer"),
                user_data.get("full_name")
            )
            
            return user_id
    
    async def get_user_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get user by ID"""
        async with self.get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM users WHERE id = $1 AND is_active = TRUE",
                user_id
            )
            return dict(row) if row else None

    async def update_last_login(self, user_id: int):
        """Update user last login timestamp"""
        async with self.get_connection() as conn:
            await conn.execute(
                "UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = $1",
                user_id
            )

    async def update_user_profile(self, user_id: int, profile_data: Dict[str, Any]) -> bool:
        """Update user profile fields"""
        async with self.get_connection() as conn:
            await conn.execute(
                """
                UPDATE users SET full_name = COALESCE($2, full_name),
                                 email = COALESCE($3, email)
                WHERE id = $1
                """,
                user_id,
                profile_data.get("full_name"),
                profile_data.get("email"),
            )
            return True

    async def update_user_password(self, user_id: int, password_hash: str) -> bool:
        """Update user password hash"""
        async with self.get_connection() as conn:
            await conn.execute(
                "UPDATE users SET password_hash = $2 WHERE id = $1",
                user_id, password_hash
            )
            return True

    # Portfolio operations
    async def get_portfolio_positions(self, user_id: int) -> List[Dict[str, Any]]:
        """Get all portfolio positions for a user"""
        
        async with self.get_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM portfolio_positions 
                WHERE user_id = $1 
                ORDER BY symbol
                """,
                user_id
            )
            
            return [dict(row) for row in rows]
    
    async def update_portfolio_position(self, user_id: int, symbol: str, shares: float, avg_cost: float):
        """Update or create portfolio position"""
        
        async with self.get_connection() as conn:
            await conn.execute(
                """
                INSERT INTO portfolio_positions (user_id, symbol, shares, avg_cost, entry_date, updated_at)
                VALUES ($1, $2, $3, $4, CURRENT_DATE, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id, symbol) 
                DO UPDATE SET 
                    shares = EXCLUDED.shares,
                    avg_cost = EXCLUDED.avg_cost,
                    updated_at = CURRENT_TIMESTAMP
                """,
                user_id, symbol.upper(), shares, avg_cost
            )
    
    # Alert operations
    async def get_user_alerts(self, user_id: int, active_only: bool = True) -> List[Dict[str, Any]]:
        """Get alerts for a user"""
        
        query = "SELECT * FROM alerts WHERE user_id = $1"
        if active_only:
            query += " AND is_active = TRUE"
        query += " ORDER BY created_at DESC"
        
        async with self.get_connection() as conn:
            rows = await conn.fetch(query, user_id)
            return [dict(row) for row in rows]
    
    async def create_alert(self, user_id: int, alert_data: Dict[str, Any]) -> int:
        """Create new alert"""
        
        async with self.get_connection() as conn:
            alert_id = await conn.fetchval(
                """
                INSERT INTO alerts (user_id, alert_type, symbol, conditions, message)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id
                """,
                user_id,
                alert_data["alert_type"],
                alert_data.get("symbol"),
                alert_data["conditions"],
                alert_data.get("message")
            )
            
            return alert_id
    
    # Market data operations
    async def store_market_data(self, symbol: str, data: Dict[str, Any]):
        """Store market data point"""
        
        async with self.get_connection() as conn:
            await conn.execute(
                """
                INSERT INTO market_data_cache 
                (symbol, timestamp, open_price, high_price, low_price, close_price, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, timestamp) DO NOTHING
                """,
                symbol.upper(),
                data["timestamp"],
                data["open"],
                data["high"],
                data["low"],
                data["close"],
                data["volume"]
            )
    
    async def get_historical_data(self, symbol: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Get historical market data from cache"""
        
        async with self.get_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM market_data_cache
                WHERE symbol = $1 AND timestamp BETWEEN $2 AND $3
                ORDER BY timestamp
                """,
                symbol.upper(), start_date, end_date
            )
            
            return [dict(row) for row in rows]
    
    # System metrics operations
    async def store_system_metric(self, metric_name: str, value: float, tags: Dict[str, Any] = None):
        """Store system metric"""
        
        async with self.get_connection() as conn:
            await conn.execute(
                """
                INSERT INTO system_metrics (timestamp, metric_name, metric_value, tags)
                VALUES (CURRENT_TIMESTAMP, $1, $2, $3)
                """,
                metric_name, value, tags or {}
            )
    
    async def get_system_metrics(self, metric_name: str = None, hours: int = 24) -> List[Dict[str, Any]]:
        """Get system metrics"""
        
        query = """
        SELECT * FROM system_metrics
        WHERE timestamp >= NOW() - INTERVAL '%s hours'
        """ % hours
        
        if metric_name:
            query += " AND metric_name = $1"
            query += " ORDER BY timestamp DESC"
            
            async with self.get_connection() as conn:
                rows = await conn.fetch(query, metric_name)
        else:
            query += " ORDER BY timestamp DESC"
            
            async with self.get_connection() as conn:
                rows = await conn.fetch(query)
        
        return [dict(row) for row in rows]