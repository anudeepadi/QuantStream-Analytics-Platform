-- QuantStream Analytics Platform - Database Initialization
-- This script runs on first Postgres container start against the default DB
-- (POSTGRES_DB=quantstream).  The Python backend also auto-creates tables,
-- but having them here ensures the schema exists before the API boots.

-- Users
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

-- Market data cache
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

-- Portfolio positions
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

-- Transactions
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    symbol VARCHAR(10) NOT NULL,
    transaction_type VARCHAR(10) NOT NULL,
    quantity DECIMAL(12, 6) NOT NULL,
    price DECIMAL(10, 4) NOT NULL,
    total_amount DECIMAL(15, 2) NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alerts
CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    alert_type VARCHAR(20) NOT NULL,
    symbol VARCHAR(10),
    conditions JSONB NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    triggered_at TIMESTAMP,
    message TEXT
);

-- Alert history
CREATE TABLE IF NOT EXISTS alert_history (
    id SERIAL PRIMARY KEY,
    alert_id INTEGER REFERENCES alerts(id),
    user_id INTEGER REFERENCES users(id),
    triggered_at TIMESTAMP NOT NULL,
    message TEXT,
    severity VARCHAR(10) DEFAULT 'medium',
    actions_taken JSONB
);

-- System metrics
CREATE TABLE IF NOT EXISTS system_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    metric_value DECIMAL(10, 4) NOT NULL,
    tags JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User sessions
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

-- Indexes
CREATE INDEX IF NOT EXISTS idx_market_data_symbol_timestamp ON market_data_cache(symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_portfolio_positions_user_symbol ON portfolio_positions(user_id, symbol);
CREATE INDEX IF NOT EXISTS idx_transactions_user_date ON transactions(user_id, transaction_date DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_user_active ON alerts(user_id, is_active);
CREATE INDEX IF NOT EXISTS idx_alert_history_triggered ON alert_history(triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_system_metrics_timestamp ON system_metrics(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_user_sessions_token ON user_sessions(session_token);

-- Seed portfolio positions for admin user (id=1, seeded by the Python app)
-- These will be inserted by the app's seed function, but we prepare the structure here.
