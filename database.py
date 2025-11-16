"""
Database initialization and utility functions
"""
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from dotenv import load_dotenv

load_dotenv()


def get_db_config():
    """Get database configuration from environment variables"""
    return {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'database': os.getenv('POSTGRES_DB', 'stock_market_db'),
        'user': os.getenv('POSTGRES_USER', 'kafka_user'),
        'password': os.getenv('POSTGRES_PASSWORD', 'kafka_password')
    }


def create_tables():
    """Create database tables if they don't exist"""
    config = get_db_config()
    
    try:
        conn = psycopg2.connect(**config)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Create trades table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id SERIAL PRIMARY KEY,
                trade_id VARCHAR(255) UNIQUE NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                symbol VARCHAR(10) NOT NULL,
                price DECIMAL(10, 2) NOT NULL,
                volume INTEGER NOT NULL,
                trade_type VARCHAR(10) NOT NULL,
                total_value DECIMAL(15, 2) NOT NULL,
                is_anomaly BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create index on timestamp for faster queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_trades_timestamp 
            ON trades(timestamp DESC)
        """)
        
        # Create index on symbol
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_trades_symbol 
            ON trades(symbol)
        """)
        
        # Create aggregated metrics table (for Flink output)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_metrics (
                id SERIAL PRIMARY KEY,
                window_start TIMESTAMP NOT NULL,
                window_end TIMESTAMP NOT NULL,
                symbol VARCHAR(10) NOT NULL,
                avg_price DECIMAL(10, 2),
                min_price DECIMAL(10, 2),
                max_price DECIMAL(10, 2),
                total_volume BIGINT,
                trade_count INTEGER,
                total_value DECIMAL(20, 2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(window_start, window_end, symbol)
            )
        """)
        
        # Create index on window times
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_metrics_window 
            ON stock_metrics(window_end DESC)
        """)
        
        print("✓ Database tables created successfully")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"✗ Error creating tables: {e}")
        raise


def get_connection():
    """Get a database connection"""
    config = get_db_config()
    return psycopg2.connect(**config)


if __name__ == "__main__":
    print("Initializing database...")
    create_tables()
    print("Database initialization complete!")
