"""
Apache Flink Stream Processing
Performs real-time windowed aggregations on stock trading data
"""
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.window import Tumble
from pyflink.table.expressions import col, lit
import os
from dotenv import load_dotenv

load_dotenv()


def create_flink_pipeline():
    """
    Create and configure Flink stream processing pipeline
    Performs tumbling window aggregations (1-minute windows)
    """
    
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    
    # Create table environment
    settings = EnvironmentSettings.new_instance() \
        .in_streaming_mode() \
        .build()
    
    table_env = StreamTableEnvironment.create(env, settings)
    
    # Kafka configuration
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    kafka_topic = os.getenv('KAFKA_TOPIC', 'stock-trades')
    
    # PostgreSQL configuration
    pg_host = os.getenv('POSTGRES_HOST', 'localhost')
    pg_port = os.getenv('POSTGRES_PORT', '5432')
    pg_db = os.getenv('POSTGRES_DB', 'stock_market_db')
    pg_user = os.getenv('POSTGRES_USER', 'kafka_user')
    pg_password = os.getenv('POSTGRES_PASSWORD', 'kafka_password')
    
    # Create Kafka source table
    kafka_source_ddl = f"""
        CREATE TABLE stock_trades_source (
            trade_id STRING,
            `timestamp` TIMESTAMP(3),
            symbol STRING,
            price DECIMAL(10, 2),
            volume INT,
            trade_type STRING,
            total_value DECIMAL(15, 2),
            is_anomaly BOOLEAN,
            WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{kafka_topic}',
            'properties.bootstrap.servers' = '{kafka_servers}',
            'properties.group.id' = 'flink-consumer-group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """
    
    table_env.execute_sql(kafka_source_ddl)
    print("✓ Created Kafka source table")
    
    # Create PostgreSQL sink table for aggregated metrics
    postgres_sink_ddl = f"""
        CREATE TABLE stock_metrics_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            symbol STRING,
            avg_price DECIMAL(10, 2),
            min_price DECIMAL(10, 2),
            max_price DECIMAL(10, 2),
            total_volume BIGINT,
            trade_count INT,
            total_value DECIMAL(20, 2),
            PRIMARY KEY (window_start, window_end, symbol) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}',
            'table-name' = 'stock_metrics',
            'username' = '{pg_user}',
            'password' = '{pg_password}',
            'driver' = 'org.postgresql.Driver'
        )
    """
    
    table_env.execute_sql(postgres_sink_ddl)
    print("✓ Created PostgreSQL sink table")
    
    # Perform windowed aggregation
    # Tumbling window of 1 minute
    result_table = table_env.sql_query("""
        SELECT
            TUMBLE_START(`timestamp`, INTERVAL '1' MINUTE) as window_start,
            TUMBLE_END(`timestamp`, INTERVAL '1' MINUTE) as window_end,
            symbol,
            AVG(price) as avg_price,
            MIN(price) as min_price,
            MAX(price) as max_price,
            SUM(volume) as total_volume,
            COUNT(*) as trade_count,
            SUM(total_value) as total_value
        FROM stock_trades_source
        GROUP BY
            symbol,
            TUMBLE(`timestamp`, INTERVAL '1' MINUTE)
    """)
    
    # Insert results into PostgreSQL
    result_table.execute_insert('stock_metrics_sink')
    
    print("\n" + "="*60)
    print("🚀 FLINK STREAM PROCESSING STARTED")
    print("="*60)
    print("Window Type: Tumbling Window (1 minute)")
    print("Operations: AVG, MIN, MAX, SUM, COUNT")
    print("Sink: PostgreSQL (stock_metrics table)")
    print("="*60 + "\n")


def create_flink_pipeline_alternative():
    """
    Alternative Flink pipeline using Python DataStream API
    This is a simpler approach that doesn't require external connectors
    """
    print("\n⚠️ Note: Full Flink integration requires:")
    print("  - flink-connector-kafka JAR")
    print("  - flink-connector-jdbc JAR")
    print("  - PostgreSQL JDBC driver")
    print("\nFor production use, run Flink SQL Client or submit a Flink job")
    print("See flink_job.sql for the complete SQL-based pipeline\n")


# SQL script for Flink SQL Client
FLINK_SQL_SCRIPT = """
-- Flink SQL Script for Stream Processing
-- Run this in Flink SQL Client

-- Set execution mode
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'tableau';

-- Create Kafka source
CREATE TABLE stock_trades_source (
    trade_id STRING,
    `timestamp` TIMESTAMP(3),
    symbol STRING,
    price DECIMAL(10, 2),
    volume INT,
    trade_type STRING,
    total_value DECIMAL(15, 2),
    is_anomaly BOOLEAN,
    WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'stock-trades',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'flink-consumer-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- Create PostgreSQL sink
CREATE TABLE stock_metrics_sink (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    symbol STRING,
    avg_price DECIMAL(10, 2),
    min_price DECIMAL(10, 2),
    max_price DECIMAL(10, 2),
    total_volume BIGINT,
    trade_count INT,
    total_value DECIMAL(20, 2),
    PRIMARY KEY (window_start, window_end, symbol) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/stock_market_db',
    'table-name' = 'stock_metrics',
    'username' = 'kafka_user',
    'password' = 'kafka_password',
    'driver' = 'org.postgresql.Driver'
);

-- Execute windowed aggregation
INSERT INTO stock_metrics_sink
SELECT
    TUMBLE_START(`timestamp`, INTERVAL '1' MINUTE) as window_start,
    TUMBLE_END(`timestamp`, INTERVAL '1' MINUTE) as window_end,
    symbol,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    SUM(volume) as total_volume,
    COUNT(*) as trade_count,
    SUM(total_value) as total_value
FROM stock_trades_source
GROUP BY
    symbol,
    TUMBLE(`timestamp`, INTERVAL '1' MINUTE);
"""


if __name__ == "__main__":
    print("Apache Flink Stream Processing")
    print("="*60)
    
    # Save SQL script
    with open('flink_job.sql', 'w') as f:
        f.write(FLINK_SQL_SCRIPT)
    print("✓ Flink SQL script saved to flink_job.sql")
    
    try:
        create_flink_pipeline()
    except Exception as e:
        print(f"\n⚠️ PyFlink pipeline creation failed: {e}")
        create_flink_pipeline_alternative()
