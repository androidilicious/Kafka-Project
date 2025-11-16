# Flink SQL Job for Stock Market Stream Processing
# This file can be executed in Flink SQL Client

-- ============================================
-- STEP 1: Configure Flink Environment
-- ============================================

SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'parallelism.default' = '2';

-- ============================================
-- STEP 2: Create Kafka Source Table
-- ============================================

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
    'properties.group.id' = 'flink-sql-consumer',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- ============================================
-- STEP 3: Create PostgreSQL Sink Table
-- ============================================

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

-- ============================================
-- STEP 4: Tumbling Window Aggregation (1 minute)
-- ============================================

INSERT INTO stock_metrics_sink
SELECT
    TUMBLE_START(`timestamp`, INTERVAL '1' MINUTE) as window_start,
    TUMBLE_END(`timestamp`, INTERVAL '1' MINUTE) as window_end,
    symbol,
    CAST(AVG(price) AS DECIMAL(10, 2)) as avg_price,
    CAST(MIN(price) AS DECIMAL(10, 2)) as min_price,
    CAST(MAX(price) AS DECIMAL(10, 2)) as max_price,
    CAST(SUM(volume) AS BIGINT) as total_volume,
    CAST(COUNT(*) AS INT) as trade_count,
    CAST(SUM(total_value) AS DECIMAL(20, 2)) as total_value
FROM stock_trades_source
GROUP BY
    symbol,
    TUMBLE(`timestamp`, INTERVAL '1' MINUTE);

-- ============================================
-- OPTIONAL: Sliding Window (5 min, slide 1 min)
-- ============================================

-- Uncomment to use sliding windows instead

/*
CREATE TABLE stock_metrics_sliding_sink (
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
    'table-name' = 'stock_metrics_sliding',
    'username' = 'kafka_user',
    'password' = 'kafka_password',
    'driver' = 'org.postgresql.Driver'
);

INSERT INTO stock_metrics_sliding_sink
SELECT
    HOP_START(`timestamp`, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) as window_start,
    HOP_END(`timestamp`, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) as window_end,
    symbol,
    CAST(AVG(price) AS DECIMAL(10, 2)) as avg_price,
    CAST(MIN(price) AS DECIMAL(10, 2)) as min_price,
    CAST(MAX(price) AS DECIMAL(10, 2)) as max_price,
    CAST(SUM(volume) AS BIGINT) as total_volume,
    CAST(COUNT(*) AS INT) as trade_count,
    CAST(SUM(total_value) AS DECIMAL(20, 2)) as total_value
FROM stock_trades_source
GROUP BY
    symbol,
    HOP(`timestamp`, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE);
*/

-- ============================================
-- OPTIONAL: Real-time Anomaly Detection Query
-- ============================================

/*
CREATE TABLE anomaly_alerts (
    `timestamp` TIMESTAMP(3),
    symbol STRING,
    price DECIMAL(10, 2),
    volume INT,
    reason STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'anomaly-alerts',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);

-- Detect trades with unusually high volume
INSERT INTO anomaly_alerts
SELECT
    `timestamp`,
    symbol,
    price,
    volume,
    'High Volume' as reason
FROM stock_trades_source
WHERE volume > 50000;
*/
