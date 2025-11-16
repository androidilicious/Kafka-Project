"""
Simplified Flink-style aggregation using Python
Performs windowed aggregations without full Flink setup
"""
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import psycopg2
from datetime import datetime, timedelta
from collections import defaultdict
import os
from dotenv import load_dotenv
from database import get_connection

load_dotenv()


class StreamAggregator:
    """
    Performs real-time windowed aggregations on stock data
    Simulates Flink-style tumbling windows
    """
    
    def __init__(self, window_size_seconds=60):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'stock-trades')
        self.consumer = None
        self.db_conn = None
        self.window_size = window_size_seconds
        
        # Store data for current window
        self.window_data = defaultdict(list)
        self.current_window_start = None
        
    def connect_kafka(self):
        """Connect to Kafka broker"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='aggregator-consumer-group'
            )
            print(f"✓ Connected to Kafka at {self.bootstrap_servers}")
        except KafkaError as e:
            print(f"✗ Failed to connect to Kafka: {e}")
            raise
    
    def connect_database(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = get_connection()
            print("✓ Connected to PostgreSQL database")
        except Exception as e:
            print(f"✗ Failed to connect to database: {e}")
            raise
    
    def get_window_start(self, timestamp):
        """Get the start of the window for a given timestamp"""
        ts = datetime.fromisoformat(timestamp)
        # Round down to nearest window boundary
        seconds_since_epoch = int(ts.timestamp())
        window_start_seconds = (seconds_since_epoch // self.window_size) * self.window_size
        return datetime.fromtimestamp(window_start_seconds)
    
    def aggregate_window(self, symbol, trades):
        """Aggregate trades for a symbol in a window"""
        if not trades:
            return None
        
        prices = [t['price'] for t in trades]
        volumes = [t['volume'] for t in trades]
        values = [t['total_value'] for t in trades]
        
        return {
            'avg_price': sum(prices) / len(prices),
            'min_price': min(prices),
            'max_price': max(prices),
            'total_volume': sum(volumes),
            'trade_count': len(trades),
            'total_value': sum(values)
        }
    
    def insert_metrics(self, window_start, window_end, symbol, metrics):
        """Insert aggregated metrics into database"""
        try:
            cursor = self.db_conn.cursor()
            
            insert_query = """
                INSERT INTO stock_metrics 
                (window_start, window_end, symbol, avg_price, min_price, 
                 max_price, total_volume, trade_count, total_value)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (window_start, window_end, symbol) 
                DO UPDATE SET
                    avg_price = EXCLUDED.avg_price,
                    min_price = EXCLUDED.min_price,
                    max_price = EXCLUDED.max_price,
                    total_volume = EXCLUDED.total_volume,
                    trade_count = EXCLUDED.trade_count,
                    total_value = EXCLUDED.total_value
            """
            
            cursor.execute(insert_query, (
                window_start,
                window_end,
                symbol,
                metrics['avg_price'],
                metrics['min_price'],
                metrics['max_price'],
                metrics['total_volume'],
                metrics['trade_count'],
                metrics['total_value']
            ))
            
            self.db_conn.commit()
            cursor.close()
            return True
            
        except Exception as e:
            print(f"✗ Error inserting metrics: {e}")
            self.db_conn.rollback()
            return False
    
    def process_window(self, window_start):
        """Process and aggregate all data for a completed window"""
        window_end = window_start + timedelta(seconds=self.window_size)
        
        print(f"\n{'='*60}")
        print(f"📊 Processing Window: {window_start} to {window_end}")
        print(f"{'='*60}")
        
        total_trades = 0
        for symbol, trades in self.window_data.items():
            metrics = self.aggregate_window(symbol, trades)
            if metrics:
                self.insert_metrics(window_start, window_end, symbol, metrics)
                total_trades += metrics['trade_count']
                
                print(f"  {symbol}: {metrics['trade_count']} trades | "
                      f"Avg: ${metrics['avg_price']:.2f} | "
                      f"Vol: {metrics['total_volume']:,} | "
                      f"Value: ${metrics['total_value']:,.2f}")
        
        print(f"{'='*60}")
        print(f"✓ Window processed: {total_trades} total trades across {len(self.window_data)} symbols")
        print(f"{'='*60}\n")
        
        # Clear window data
        self.window_data.clear()
    
    def run(self):
        """Main aggregation loop"""
        print("\n" + "="*60)
        print("🔄 STREAM AGGREGATOR STARTED")
        print("="*60)
        print(f"Window Type: Tumbling Window ({self.window_size} seconds)")
        print("Operations: AVG, MIN, MAX, SUM, COUNT")
        print("Output: PostgreSQL (stock_metrics table)")
        print("="*60 + "\n")
        
        try:
            for message in self.consumer:
                trade = message.value
                
                # Get window for this trade
                window_start = self.get_window_start(trade['timestamp'])
                
                # Initialize window if needed
                if self.current_window_start is None:
                    self.current_window_start = window_start
                
                # Check if we've moved to a new window
                if window_start > self.current_window_start:
                    # Process completed window
                    self.process_window(self.current_window_start)
                    self.current_window_start = window_start
                
                # Add trade to current window
                self.window_data[trade['symbol']].append(trade)
                
        except KeyboardInterrupt:
            print("\n\nShutting down aggregator...")
            # Process any remaining window data
            if self.window_data:
                self.process_window(self.current_window_start)
        finally:
            if self.consumer:
                self.consumer.close()
                print("✓ Kafka consumer closed")
            if self.db_conn:
                self.db_conn.close()
                print("✓ Database connection closed")


if __name__ == "__main__":
    aggregator = StreamAggregator(window_size_seconds=60)
    aggregator.connect_kafka()
    aggregator.connect_database()
    aggregator.run()
