"""
Stock Market Data Consumer
Consumes stock trading events from Kafka and stores them in PostgreSQL
"""
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import psycopg2
from datetime import datetime
import os
from dotenv import load_dotenv
from database import get_connection, create_tables

load_dotenv()


class StockDataConsumer:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'stock-trades')
        self.consumer = None
        self.db_conn = None
        self.db_cursor = None
        
    def connect_kafka(self):
        """Connect to Kafka broker"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset='latest',  # Start from latest messages
                enable_auto_commit=True,
                group_id='stock-consumer-group'
            )
            print(f"✓ Connected to Kafka at {self.bootstrap_servers}")
            print(f"✓ Subscribed to topic: {self.topic}")
        except KafkaError as e:
            print(f"✗ Failed to connect to Kafka: {e}")
            raise
    
    def connect_database(self):
        """Connect to PostgreSQL database"""
        try:
            # Ensure tables exist
            create_tables()
            
            self.db_conn = get_connection()
            self.db_cursor = self.db_conn.cursor()
            print("✓ Connected to PostgreSQL database")
        except Exception as e:
            print(f"✗ Failed to connect to database: {e}")
            raise
    
    def insert_trade(self, trade):
        """Insert trade into database"""
        try:
            # Parse timestamp
            timestamp = datetime.fromisoformat(trade['timestamp'])
            
            insert_query = """
                INSERT INTO trades 
                (trade_id, timestamp, symbol, price, volume, trade_type, total_value, is_anomaly)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trade_id) DO NOTHING
            """
            
            self.db_cursor.execute(insert_query, (
                trade['trade_id'],
                timestamp,
                trade['symbol'],
                trade['price'],
                trade['volume'],
                trade['trade_type'],
                trade['total_value'],
                trade.get('is_anomaly', False)
            ))
            
            self.db_conn.commit()
            return True
            
        except Exception as e:
            print(f"✗ Error inserting trade: {e}")
            self.db_conn.rollback()
            return False
    
    def run(self):
        """Main consumer loop"""
        print("\n" + "="*60)
        print("🔄 STOCK MARKET DATA CONSUMER STARTED")
        print("="*60)
        print("Waiting for messages...\n")
        
        trade_count = 0
        anomaly_count = 0
        
        try:
            for message in self.consumer:
                trade = message.value
                
                if self.insert_trade(trade):
                    trade_count += 1
                    if trade.get('is_anomaly', False):
                        anomaly_count += 1
                        status = "⚠️ ANOMALY"
                    else:
                        status = "✓"
                    
                    print(f"{status} [{trade_count}] Stored: {trade['symbol']} | "
                          f"${trade['price']} | Vol: {trade['volume']:,} | "
                          f"Value: ${trade['total_value']:,.2f}")
                    
                    if trade_count % 100 == 0:
                        print(f"📊 Stats: {trade_count} trades processed "
                              f"({anomaly_count} anomalies detected)")
                
        except KeyboardInterrupt:
            print(f"\n\n📊 Final Statistics:")
            print(f"   Total trades processed: {trade_count}")
            print(f"   Anomalies detected: {anomaly_count}")
            print("\nShutting down consumer...")
        finally:
            if self.consumer:
                self.consumer.close()
                print("✓ Kafka consumer closed")
            if self.db_cursor:
                self.db_cursor.close()
            if self.db_conn:
                self.db_conn.close()
                print("✓ Database connection closed")


if __name__ == "__main__":
    consumer = StockDataConsumer()
    consumer.connect_kafka()
    consumer.connect_database()
    consumer.run()
