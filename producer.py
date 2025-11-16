"""
Stock Market Data Producer
Generates synthetic stock trading events and streams them to Kafka
"""
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import os
from dotenv import load_dotenv

load_dotenv()

# Stock symbols and their base prices
STOCKS = {
    'AAPL': 175.00,
    'GOOGL': 140.00,
    'MSFT': 380.00,
    'AMZN': 155.00,
    'TSLA': 240.00,
    'META': 350.00,
    'NVDA': 490.00,
    'JPM': 150.00,
    'V': 260.00,
    'WMT': 165.00
}

TRADE_TYPES = ['BUY', 'SELL']


class StockDataProducer:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'stock-trades')
        self.interval = float(os.getenv('PRODUCER_INTERVAL', '0.5'))
        self.producer = None
        self.stock_prices = STOCKS.copy()
        
    def connect(self):
        """Connect to Kafka broker"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3
            )
            print(f"✓ Connected to Kafka at {self.bootstrap_servers}")
            print(f"✓ Publishing to topic: {self.topic}")
        except KafkaError as e:
            print(f"✗ Failed to connect to Kafka: {e}")
            raise
    
    def generate_trade(self):
        """Generate a synthetic stock trade event"""
        symbol = random.choice(list(self.stock_prices.keys()))
        
        # Simulate price movement with random walk
        price_change_pct = random.gauss(0, 0.02)  # Mean 0, std 2%
        self.stock_prices[symbol] *= (1 + price_change_pct)
        
        # Ensure price doesn't deviate too much from base
        if abs(self.stock_prices[symbol] - STOCKS[symbol]) > STOCKS[symbol] * 0.3:
            self.stock_prices[symbol] = STOCKS[symbol] * (1 + random.uniform(-0.3, 0.3))
        
        price = round(self.stock_prices[symbol], 2)
        volume = random.randint(100, 10000)
        trade_type = random.choice(TRADE_TYPES)
        
        # Occasionally generate anomalies
        is_anomaly = random.random() < 0.05
        if is_anomaly:
            volume = random.randint(50000, 100000)  # Unusual volume
            price_change_pct = random.uniform(0.05, 0.15)  # Large price movement
            self.stock_prices[symbol] *= (1 + price_change_pct)
            price = round(self.stock_prices[symbol], 2)
        
        trade = {
            'trade_id': f"TRD{int(time.time() * 1000000)}",
            'timestamp': datetime.now().isoformat(),
            'symbol': symbol,
            'price': price,
            'volume': volume,
            'trade_type': trade_type,
            'total_value': round(price * volume, 2),
            'is_anomaly': is_anomaly
        }
        
        return trade
    
    def send_trade(self, trade):
        """Send trade event to Kafka"""
        try:
            future = self.producer.send(
                self.topic,
                key=trade['symbol'],
                value=trade
            )
            # Wait for the send to complete
            record_metadata = future.get(timeout=10)
            return True
        except KafkaError as e:
            print(f"✗ Failed to send trade: {e}")
            return False
    
    def run(self):
        """Main producer loop"""
        print("\n" + "="*60)
        print("🚀 STOCK MARKET DATA PRODUCER STARTED")
        print("="*60)
        print(f"Interval: {self.interval}s between trades")
        print(f"Tracking {len(STOCKS)} stocks: {', '.join(STOCKS.keys())}")
        print("Press Ctrl+C to stop\n")
        
        trade_count = 0
        try:
            while True:
                trade = self.generate_trade()
                if self.send_trade(trade):
                    trade_count += 1
                    status = "⚠️ ANOMALY" if trade['is_anomaly'] else "✓"
                    print(f"{status} [{trade_count}] {trade['symbol']} | "
                          f"${trade['price']} | Vol: {trade['volume']:,} | "
                          f"{trade['trade_type']} | Value: ${trade['total_value']:,.2f}")
                
                time.sleep(self.interval)
                
        except KeyboardInterrupt:
            print(f"\n\n📊 Total trades generated: {trade_count}")
            print("Shutting down producer...")
        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()
                print("✓ Producer closed successfully")


if __name__ == "__main__":
    producer = StockDataProducer()
    producer.connect()
    producer.run()
