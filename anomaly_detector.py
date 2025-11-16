"""
Anomaly Detection Model for Stock Trading Data
Uses Isolation Forest and statistical methods to detect unusual trading patterns
"""
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import pickle
import os
from database import get_connection


class TradingAnomalyDetector:
    """
    Detects anomalies in stock trading patterns using multiple approaches:
    1. Isolation Forest for multivariate anomaly detection
    2. Statistical outlier detection (Z-score)
    3. Sequential pattern analysis
    """
    
    def __init__(self, contamination=0.05):
        self.contamination = contamination
        self.models = {}  # One model per symbol
        self.scalers = {}
        self.stats = {}  # Statistical parameters per symbol
        
    def prepare_features(self, df):
        """Extract features from trading data"""
        features = pd.DataFrame()
        
        # Basic features
        features['price'] = df['price']
        features['volume'] = df['volume']
        features['total_value'] = df['total_value']
        
        # Price-based features
        features['price_change'] = df['price'].pct_change()
        features['price_volatility'] = df['price'].rolling(window=10, min_periods=1).std()
        
        # Volume-based features
        features['volume_change'] = df['volume'].pct_change()
        features['volume_ma'] = df['volume'].rolling(window=10, min_periods=1).mean()
        features['volume_ratio'] = df['volume'] / features['volume_ma']
        
        # Time-based features
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        features['hour'] = df['timestamp'].dt.hour
        features['minute'] = df['timestamp'].dt.minute
        
        # Sequential features (looking back)
        features['price_ma_5'] = df['price'].rolling(window=5, min_periods=1).mean()
        features['price_ma_10'] = df['price'].rolling(window=10, min_periods=1).mean()
        features['price_deviation_from_ma'] = (df['price'] - features['price_ma_10']) / features['price_ma_10']
        
        # Replace inf and nan
        features = features.replace([np.inf, -np.inf], np.nan)
        features = features.fillna(0)
        
        return features
    
    def train_model(self, symbol):
        """Train anomaly detection model for a specific symbol"""
        conn = get_connection()
        
        # Fetch historical data for the symbol
        query = """
            SELECT timestamp, price, volume, total_value
            FROM trades
            WHERE symbol = %s
            ORDER BY timestamp ASC
            LIMIT 1000
        """
        
        df = pd.read_sql_query(query, conn, params=(symbol,))
        conn.close()
        
        if len(df) < 20:
            print(f"⚠️ Not enough data to train model for {symbol}")
            return False
        
        # Prepare features
        features = self.prepare_features(df)
        
        # Scale features
        scaler = StandardScaler()
        features_scaled = scaler.fit_transform(features)
        
        # Train Isolation Forest
        model = IsolationForest(
            contamination=self.contamination,
            random_state=42,
            n_estimators=100
        )
        model.fit(features_scaled)
        
        # Store model and scaler
        self.models[symbol] = model
        self.scalers[symbol] = scaler
        
        # Store statistical parameters
        self.stats[symbol] = {
            'price_mean': df['price'].mean(),
            'price_std': df['price'].std(),
            'volume_mean': df['volume'].mean(),
            'volume_std': df['volume'].std()
        }
        
        print(f"✓ Trained anomaly detection model for {symbol}")
        return True
    
    def detect_anomalies(self, symbol, recent_trades):
        """
        Detect anomalies in recent trades
        Returns: list of (trade_index, anomaly_score, anomaly_type)
        """
        if symbol not in self.models:
            return []
        
        df = pd.DataFrame(recent_trades)
        if len(df) == 0:
            return []
        
        features = self.prepare_features(df)
        features_scaled = self.scalers[symbol].transform(features)
        
        # Isolation Forest predictions
        predictions = self.models[symbol].predict(features_scaled)
        anomaly_scores = self.models[symbol].score_samples(features_scaled)
        
        # Statistical anomaly detection
        stats = self.stats[symbol]
        price_z_scores = np.abs((df['price'] - stats['price_mean']) / stats['price_std'])
        volume_z_scores = np.abs((df['volume'] - stats['volume_mean']) / stats['volume_std'])
        
        anomalies = []
        for i in range(len(df)):
            is_anomaly = False
            anomaly_type = []
            
            # Check Isolation Forest
            if predictions[i] == -1:
                is_anomaly = True
                anomaly_type.append('pattern')
            
            # Check statistical outliers
            if price_z_scores[i] > 3:
                is_anomaly = True
                anomaly_type.append('price')
            
            if volume_z_scores[i] > 3:
                is_anomaly = True
                anomaly_type.append('volume')
            
            if is_anomaly:
                anomalies.append({
                    'index': i,
                    'score': float(anomaly_scores[i]),
                    'types': anomaly_type,
                    'price_z': float(price_z_scores[i]),
                    'volume_z': float(volume_z_scores[i])
                })
        
        return anomalies
    
    def save_models(self, filepath='models/anomaly_models.pkl'):
        """Save trained models to disk"""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        model_data = {
            'models': self.models,
            'scalers': self.scalers,
            'stats': self.stats,
            'contamination': self.contamination
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f"✓ Models saved to {filepath}")
    
    def load_models(self, filepath='models/anomaly_models.pkl'):
        """Load trained models from disk"""
        if not os.path.exists(filepath):
            print(f"⚠️ Model file not found: {filepath}")
            return False
        
        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)
        
        self.models = model_data['models']
        self.scalers = model_data['scalers']
        self.stats = model_data['stats']
        self.contamination = model_data['contamination']
        
        print(f"✓ Models loaded from {filepath}")
        print(f"  Loaded {len(self.models)} models for symbols: {list(self.models.keys())}")
        return True


def train_all_models():
    """Train anomaly detection models for all symbols"""
    print("\n" + "="*60)
    print("🤖 TRAINING ANOMALY DETECTION MODELS")
    print("="*60 + "\n")
    
    # Get all unique symbols
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT symbol FROM trades ORDER BY symbol")
    symbols = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    
    if not symbols:
        print("⚠️ No trading data found. Run the producer and consumer first.")
        return
    
    print(f"Found {len(symbols)} symbols: {', '.join(symbols)}\n")
    
    # Train models
    detector = TradingAnomalyDetector()
    
    for symbol in symbols:
        detector.train_model(symbol)
    
    # Save models
    detector.save_models()
    
    print("\n" + "="*60)
    print("✓ Training complete!")
    print("="*60)


def analyze_recent_trades(symbol=None, limit=100):
    """Analyze recent trades for anomalies"""
    print("\n" + "="*60)
    print("🔍 ANALYZING RECENT TRADES FOR ANOMALIES")
    print("="*60 + "\n")
    
    # Load models
    detector = TradingAnomalyDetector()
    if not detector.load_models():
        print("No trained models found. Run train_all_models() first.")
        return
    
    # Fetch recent trades
    conn = get_connection()
    
    if symbol:
        query = """
            SELECT trade_id, timestamp, symbol, price, volume, total_value
            FROM trades
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT %s
        """
        df = pd.read_sql_query(query, conn, params=(symbol, limit))
    else:
        query = """
            SELECT trade_id, timestamp, symbol, price, volume, total_value
            FROM trades
            ORDER BY timestamp DESC
            LIMIT %s
        """
        df = pd.read_sql_query(query, conn, params=(limit,))
    
    conn.close()
    
    if df.empty:
        print("No trades found.")
        return
    
    # Group by symbol and analyze
    anomaly_count = 0
    
    for sym in df['symbol'].unique():
        if sym not in detector.models:
            print(f"⚠️ No model for {sym}, skipping...")
            continue
        
        symbol_trades = df[df['symbol'] == sym].to_dict('records')
        anomalies = detector.detect_anomalies(sym, symbol_trades)
        
        if anomalies:
            print(f"\n{sym}: Found {len(anomalies)} anomalies")
            for anom in anomalies[:5]:  # Show first 5
                trade = symbol_trades[anom['index']]
                print(f"  ⚠️ Trade {trade['trade_id']}")
                print(f"     Price: ${trade['price']:.2f} (Z-score: {anom['price_z']:.2f})")
                print(f"     Volume: {trade['volume']:,} (Z-score: {anom['volume_z']:.2f})")
                print(f"     Types: {', '.join(anom['types'])}")
            
            anomaly_count += len(anomalies)
    
    print(f"\n{'='*60}")
    print(f"📊 Total anomalies detected: {anomaly_count}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'train':
            train_all_models()
        elif sys.argv[1] == 'analyze':
            symbol = sys.argv[2] if len(sys.argv) > 2 else None
            analyze_recent_trades(symbol)
        else:
            print("Usage:")
            print("  python anomaly_detector.py train          - Train models")
            print("  python anomaly_detector.py analyze [SYMBOL] - Analyze trades")
    else:
        print("Usage:")
        print("  python anomaly_detector.py train          - Train models")
        print("  python anomaly_detector.py analyze [SYMBOL] - Analyze trades")
