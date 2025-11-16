"""
System Test Script
Validates that all components are working correctly
"""
import os
import time
import subprocess
import sys
from dotenv import load_dotenv

load_dotenv()


def print_header(text):
    """Print a formatted header"""
    print("\n" + "="*60)
    print(f"  {text}")
    print("="*60 + "\n")


def check_docker_services():
    """Check if Docker services are running"""
    print_header("Checking Docker Services")
    
    try:
        result = subprocess.run(
            ["docker-compose", "ps"],
            capture_output=True,
            text=True,
            check=True
        )
        print(result.stdout)
        
        # Check if all services are running
        required_services = ['zookeeper', 'kafka', 'postgres']
        output = result.stdout.lower()
        
        all_running = all(service in output for service in required_services)
        
        if all_running:
            print("✓ All required Docker services are running")
            return True
        else:
            print("✗ Some Docker services are not running")
            print("  Run: docker-compose up -d")
            return False
            
    except subprocess.CalledProcessError as e:
        print("✗ Error checking Docker services")
        print("  Make sure Docker is installed and running")
        return False


def check_kafka_connection():
    """Check Kafka connectivity"""
    print_header("Checking Kafka Connection")
    
    try:
        from kafka import KafkaProducer
        from kafka.errors import KafkaError
        
        bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=5000
        )
        
        producer.close()
        print(f"✓ Successfully connected to Kafka at {bootstrap_servers}")
        return True
        
    except KafkaError as e:
        print(f"✗ Failed to connect to Kafka: {e}")
        print("  Make sure Kafka is running: docker-compose up -d")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def check_postgres_connection():
    """Check PostgreSQL connectivity"""
    print_header("Checking PostgreSQL Connection")
    
    try:
        import psycopg2
        from database import get_db_config
        
        config = get_db_config()
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        
        # Test query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"✓ Successfully connected to PostgreSQL")
        print(f"  Version: {version.split(',')[0]}")
        return True
        
    except Exception as e:
        print(f"✗ Failed to connect to PostgreSQL: {e}")
        print("  Make sure PostgreSQL is running: docker-compose up -d")
        return False


def check_database_tables():
    """Check if database tables exist"""
    print_header("Checking Database Tables")
    
    try:
        import psycopg2
        from database import get_connection
        
        conn = get_connection()
        cursor = conn.cursor()
        
        # Check for tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        required_tables = ['trades', 'stock_metrics']
        
        if all(table in tables for table in required_tables):
            print(f"✓ All required tables exist: {', '.join(tables)}")
            return True
        else:
            print(f"⚠ Some tables missing. Found: {', '.join(tables) if tables else 'none'}")
            print("  Run: python database.py")
            return False
            
    except Exception as e:
        print(f"✗ Error checking tables: {e}")
        return False


def check_trade_data():
    """Check if there's any trade data"""
    print_header("Checking Trade Data")
    
    try:
        import psycopg2
        from database import get_connection
        
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM trades;")
        count = cursor.fetchone()[0]
        
        if count > 0:
            cursor.execute("""
                SELECT symbol, COUNT(*) as trade_count 
                FROM trades 
                GROUP BY symbol 
                ORDER BY trade_count DESC;
            """)
            symbol_counts = cursor.fetchall()
            
            print(f"✓ Database contains {count:,} trades")
            print("\nTrades by symbol:")
            for symbol, trade_count in symbol_counts:
                print(f"  {symbol}: {trade_count:,} trades")
        else:
            print("⚠ No trade data found")
            print("  Start the producer: python producer.py")
            print("  Start the consumer: python consumer.py")
        
        cursor.close()
        conn.close()
        
        return count > 0
        
    except Exception as e:
        print(f"✗ Error checking trade data: {e}")
        return False


def check_python_packages():
    """Check if required Python packages are installed"""
    print_header("Checking Python Packages")
    
    required_packages = [
        'kafka',
        'psycopg2',
        'streamlit',
        'pandas',
        'plotly',
        'numpy',
        'sklearn',
        'dotenv'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✓ {package}")
        except ImportError:
            print(f"✗ {package} - MISSING")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n⚠ Missing packages: {', '.join(missing_packages)}")
        print("  Run: pip install -r requirements.txt")
        return False
    else:
        print("\n✓ All required packages are installed")
        return True


def run_all_tests():
    """Run all system tests"""
    print("\n" + "="*60)
    print("  STOCK MARKET STREAMING SYSTEM - HEALTH CHECK")
    print("="*60)
    
    results = {
        "Python Packages": check_python_packages(),
        "Docker Services": check_docker_services(),
        "Kafka Connection": check_kafka_connection(),
        "PostgreSQL Connection": check_postgres_connection(),
        "Database Tables": check_database_tables(),
        "Trade Data": check_trade_data()
    }
    
    # Summary
    print_header("Test Summary")
    
    passed = sum(results.values())
    total = len(results)
    
    for test, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status:10} - {test}")
    
    print("\n" + "-"*60)
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 System is fully operational!")
        print("\nYou can now:")
        print("  1. Run the producer: python producer.py")
        print("  2. Run the consumer: python consumer.py")
        print("  3. View dashboard: streamlit run dashboard.py")
        print("  4. Run aggregator: python aggregator.py (optional)")
    else:
        print("\n⚠️ System is not ready. Please fix the failing tests above.")
        sys.exit(1)


if __name__ == "__main__":
    run_all_tests()
