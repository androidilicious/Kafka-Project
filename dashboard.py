"""
Real-Time Stock Market Dashboard
Interactive Streamlit dashboard for visualizing live stock trading data
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Real-Time Stock Market Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .anomaly-badge {
        background-color: #ff4b4b;
        color: white;
        padding: 0.2rem 0.5rem;
        border-radius: 0.3rem;
        font-size: 0.8rem;
    }
    </style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_db_engine():
    """Create a cached SQLAlchemy engine"""
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    database = os.getenv('POSTGRES_DB', 'stock_market_db')
    user = os.getenv('POSTGRES_USER', 'kafka_user')
    password = os.getenv('POSTGRES_PASSWORD', 'kafka_password')
    
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(connection_string)


def fetch_recent_trades(limit=100):
    """Fetch the most recent trades"""
    engine = get_db_engine()
    query = f"""
        SELECT trade_id, timestamp, symbol, price, volume, 
               trade_type, total_value, is_anomaly
        FROM trades
        ORDER BY timestamp DESC
        LIMIT {limit}
    """
    df = pd.read_sql_query(query, engine)
    return df


def fetch_stock_summary(hours=None):
    """Fetch summary statistics by stock symbol"""
    engine = get_db_engine()
    if hours:
        time_filter = f"WHERE timestamp > NOW() - INTERVAL '{hours} hours'"
    else:
        time_filter = ""
    
    query = f"""
        SELECT 
            symbol,
            COUNT(*) as trade_count,
            AVG(price) as avg_price,
            MIN(price) as min_price,
            MAX(price) as max_price,
            SUM(volume) as total_volume,
            SUM(total_value) as total_value,
            SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomaly_count,
            MAX(timestamp) as last_trade
        FROM trades
        {time_filter}
        GROUP BY symbol
        ORDER BY total_value DESC
    """
    df = pd.read_sql_query(query, engine)
    return df


def fetch_time_series(symbol, hours=None):
    """Fetch time series data for a specific symbol"""
    engine = get_db_engine()
    if hours:
        time_filter = f"AND timestamp > NOW() - INTERVAL '{hours} hours'"
    else:
        time_filter = ""
    
    query = f"""
        SELECT timestamp, price, volume, is_anomaly
        FROM trades
        WHERE symbol = '{symbol}' 
        {time_filter}
        ORDER BY timestamp ASC
    """
    df = pd.read_sql_query(query, engine)
    return df


def fetch_overall_metrics(hours=None):
    """Fetch overall system metrics"""
    engine = get_db_engine()
    if hours:
        time_filter = f"WHERE timestamp > NOW() - INTERVAL '{hours} hours'"
    else:
        time_filter = ""
    
    query = f"""
        SELECT 
            COUNT(*) as total_trades,
            SUM(volume) as total_volume,
            SUM(total_value) as total_value,
            AVG(price) as avg_price,
            COUNT(DISTINCT symbol) as active_symbols,
            SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomaly_count,
            MAX(timestamp) as last_update
        FROM trades
        {time_filter}
    """
    df = pd.read_sql_query(query, engine)
    return df.iloc[0] if not df.empty else None


def main():
    """Main dashboard function"""
    
    # Header
    st.markdown('<div class="main-header">📈 Real-Time Stock Market Dashboard</div>', 
                unsafe_allow_html=True)
    
    # Sidebar
    st.sidebar.title("⚙️ Settings")
    refresh_interval = st.sidebar.slider(
        "Refresh Interval (seconds)", 
        min_value=1, 
        max_value=10, 
        value=2
    )
    
    trade_limit = st.sidebar.selectbox(
        "Recent Trades to Display",
        [50, 100, 200, 500],
        index=1,
        key="trade_limit_select"
    )
    
    time_window = st.sidebar.selectbox(
        "Time Window (hours)",
        ["All Data", 1, 3, 6, 12, 24],
        index=0,
        key="time_window_select"
    )
    
    show_anomalies_only = st.sidebar.checkbox("Show Anomalies Only", False)
    
    st.sidebar.markdown("---")
    st.sidebar.info(
        "🔄 Dashboard auto-refreshes every " + str(refresh_interval) + " seconds\n\n"
        "📊 Data Source: PostgreSQL\n\n"
        "🚀 Stream: Apache Kafka"
    )
    
    # Fetch data
    hours_filter = None if time_window == "All Data" else time_window
    metrics = fetch_overall_metrics(hours_filter)
    
    if metrics is not None and metrics['total_trades'] > 0:
        # Top metrics row
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                "Total Trades",
                f"{int(metrics['total_trades']):,}",
                delta=None
            )
        
        with col2:
            st.metric(
                "Total Volume",
                f"{int(metrics['total_volume']):,}",
                delta=None
            )
        
        with col3:
            st.metric(
                "Total Value",
                f"${metrics['total_value']:,.0f}",
                delta=None
            )
        
        with col4:
            st.metric(
                "Active Symbols",
                f"{int(metrics['active_symbols'])}",
                delta=None
            )
        
        with col5:
            st.metric(
                "Anomalies",
                f"{int(metrics['anomaly_count'])}",
                delta=None,
                delta_color="inverse"
            )
        
        st.markdown("---")
        
        # Stock summary table
        st.subheader("📊 Stock Performance Summary")
        summary_df = fetch_stock_summary(hours_filter)
        
        if not summary_df.empty:
            # Format the dataframe
            summary_display = summary_df.copy()
            summary_display['avg_price'] = summary_display['avg_price'].apply(lambda x: f"${x:.2f}")
            summary_display['min_price'] = summary_display['min_price'].apply(lambda x: f"${x:.2f}")
            summary_display['max_price'] = summary_display['max_price'].apply(lambda x: f"${x:.2f}")
            summary_display['total_volume'] = summary_display['total_volume'].apply(lambda x: f"{x:,}")
            summary_display['total_value'] = summary_display['total_value'].apply(lambda x: f"${x:,.2f}")
            
            st.dataframe(
                summary_display,
                use_container_width=True,
                hide_index=True
            )
            
            # Visualizations
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("💰 Total Value by Symbol")
                fig_value = px.bar(
                    summary_df,
                    x='symbol',
                    y='total_value',
                    color='total_value',
                    color_continuous_scale='Blues',
                    labels={'total_value': 'Total Value ($)', 'symbol': 'Symbol'}
                )
                fig_value.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig_value, use_container_width=True)
            
            with col2:
                st.subheader("📊 Trading Volume Distribution")
                fig_volume = px.pie(
                    summary_df,
                    values='total_volume',
                    names='symbol',
                    hole=0.4
                )
                fig_volume.update_layout(height=400)
                st.plotly_chart(fig_volume, use_container_width=True)
            
            # Price trends
            st.subheader("📈 Stock Price Trends")
            selected_symbol = st.selectbox(
                "Select Symbol",
                summary_df['symbol'].tolist(),
                key="symbol_select"
            )
            
            if selected_symbol:
                ts_df = fetch_time_series(selected_symbol, hours_filter)
                
                if not ts_df.empty:
                    fig_price = go.Figure()
                    
                    # Add price line
                    fig_price.add_trace(go.Scatter(
                        x=ts_df['timestamp'],
                        y=ts_df['price'],
                        mode='lines+markers',
                        name='Price',
                        line=dict(color='blue', width=2),
                        marker=dict(size=4)
                    ))
                    
                    # Highlight anomalies
                    anomalies = ts_df[ts_df['is_anomaly'] == True]
                    if not anomalies.empty:
                        fig_price.add_trace(go.Scatter(
                            x=anomalies['timestamp'],
                            y=anomalies['price'],
                            mode='markers',
                            name='Anomaly',
                            marker=dict(
                                size=12,
                                color='red',
                                symbol='star',
                                line=dict(width=2, color='darkred')
                            )
                        ))
                    
                    fig_price.update_layout(
                        title=f"{selected_symbol} Price History",
                        xaxis_title="Time",
                        yaxis_title="Price ($)",
                        height=400,
                        hovermode='x unified'
                    )
                    
                    st.plotly_chart(fig_price, use_container_width=True)
                    
                    # Volume chart
                    fig_vol = go.Figure()
                    fig_vol.add_trace(go.Bar(
                        x=ts_df['timestamp'],
                        y=ts_df['volume'],
                        name='Volume',
                        marker_color='lightblue'
                    ))
                    
                    fig_vol.update_layout(
                        title=f"{selected_symbol} Trading Volume",
                        xaxis_title="Time",
                        yaxis_title="Volume",
                        height=300
                    )
                    
                    st.plotly_chart(fig_vol, use_container_width=True)
        
        # Recent trades table
        st.markdown("---")
        st.subheader("🔄 Recent Trades")
        
        trades_df = fetch_recent_trades(trade_limit)
        
        if not trades_df.empty:
            if show_anomalies_only:
                trades_df = trades_df[trades_df['is_anomaly'] == True]
            
            # Format display
            trades_display = trades_df.copy()
            trades_display['price'] = trades_display['price'].apply(lambda x: f"${x:.2f}")
            trades_display['volume'] = trades_display['volume'].apply(lambda x: f"{x:,}")
            trades_display['total_value'] = trades_display['total_value'].apply(lambda x: f"${x:,.2f}")
            trades_display['is_anomaly'] = trades_display['is_anomaly'].apply(
                lambda x: "⚠️ Yes" if x else "No"
            )
            
            st.dataframe(
                trades_display,
                use_container_width=True,
                hide_index=True,
                height=400
            )
        else:
            st.info("No trades to display. Make sure the producer and consumer are running.")
    
    else:
        st.warning("⏳ Waiting for data... Please ensure the producer and consumer are running.")
        st.info(
            "To start the system:\n"
            "1. Run: `docker-compose up -d`\n"
            "2. Run: `python producer.py`\n"
            "3. Run: `python consumer.py`"
        )
    
    # Last update time
    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Auto-refresh
    time.sleep(refresh_interval)
    st.rerun()


if __name__ == "__main__":
    main()
