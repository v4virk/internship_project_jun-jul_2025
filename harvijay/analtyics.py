import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time
import os

# Constants
HDFS_BASE_PATH = "hdfs:///user/harvijaysingh/events"
EVENT_TYPES = ['login', 'view_product', 'add_to_cart', 'purchase', 'logout', 'search']
PRODUCT_CATEGORIES = ['electronics', 'clothing', 'home', 'books', 'beauty']

# Initialize Spark Session with error handling
@st.cache_resource
def init_spark():
    try:
        spark = SparkSession.builder \
            .appName("EcommerceAnalytics") \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .config("spark.sql.parquet.binaryAsString", "true") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        return spark
    except Exception as e:
        st.error(f"Spark initialization failed: {str(e)}")
        return None

# Data loading with multiple fallback strategies
@st.cache_data(ttl=60)
def load_data(spark):
    try:
        # Try reading with Spark
        df = spark.read.parquet(HDFS_BASE_PATH)
        
        # Convert timestamp if exists
        if 'timestamp' in df.columns:
            try:
                df = df.withColumn('timestamp', F.from_unixtime(F.col('timestamp')))
                df = df.withColumn('timestamp', F.to_timestamp('timestamp'))
            except:
                try:
                    df = df.withColumn('timestamp', F.to_timestamp('timestamp'))
                except:
                    pass
        
        # Convert to Pandas with multiple fallbacks
        try:
            return df.toPandas()
        except:
            try:
                # Fallback to Arrow conversion
                return df.toPandas(use_arrow=True)
            except:
                # Final fallback to collect()
                return pd.DataFrame(df.collect(), columns=df.columns)
                
    except Exception as e:
        st.error(f"Data loading error: {str(e)}")
        return pd.DataFrame()

# Journey classification
def classify_journey(events):
    events = [str(e).lower() for e in events if pd.notna(e)]
    if 'login' in events:
        if 'purchase' in events:
            return 'Completed Purchase'
        elif 'add_to_cart' in events:
            return 'Abandoned Cart'
        return 'Browsing Only'
    return 'Other'

# Visualization functions
def plot_journey_funnel(data, ax):
    user_paths = data.groupby('user_id')['event_type'].apply(list).reset_index()
    user_paths['journey_type'] = user_paths['event_type'].apply(classify_journey)
    journey_counts = user_paths['journey_type'].value_counts()
    sns.barplot(x=journey_counts.values, y=journey_counts.index, 
                palette='viridis', ax=ax)
    ax.set_xlabel('User Count')
    ax.set_title('User Journey Funnel')

def plot_event_frequency(data, ax):
    event_counts = data['event_type'].value_counts()
    event_counts.plot(kind='bar', color='teal', ax=ax)
    ax.set_ylabel('Count')
    ax.set_title('Event Frequency')

def plot_product_categories(data, ax):
    if 'product_category' in data.columns:
        category_counts = data['product_category'].value_counts()
        category_counts.plot(kind='pie', autopct='%1.1f%%', ax=ax)
        ax.set_title('Product Categories')
    else:
        ax.text(0.5, 0.5, 'No category data', ha='center', va='center')

def plot_events_over_time(data, ax):
    if 'timestamp' in data.columns:
        time_series = data.set_index('timestamp').resample('5min').size()
        time_series.plot(marker='o', color='orange', ax=ax)
        ax.set_ylabel('Events')
        ax.set_title('Events Over Time')
    else:
        ax.text(0.5, 0.5, 'No timestamp data', ha='center', va='center')

def plot_top_users(data, ax):
    top_users = data['user_id'].value_counts().nlargest(5)
    if not top_users.empty:
        sns.barplot(x=top_users.values, y=top_users.index, ax=ax)
        ax.set_xlabel('Event Count')
        ax.set_title('Top 5 Active Users')
    else:
        ax.text(0.5, 0.5, 'No user data', ha='center', va='center')

def plot_category_event_heatmap(data, ax):
    if 'product_category' in data.columns:
        category_event_matrix = pd.crosstab(data['product_category'], data['event_type'])
        sns.heatmap(category_event_matrix, annot=True, fmt='d', cmap='YlGnBu', ax=ax)
        ax.set_title('Category vs Event Type')
    else:
        ax.text(0.5, 0.5, 'No category data', ha='center', va='center')

def plot_event_type_trend(data, ax):
    if 'timestamp' in data.columns:
        event_time_series = data.set_index('timestamp').groupby(
            [pd.Grouper(freq='15min'), 'event_type']).size().unstack(fill_value=0)
        if len(event_time_series) > 1:
            event_time_series.plot.area(ax=ax, stacked=True, alpha=0.6)
            ax.set_ylabel('Count')
            ax.set_title('Event Type Over Time')
        else:
            ax.text(0.5, 0.5, 'Not enough data points', ha='center', va='center')
    else:
        ax.text(0.5, 0.5, 'No timestamp data', ha='center', va='center')

def plot_category_trend(data, ax):
    if 'timestamp' in data.columns and 'product_category' in data.columns:
        cat_time_series = data.set_index('timestamp').groupby(
            [pd.Grouper(freq='15min'), 'product_category']).size().unstack(fill_value=0)
        if len(cat_time_series) > 1:
            cat_time_series.plot(ax=ax, marker='o')
            ax.set_ylabel('Count')
            ax.set_title('Product Categories Over Time')
        else:
            ax.text(0.5, 0.5, 'Not enough data points', ha='center', va='center')
    else:
        ax.text(0.5, 0.5, 'Missing required columns', ha='center', va='center')

# Main dashboard function
def main():
    st.set_page_config(layout="wide", page_title="E-commerce Analytics")
    st.title("📊 E-commerce Analytics Dashboard")
    
    # Initialize Spark
    spark = init_spark()
    if spark is None:
        return
    
    # Sidebar controls
    with st.sidebar:
        st.header("Dashboard Controls")
        time_window = st.selectbox(
            "Analysis Time Window",
            ["30min", "1h", "4h", "12h", "24h", "7d", "All"],
            index=4
        )
        auto_refresh = st.checkbox("Enable Auto-Refresh", True)
        refresh_rate = st.slider("Refresh Rate (seconds)", 5, 60, 30)
        st.markdown("---")
        st.markdown("**Data Status**")
        data_status = st.empty()
    
    # Load data
    data = load_data(spark)
    if data.empty:
        st.warning("No data loaded - please check your HDFS connection")
        return
    
    # Filter by time window if not "All"
    if time_window != "All" and 'timestamp' in data.columns:
        try:
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            if time_window.endswith('min'):
                minutes = int(time_window[:-3])
                threshold = datetime.now() - timedelta(minutes=minutes)
            elif time_window.endswith('h'):
                hours = int(time_window[:-1])
                threshold = datetime.now() - timedelta(hours=hours)
            elif time_window.endswith('d'):
                days = int(time_window[:-1])
                threshold = datetime.now() - timedelta(days=days)
            
            recent_data = data[data['timestamp'] > threshold]
        except:
            recent_data = data
            st.warning("Could not filter by time window - showing all data")
    else:
        recent_data = data
    
    # Update data status
    data_status.markdown(f"""
    - **Total Events**: {len(data):,}
    - **Recent Events**: {len(recent_data):,}
    - **Time Window**: {time_window}
    """)
    
    # Create visualizations in a 4x2 grid
    fig, axes = plt.subplots(4, 2, figsize=(18, 20))
    plt.subplots_adjust(hspace=0.5)
    
    try:
        plot_journey_funnel(recent_data, axes[0, 0])
        plot_event_frequency(recent_data, axes[0, 1])
        plot_product_categories(recent_data, axes[1, 0])
        plot_events_over_time(recent_data, axes[1, 1])
        plot_top_users(recent_data, axes[2, 0])
        plot_category_event_heatmap(recent_data, axes[2, 1])
        plot_event_type_trend(recent_data, axes[3, 0])
        plot_category_trend(recent_data, axes[3, 1])
        
        st.pyplot(fig)
    except Exception as e:
        st.error(f"Visualization error: {str(e)}")
    
    # Auto-refresh logic
    if auto_refresh:
        time.sleep(refresh_rate)
        st.rerun()

if __name__ == "__main__":
    # Add environment variables if needed
    if 'SPARK_HOME' not in os.environ:
        os.environ['SPARK_HOME'] = '/usr/local/spark'  # Update with your Spark path
    if 'JAVA_HOME' not in os.environ:
        os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-amd64'  # Update with your Java path
    
    main()