import streamlit as st
import pandas as pd
import altair as alt
import json
import time
from datetime import datetime, timedelta

# Path to your JSON file
EVENTS_FILE = '/Users/harvijaysingh/events_log.json'

# Function to load events from the JSON file
@st.cache_data(ttl=5)
def load_events():
    events = []
    with open(EVENTS_FILE, 'r') as f:
        for line in f:
            event = json.loads(line.strip())
            event['timestamp'] = datetime.fromtimestamp(event['timestamp'])
            event['product_categories'] = [p.get('product_category', 'Unknown') for p in event.get('products', [])]
            events.append(event)
    return pd.DataFrame(events)

# Streamlit Page Config
st.set_page_config(page_title="Real-Time Event Dashboard", layout="wide")

st.title("📊 Real-Time Event Analytics Dashboard")

# Load data
data_load_state = st.text('Loading data...')
df = load_events()
data_load_state.text('')

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

# Filter by time window
if time_window != "All":
    try:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        if time_window.endswith('min'):
            minutes = int(time_window[:-3])
            threshold = datetime.now() - timedelta(minutes=minutes)
        elif time_window.endswith('h'):
            hours = int(time_window[:-1])
            threshold = datetime.now() - timedelta(hours=hours)
        elif time_window.endswith('d'):
            days = int(time_window[:-1])
            threshold = datetime.now() - timedelta(days=days)

        df = df[df['timestamp'] > threshold]
    except Exception as e:
        st.warning(f"Could not filter by time window: {e} — showing all data")

# Update sidebar data status
data_status.markdown(f"""
- **Total Events**: {len(df):,}
- **Time Window**: {time_window}
""")

# Sidebar for filtering event types
event_types = st.sidebar.multiselect('Select Event Types', options=df['event_type'].unique(), default=df['event_type'].unique())
use_recent_only = st.sidebar.checkbox("Use only recent (last 60 seconds) data", value=False)

filtered_df = df[df['event_type'].isin(event_types)]

# Metric Summary
col1, col2, col3 = st.columns(3)
col1.metric("Total Events", len(filtered_df))
col2.metric("Unique Event Types", filtered_df['event_type'].nunique())
col3.metric("Total Products Involved", sum(filtered_df['product_categories'].apply(len)))

# Event Type Distribution
event_counts = filtered_df['event_type'].value_counts().reset_index()
event_counts.columns = ['event_type', 'count']

chart1 = alt.Chart(event_counts).mark_bar().encode(
    x=alt.X('count:Q', title='Count'),
    y=alt.Y('event_type:N', sort='-x', title='Event Type'),
    color='event_type:N'
).properties(title='Event Type Distribution', width=500, height=300)

st.altair_chart(chart1, use_container_width=True)

# Product Category Distribution
all_categories = []
for cats in filtered_df['product_categories']:
    all_categories.extend(cats)

category_df = pd.Series(all_categories).value_counts().reset_index()
category_df.columns = ['category', 'count']

chart2 = alt.Chart(category_df).mark_bar().encode(
    x=alt.X('count:Q', title='Count'),
    y=alt.Y('category:N', sort='-x', title='Product Category'),
    color='category:N'
).properties(title='Product Category Distribution', width=500, height=300)

st.altair_chart(chart2, use_container_width=True)

# Time Series: Events per Second
filtered_df.set_index('timestamp', inplace=True)
time_series = filtered_df.resample('1s').size().reset_index(name='event_count')

chart3 = alt.Chart(time_series).mark_line(point=True).encode(
    x='timestamp:T',
    y='event_count:Q'
).properties(title='Events per Second', width=1000, height=300)

st.altair_chart(chart3, use_container_width=True)

# Time Series: Top Product Categories
exploded = filtered_df.explode('product_categories')
top_cats = exploded['product_categories'].value_counts().nlargest(5).index.tolist()
top_cats_df = exploded[exploded['product_categories'].isin(top_cats)]
top_cat_series = top_cats_df.groupby([pd.Grouper(freq='5s'), 'product_categories']).size().reset_index(name='count')

chart4 = alt.Chart(top_cat_series).mark_line(point=True).encode(
    x='timestamp:T',
    y='count:Q',
    color='product_categories:N'
).properties(title='Top Product Categories Over Time', width=1000, height=300)

st.altair_chart(chart4, use_container_width=True)

# --------------------- Additional Real-Time Analytics ---------------------

if use_recent_only:
    time_threshold = datetime.now() - timedelta(seconds=60)
    recent_data = filtered_df[filtered_df.index > time_threshold].copy()
else:
    recent_data = filtered_df.copy()

recent_data['product_categories_exploded'] = recent_data['product_categories'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else 'Unknown')

# Journey categorization
def categorize_journey(events):
    if 'login' in events and 'purchase' in events:
        return 'Completed Purchase'
    elif 'login' in events and 'add_to_cart' in events:
        return 'Abandoned Cart'
    elif 'login' in events:
        return 'Browsing Only'
    else:
        return 'Other'

user_paths = recent_data.groupby('event_id')['event_type'].apply(list).reset_index()
user_paths['journey_type'] = user_paths['event_type'].apply(categorize_journey)
journey_counts = user_paths['journey_type'].value_counts().reset_index()
journey_counts.columns = ['journey_type', 'count']

chart_journey = alt.Chart(journey_counts).mark_bar().encode(
    x=alt.X('count:Q', title='Count'),
    y=alt.Y('journey_type:N', sort='-x', title='Journey Type'),
    color='journey_type:N'
).properties(title='User Journey Funnel', width=500, height=300)

# Top Active Users
top_users = recent_data['event_id'].value_counts().nlargest(5).reset_index()
top_users.columns = ['event_id', 'count']

chart_top_users = alt.Chart(top_users).mark_bar().encode(
    x=alt.X('count:Q', title='Count'),
    y=alt.Y('event_id:N', sort='-x', title='Event ID'),
    color='event_id:N'
).properties(title='Top 5 Active Users', width=500, height=300)

# Heatmap
matrix = pd.crosstab(recent_data['product_categories_exploded'], recent_data['event_type']).reset_index().melt(
    id_vars='product_categories_exploded', var_name='event_type', value_name='count'
)

chart_heatmap = alt.Chart(matrix).mark_rect().encode(
    x=alt.X('event_type:N', title='Event Type'),
    y=alt.Y('product_categories_exploded:N', title='Product Category'),
    color=alt.Color('count:Q', title='Count')
).properties(title='Category vs Event Type Heatmap', width=500, height=300)

# Events Over Time
event_ts = recent_data.reset_index().groupby([pd.Grouper(key='timestamp', freq='5s'), 'event_type']).size().unstack(fill_value=0).reset_index()
event_ts_melted = event_ts.melt(id_vars='timestamp', var_name='event_type', value_name='count')

chart_event_ts = alt.Chart(event_ts_melted).mark_area().encode(
    x=alt.X('timestamp:T', title='Time'),
    y=alt.Y('count:Q', stack='zero', title='Count'),
    color='event_type:N'
).properties(title='Event Types Over Time', width=1000, height=300)

st.header("Additional Real-Time Analytics")

col5, col6 = st.columns(2)
with col5:
    st.altair_chart(chart_journey, use_container_width=True)
with col6:
    st.altair_chart(chart_top_users, use_container_width=True)

col7, col8 = st.columns(2)
with col7:
    st.altair_chart(chart_heatmap, use_container_width=True)
with col8:
    st.altair_chart(chart_event_ts, use_container_width=True)

# Handle auto-refresh
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()
