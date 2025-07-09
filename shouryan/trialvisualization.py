import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import json
import os

FILE_PATH = "events_log.json"
st_autorefresh(interval=5000, key="auto_refresh")

if "data" not in st.session_state:
    st.session_state.data = pd.DataFrame(columns=['timestamp', 'user_id', 'event_type', 'product_category'])
    st.session_state.last_read_len = 0

st.set_page_config(layout="wide")
st.title("📊 Real-Time Analytics Dashboard ")

st.experimental_rerun = st.experimental_rerun if hasattr(st, "experimental_rerun") else lambda: None
st_autorefresh = st.experimental_rerun
st.experimental_rerun = None

if os.path.exists(FILE_PATH):
    with open(FILE_PATH, "r") as f:
        lines = f.readlines()
    new_lines = lines[st.session_state.last_read_len:]
    new_events = []
    for line in new_lines:
        try:
            event = json.loads(line)
            event["timestamp"] = pd.to_datetime(event["timestamp"])
            new_events.append(event)
        except:
            continue
    if new_events:
        st.session_state.data = pd.concat([st.session_state.data, pd.DataFrame(new_events)], ignore_index=True)
        st.session_state.last_read_len += len(new_lines)

now = datetime.now()
time_threshold = now - timedelta(seconds=60)
recent_data = st.session_state.data[st.session_state.data['timestamp'] > time_threshold]

col1, col2 = st.columns(2)
with col1:
    user_paths = recent_data.groupby('user_id')['event_type'].apply(list).reset_index()
    user_paths['journey_type'] = user_paths['event_type'].apply(lambda events: (
        'Completed Purchase' if 'login' in events and 'purchase' in events else
        'Abandoned Cart' if 'login' in events and 'add_to_cart' in events else
        'Browsing Only' if 'login' in events else 'Other'
    ))
    journey_counts = user_paths['journey_type'].value_counts()
    fig, ax = plt.subplots()
    sns.barplot(x=journey_counts.values, y=journey_counts.index, ax=ax, palette='viridis')
    ax.set_title("User Journey Funnel (Last 60s)")
    st.pyplot(fig)

with col2:
    event_counts = recent_data['event_type'].value_counts()
    fig, ax = plt.subplots()
    sns.barplot(x=event_counts.index, y=event_counts.values, ax=ax, palette='Set2')
    ax.set_title("Event Frequency")
    ax.tick_params(axis='x', rotation=45)
    st.pyplot(fig)

col3, col4 = st.columns(2)
with col3:
    cat_counts = recent_data['product_category'].value_counts()
    fig, ax = plt.subplots()
    ax.pie(cat_counts.values, labels=cat_counts.index, autopct='%1.1f%%')
    ax.set_title("Product Categories")
    st.pyplot(fig)

with col4:
    time_series = recent_data.set_index('timestamp').resample('1s').count()['user_id']
    fig, ax = plt.subplots()
    if not time_series.empty:
        time_series.plot(ax=ax, marker='o', color='orange')
    else:
        ax.text(0.5, 0.5, 'Waiting for data...', ha='center')
    ax.set_title("Events Per Second")
    st.pyplot(fig)

col5, col6 = st.columns(2)
with col5:
    top_users = recent_data['user_id'].value_counts().nlargest(5)
    fig, ax = plt.subplots()
    sns.barplot(x=top_users.values, y=top_users.index, ax=ax, palette='coolwarm')
    ax.set_title("Top 5 Active Users")
    st.pyplot(fig)

with col6:
    matrix = pd.crosstab(recent_data['product_category'], recent_data['event_type'])
    fig, ax = plt.subplots()
    if not matrix.empty:
        sns.heatmap(matrix, annot=True, fmt='d', cmap='YlGnBu', ax=ax)
    else:
        ax.text(0.5, 0.5, 'No data available', ha='center')
    ax.set_title("Category vs Event Type Heatmap")
    st.pyplot(fig)

col7, col8 = st.columns(2)
with col7:
    event_ts = recent_data.set_index('timestamp').groupby([pd.Grouper(freq='5s'), 'event_type']).size().unstack(fill_value=0)
    fig, ax = plt.subplots()
    if not event_ts.empty:
        event_ts.plot.area(ax=ax, stacked=True, alpha=0.6, colormap='tab20')
    else:
        ax.text(0.5, 0.5, 'Not enough data', ha='center')
    ax.set_title("Event Types Over Time")
    st.pyplot(fig)

with col8:
    cat_ts = recent_data.set_index('timestamp').groupby([pd.Grouper(freq='5s'), 'product_category']).size().unstack(fill_value=0)
    fig, ax = plt.subplots()
    if not cat_ts.empty:
        cat_ts.plot(ax=ax, marker='o', colormap='Set1')
    else:
        ax.text(0.5, 0.5, 'Not enough data', ha='center')
    ax.set_title("Product Categories Over Time")
    st.pyplot(fig)
