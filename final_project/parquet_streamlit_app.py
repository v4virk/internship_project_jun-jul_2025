import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

import altair as alt
import pandas as pd
import streamlit as st

try:
    from pyspark.sql import SparkSession
except ImportError:  # pragma: no cover - Streamlit displays this clearly at runtime
    SparkSession = None


DEFAULT_PARQUET_PATH = "events/"
DEFAULT_CHECKPOINT_NOTE = "checkpoints/"
PRODUCT_FIELD_ORDER = ["product_id", "product_category", "price", "product_type", "brand"]


st.set_page_config(page_title="Parquet Event Analytics Dashboard", layout="wide")


@st.cache_resource(show_spinner=False)
def get_spark_session() -> SparkSession:
    """Create one reusable SparkSession for the Streamlit process."""
    if SparkSession is None:
        raise RuntimeError(
            "PySpark is not installed. Install it with: pip install pyspark"
        )

    return (
        SparkSession.builder.appName("ParquetEventAnalyticsDashboard")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


@st.cache_data(ttl=5, show_spinner=False)
def load_parquet_data(parquet_path: str) -> pd.DataFrame:
    """
    Read Spark-written Parquet data and return a Pandas DataFrame.

    The path can point to a local Parquet folder or an HDFS Parquet folder.
    Spark reads the whole folder, including partition folders such as
    event_type=purchase/ and event_type=login/.
    """
    clean_path = parquet_path.strip()
    if not clean_path:
        raise ValueError("Parquet path cannot be empty.")

    spark = get_spark_session()
    spark_df = spark.read.parquet(clean_path)
    return spark_df.toPandas()


def product_to_dict(product: Any) -> Dict[str, Any]:
    """Convert Spark Row, dict, tuple, or list product values into dictionaries."""
    if product is None:
        return {}

    if isinstance(product, dict):
        return product

    if hasattr(product, "asDict"):
        return product.asDict(recursive=True)

    if isinstance(product, (list, tuple)):
        return {
            field: product[index] if index < len(product) else None
            for index, field in enumerate(PRODUCT_FIELD_ORDER)
        }

    return {}


def normalize_products(products: Any) -> List[Dict[str, Any]]:
    """Normalize the nested products column so it is easy to visualize."""
    if products is None:
        return []

    if isinstance(products, float) and pd.isna(products):
        return []

    if hasattr(products, "tolist"):
        products = products.tolist()

    if not isinstance(products, (list, tuple)):
        return []

    return [product_to_dict(product) for product in products if product is not None]


def prepare_for_visualization(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich the Parquet data for Streamlit and Altair charts."""
    df = raw_df.copy()

    if df.empty:
        return df

    if "event_type" not in df.columns:
        df["event_type"] = "unknown"

    if "event_id" not in df.columns:
        df["event_id"] = "unknown"

    if "timestamp" in df.columns:
        if pd.api.types.is_numeric_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")
        else:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    else:
        df["timestamp"] = pd.NaT

    if "products" not in df.columns:
        df["products"] = [[] for _ in range(len(df))]

    df["products_normalized"] = df["products"].apply(normalize_products)
    df["product_categories"] = df["products_normalized"].apply(
        lambda products: [
            product.get("product_category") or "Unknown"
            for product in products
            if product.get("product_category")
        ]
    )
    df["product_count"] = df["products_normalized"].apply(len)

    return df


def apply_time_window(df: pd.DataFrame, time_window: str) -> pd.DataFrame:
    """Filter a DataFrame using the selected dashboard time window."""
    if df.empty or time_window == "All" or "timestamp" not in df.columns:
        return df

    valid_timestamps = df["timestamp"].dropna()
    if valid_timestamps.empty:
        return df

    now = datetime.now()
    if time_window.endswith("min"):
        threshold = now - timedelta(minutes=int(time_window[:-3]))
    elif time_window.endswith("h"):
        threshold = now - timedelta(hours=int(time_window[:-1]))
    elif time_window.endswith("d"):
        threshold = now - timedelta(days=int(time_window[:-1]))
    else:
        return df

    return df[df["timestamp"] > threshold]


def show_altair_chart(chart: alt.Chart, empty_message: str) -> None:
    """Show a chart with a consistent empty-state message."""
    if chart is None:
        st.info(empty_message)
    else:
        st.altair_chart(chart, use_container_width=True)


def event_type_distribution_chart(df: pd.DataFrame):
    if df.empty:
        return None

    event_counts = df["event_type"].value_counts().reset_index()
    event_counts.columns = ["event_type", "count"]

    return (
        alt.Chart(event_counts)
        .mark_bar()
        .encode(
            x=alt.X("count:Q", title="Count"),
            y=alt.Y("event_type:N", sort="-x", title="Event Type"),
            color=alt.Color("event_type:N", legend=None),
            tooltip=["event_type:N", "count:Q"],
        )
        .properties(title="Event Type Distribution", height=300)
    )


def product_category_distribution_chart(df: pd.DataFrame):
    if df.empty or "product_categories" not in df.columns:
        return None

    exploded = df.explode("product_categories")
    exploded = exploded.dropna(subset=["product_categories"])
    exploded = exploded[exploded["product_categories"] != ""]

    if exploded.empty:
        return None

    category_counts = exploded["product_categories"].value_counts().reset_index()
    category_counts.columns = ["category", "count"]

    return (
        alt.Chart(category_counts)
        .mark_bar()
        .encode(
            x=alt.X("count:Q", title="Count"),
            y=alt.Y("category:N", sort="-x", title="Product Category"),
            color=alt.Color("category:N", legend=None),
            tooltip=["category:N", "count:Q"],
        )
        .properties(title="Product Category Distribution", height=300)
    )


def events_over_time_chart(df: pd.DataFrame, time_bucket: str):
    if df.empty or df["timestamp"].dropna().empty:
        return None

    time_series = (
        df.dropna(subset=["timestamp"])
        .set_index("timestamp")
        .resample(time_bucket)
        .size()
        .reset_index(name="event_count")
    )

    if time_series.empty:
        return None


    return (
        alt.Chart(time_series)
        .mark_line(point=True)
        .encode(
            x=alt.X("timestamp:T", title="Time"),
            y=alt.Y("event_count:Q", title="Event Count"),
            tooltip=["timestamp:T", "event_count:Q"],
        )
        .properties(title=f"Events Over Time ({time_bucket})", height=300)
    )


def top_categories_over_time_chart(df: pd.DataFrame, time_bucket: str):
    if df.empty or df["timestamp"].dropna().empty:
        return None

    exploded = df.explode("product_categories").dropna(
        subset=["timestamp", "product_categories"]
    )
    if exploded.empty:
        return None

    top_categories = exploded["product_categories"].value_counts().nlargest(5).index
    top_df = exploded[exploded["product_categories"].isin(top_categories)]

    category_series = (
        top_df.set_index("timestamp")
        .groupby([pd.Grouper(freq=time_bucket), "product_categories"])
        .size()
        .reset_index(name="count")
    )

    if category_series.empty:
        return None

    return (
        alt.Chart(category_series)
        .mark_line(point=True)
        .encode(
            x=alt.X("timestamp:T", title="Time"),
            y=alt.Y("count:Q", title="Count"),
            color=alt.Color("product_categories:N", title="Product Category"),
            tooltip=["timestamp:T", "product_categories:N", "count:Q"],
        )
        .properties(title=f"Top Product Categories Over Time ({time_bucket})", height=300)
    )


def category_event_heatmap(df: pd.DataFrame):
    if df.empty:
        return None

    exploded = df.explode("product_categories").dropna(subset=["product_categories"])
    if exploded.empty:
        return None

    matrix = (
        pd.crosstab(exploded["product_categories"], exploded["event_type"])
        .reset_index()
        .melt(
            id_vars="product_categories",
            var_name="event_type",
            value_name="count",
        )
    )

    return (
        alt.Chart(matrix)
        .mark_rect()
        .encode(
            x=alt.X("event_type:N", title="Event Type"),
            y=alt.Y("product_categories:N", title="Product Category"),
            color=alt.Color("count:Q", title="Count"),
            tooltip=["product_categories:N", "event_type:N", "count:Q"],
        )
        .properties(title="Category vs Event Type Heatmap", height=350)
    )


def event_types_area_chart(df: pd.DataFrame, time_bucket: str):
    if df.empty or df["timestamp"].dropna().empty:
        return None

    event_ts = (
        df.dropna(subset=["timestamp"])
        .set_index("timestamp")
        .groupby([pd.Grouper(freq=time_bucket), "event_type"])
        .size()
        .reset_index(name="count")
    )

    if event_ts.empty:
        return None

    return (
        alt.Chart(event_ts)
        .mark_area()
        .encode(
            x=alt.X("timestamp:T", title="Time"),
            y=alt.Y("count:Q", stack="zero", title="Count"),
            color=alt.Color("event_type:N", title="Event Type"),
            tooltip=["timestamp:T", "event_type:N", "count:Q"],
        )
        .properties(title=f"Event Types Over Time ({time_bucket})", height=350)
    )


def top_active_ids_chart(df: pd.DataFrame):
    if df.empty:
        return None

    top_ids = df["event_id"].value_counts().nlargest(5).reset_index()
    top_ids.columns = ["event_id", "count"]

    return (
        alt.Chart(top_ids)
        .mark_bar()
        .encode(
            x=alt.X("count:Q", title="Count"),
            y=alt.Y("event_id:N", sort="-x", title="Event ID"),
            color=alt.Color("event_id:N", legend=None),
            tooltip=["event_id:N", "count:Q"],
        )
        .properties(title="Top 5 Active Event IDs", height=300)
    )


def categorize_journey(events: List[str]) -> str:
    event_set = set(events)
    if "login" in event_set and "purchase" in event_set:
        return "Completed Purchase"
    if "login" in event_set and "add_to_cart" in event_set:
        return "Abandoned Cart"
    if "login" in event_set:
        return "Browsing Only"
    return "Other"


def journey_funnel_chart(df: pd.DataFrame):
    if df.empty:
        return None

    user_paths = df.groupby("event_id")["event_type"].apply(list).reset_index()
    user_paths["journey_type"] = user_paths["event_type"].apply(categorize_journey)
    journey_counts = user_paths["journey_type"].value_counts().reset_index()
    journey_counts.columns = ["journey_type", "count"]

    return (
        alt.Chart(journey_counts)
        .mark_bar()
        .encode(
            x=alt.X("count:Q", title="Count"),
            y=alt.Y("journey_type:N", sort="-x", title="Journey Type"),
            color=alt.Color("journey_type:N", legend=None),
            tooltip=["journey_type:N", "count:Q"],
        )
        .properties(title="User Journey Funnel", height=300)
    )


def dataframe_for_download(df: pd.DataFrame) -> pd.DataFrame:
    """Return a CSV-friendly version of the prepared data."""
    download_df = df.copy()
    download_df["product_categories"] = download_df["product_categories"].apply(
        lambda values: ", ".join(values) if isinstance(values, list) else ""
    )
    return download_df.drop(columns=["products", "products_normalized"], errors="ignore")


st.title("📊 Parquet-Based Real-Time Event Analytics Dashboard")
st.caption(
    "This dashboard reads Spark-generated Parquet files instead of raw JSON logs. "
    "Use it with the Parquet output folder created by the Kafka-to-HDFS Spark job."
)

with st.sidebar:
    st.header("Parquet Data Source")
    parquet_path = st.text_input(
        "Parquet folder path",
        value=DEFAULT_PARQUET_PATH,
        help=(
            "Enter the folder containing Spark Parquet output. "
            "This can be a local folder or an HDFS path."
        ),
    )
    st.caption(f"Spark checkpoints are usually stored separately, for example: `{DEFAULT_CHECKPOINT_NOTE}`")

    st.markdown("---")
    st.header("Dashboard Controls")
    time_window = st.selectbox(
        "Analysis Time Window",
        ["30min", "1h", "4h", "12h", "24h", "7d", "All"],
        index=6,
    )
    time_bucket = st.selectbox(
        "Time Chart Bucket",
        ["1s", "5s", "30s", "1min", "5min", "15min", "1h"],
        index=1,
    )
    auto_refresh = st.checkbox("Enable Auto-Refresh", value=True)
    refresh_rate = st.slider("Refresh Rate (seconds)", 5, 120, 30)
    clear_cache = st.button("Clear Cached Parquet Data")

if clear_cache:
    load_parquet_data.clear()
    st.success("Cached Parquet data cleared. The next run will reload from source.")

try:
    with st.spinner("Reading Parquet files and preparing dashboard data..."):
        raw_df = load_parquet_data(parquet_path)
        df = prepare_for_visualization(raw_df)
except Exception as exc:
    st.error("Could not read the Parquet data source.")
    st.write("Check that the Parquet folder exists and contains Spark-written `.parquet` files.")
    st.exception(exc)
    st.stop()

if df.empty:
    st.warning("The Parquet source was read successfully, but it does not contain any rows yet.")
    st.stop()

df = apply_time_window(df, time_window)

available_event_types = sorted(
    event_type for event_type in df["event_type"].dropna().unique().tolist()
)

with st.sidebar:
    selected_event_types = st.multiselect(
        "Select Event Types",
        options=available_event_types,
        default=available_event_types,
    )
    use_recent_only = st.checkbox("Use only recent data from last 60 seconds", value=False)

if selected_event_types:
    filtered_df = df[df["event_type"].isin(selected_event_types)].copy()
else:
    filtered_df = df.iloc[0:0].copy()

if use_recent_only and not filtered_df["timestamp"].dropna().empty:
    recent_threshold = datetime.now() - timedelta(seconds=60)
    filtered_df = filtered_df[filtered_df["timestamp"] > recent_threshold]

with st.sidebar:
    st.markdown("---")
    st.markdown("**Data Status**")
    st.write(f"Rows after filters: `{len(filtered_df):,}`")
    st.write(f"Source columns: `{len(raw_df.columns)}`")
    st.write(f"Parquet path: `{parquet_path}`")

if filtered_df.empty:
    st.warning("No rows match the selected filters.")
    st.stop()

metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
metric_col1.metric("Total Events", f"{len(filtered_df):,}")
metric_col2.metric("Unique Event Types", filtered_df["event_type"].nunique())
metric_col3.metric("Total Products", int(filtered_df["product_count"].sum()))
metric_col4.metric("Unique Event IDs", filtered_df["event_id"].nunique())

st.subheader("Prepared Data Preview")
preview_columns = [
    column
    for column in ["event_id", "event_type", "timestamp", "product_count", "product_categories"]
    if column in filtered_df.columns
]
st.dataframe(filtered_df[preview_columns].head(100), use_container_width=True)

download_ready_df = dataframe_for_download(filtered_df)
st.download_button(
    "Download Prepared Data as CSV",
    data=download_ready_df.to_csv(index=False).encode("utf-8"),
    file_name="prepared_parquet_events.csv",
    mime="text/csv",
)

st.subheader("Core Analytics")
chart_col1, chart_col2 = st.columns(2)
with chart_col1:
    show_altair_chart(
        event_type_distribution_chart(filtered_df),
        "No event type data available for the selected filters.",
    )
with chart_col2:
    show_altair_chart(
        product_category_distribution_chart(filtered_df),
        "No product category data available for the selected filters.",
    )

show_altair_chart(
    events_over_time_chart(filtered_df, time_bucket),
    "No timestamp data available for events-over-time chart.",
)

show_altair_chart(
    top_categories_over_time_chart(filtered_df, time_bucket),
    "No product category time-series data available.",
)

st.subheader("Additional Analytics")
extra_col1, extra_col2 = st.columns(2)
with extra_col1:
    show_altair_chart(
        journey_funnel_chart(filtered_df),
        "No journey data available for the selected filters.",
    )
with extra_col2:
    show_altair_chart(
        top_active_ids_chart(filtered_df),
        "No event ID data available for the selected filters.",
    )

extra_col3, extra_col4 = st.columns(2)
with extra_col3:
    show_altair_chart(
        category_event_heatmap(filtered_df),
        "No category/event matrix data available.",
    )
with extra_col4:
    show_altair_chart(
        event_types_area_chart(filtered_df, time_bucket),
        "No event type time-series data available.",
    )

with st.expander("Raw Parquet Schema Columns"):
    st.write(list(raw_df.columns))

if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()