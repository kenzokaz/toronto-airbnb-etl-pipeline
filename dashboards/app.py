"""
dashboard/app.py
----------------
Streamlit dashboard for the Toronto Airbnb ETL pipeline.
Uses review scores, occupancy, and availability as primary metrics
since price data is not available in this dataset.
"""

import os
import streamlit as st
import pandas as pd
import psycopg2
import sqlalchemy
import plotly.express as px

st.set_page_config(
    page_title="Toronto Airbnb Analytics",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

@st.cache_resource
def get_engine():
    db = {
        "host":     os.getenv("DB_HOST",     "postgres"),
        "port":     os.getenv("DB_PORT",     "5432"),
        "dbname":   os.getenv("DB_NAME",     "airbnb_dw"),
        "user":     os.getenv("DB_USER",     "airflow"),
        "password": os.getenv("DB_PASSWORD", "airflow"),
    }
    url = f"postgresql+psycopg2://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['dbname']}"
    return sqlalchemy.create_engine(url)

@st.cache_data(ttl=300)
def query(sql):
    return pd.read_sql(sql, get_engine())

st.title("🏠 Toronto Airbnb Market Analytics")
st.markdown("*End-to-end ETL Pipeline · COSC4606 · Data sourced from Inside Airbnb (Jan 2026)*")
st.divider()

kpis = query("""
    SELECT
        COUNT(*)                             AS total_listings,
        COUNT(DISTINCT f.host_id)            AS unique_hosts,
        COUNT(DISTINCT f.neighbourhood_id)   AS neighbourhoods,
        ROUND(AVG(f.availability_365), 1)    AS avg_availability,
        ROUND(AVG(f.number_of_reviews), 1)   AS avg_reviews,
        SUM(f.number_of_reviews)             AS total_reviews
    FROM warehouse.fact_listings f
""")

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Total Listings",      f"{int(kpis['total_listings'][0]):,}")
c2.metric("Unique Hosts",        f"{int(kpis['unique_hosts'][0]):,}")
c3.metric("Neighbourhoods",      f"{int(kpis['neighbourhoods'][0])}")
c4.metric("Avg Availability",    f"{kpis['avg_availability'][0]} days/yr")
c5.metric("Avg Reviews",         f"{kpis['avg_reviews'][0]}")
c6.metric("Total Reviews",       f"{int(kpis['total_reviews'][0]):,}")

st.divider()


st.sidebar.title("Filters")
room_types = query("SELECT DISTINCT room_type FROM warehouse.dim_room_type ORDER BY 1")["room_type"].tolist()
selected_rooms = st.sidebar.multiselect("Room Type", room_types, default=room_types)
min_reviews = st.sidebar.slider("Minimum Reviews", 0, 100, 0)

rt_filter = "'" + "','".join(selected_rooms) + "'"

st.subheader("Top 20 Neighbourhoods by Number of Listings")
nb_df = query(f"""
    SELECT n.neighbourhood, rt.room_type, COUNT(*) AS listing_count
    FROM warehouse.fact_listings f
    JOIN warehouse.dim_neighbourhood n ON f.neighbourhood_id = n.neighbourhood_id
    JOIN warehouse.dim_room_type rt    ON f.room_type_id     = rt.room_type_id
    WHERE rt.room_type IN ({rt_filter})
    GROUP BY n.neighbourhood, rt.room_type
    ORDER BY listing_count DESC
    LIMIT 20
""")
if not nb_df.empty:
    fig1 = px.bar(
        nb_df, x="listing_count", y="neighbourhood", color="room_type",
        orientation="h", height=500,
        labels={"listing_count": "Number of Listings", "neighbourhood": "Neighbourhood"},
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig1.update_layout(yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig1, use_container_width=True)

st.divider()

#  Chart 2 & 3 side by side 
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("🛏 Room Type Distribution")
    rt_df = query("""
        SELECT rt.room_type, COUNT(*) AS total
        FROM warehouse.fact_listings f
        JOIN warehouse.dim_room_type rt ON f.room_type_id = rt.room_type_id
        GROUP BY rt.room_type ORDER BY total DESC
    """)
    if not rt_df.empty:
        fig2 = px.pie(
            rt_df, names="room_type", values="total", hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Pastel,
        )
        fig2.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig2, use_container_width=True)

with col_b:
    st.subheader("Avg Availability by Neighbourhood (Top 20)")
    avail_df = query("""
        SELECT n.neighbourhood,
               ROUND(AVG(f.availability_365), 1) AS avg_availability,
               COUNT(*) AS listings
        FROM warehouse.fact_listings f
        JOIN warehouse.dim_neighbourhood n ON f.neighbourhood_id = n.neighbourhood_id
        GROUP BY n.neighbourhood
        ORDER BY avg_availability DESC
        LIMIT 20
    """)
    if not avail_df.empty:
        fig3 = px.bar(
            avail_df, x="avg_availability", y="neighbourhood", orientation="h",
            color="avg_availability", color_continuous_scale="Blues",
            labels={"avg_availability": "Avg Days Available/Year", "neighbourhood": ""},
            height=450,
        )
        fig3.update_layout(yaxis=dict(autorange="reversed"), coloraxis_showscale=False)
        st.plotly_chart(fig3, use_container_width=True)

st.divider()

#  Chart 4: Top hosts by reviews 
st.subheader("Top 15 Hosts by Total Reviews")
host_df = query(f"""
    SELECT h.host_name, h.total_listings,
           SUM(f.number_of_reviews) AS total_reviews,
           ROUND(AVG(f.availability_365), 0) AS avg_availability
    FROM warehouse.fact_listings f
    JOIN warehouse.dim_host h ON f.host_id = h.host_id
    WHERE f.number_of_reviews >= {min_reviews}
    GROUP BY h.host_id, h.host_name, h.total_listings
    ORDER BY total_reviews DESC
    LIMIT 15
""")
if not host_df.empty:
    fig4 = px.bar(
        host_df, x="host_name", y="total_reviews",
        color="total_listings", color_continuous_scale="Teal",
        labels={"host_name": "Host", "total_reviews": "Total Reviews", "total_listings": "# Listings"},
        height=400,
    )
    fig4.update_layout(xaxis_tickangle=-35)
    st.plotly_chart(fig4, use_container_width=True)

st.divider()

#  Chart 5: Reviews per month by room type 
st.subheader("Avg Reviews per Month by Room Type & Neighbourhood")
rpm_df = query(f"""
    SELECT n.neighbourhood, rt.room_type,
           ROUND(AVG(f.reviews_per_month), 2) AS avg_rpm,
           COUNT(*) AS listings
    FROM warehouse.fact_listings f
    JOIN warehouse.dim_neighbourhood n ON f.neighbourhood_id = n.neighbourhood_id
    JOIN warehouse.dim_room_type rt    ON f.room_type_id     = rt.room_type_id
    WHERE rt.room_type IN ({rt_filter})
      AND f.reviews_per_month > 0
    GROUP BY n.neighbourhood, rt.room_type
    ORDER BY avg_rpm DESC
    LIMIT 20
""")
if not rpm_df.empty:
    fig5 = px.scatter(
        rpm_df, x="listings", y="avg_rpm", color="room_type",
        size="listings", hover_name="neighbourhood",
        labels={"listings": "Number of Listings", "avg_rpm": "Avg Reviews/Month"},
        color_discrete_sequence=px.colors.qualitative.Bold,
        height=400,
    )
    st.plotly_chart(fig5, use_container_width=True)

st.divider()

#  Raw data explorer 
with st.expander("Explore Raw Warehouse Data"):
    limit = st.slider("Rows to show", 10, 500, 50)
    raw_df = query(f"""
        SELECT f.listing_id, f.listing_name, h.host_name, n.neighbourhood,
               rt.room_type, f.minimum_nights, f.number_of_reviews,
               f.reviews_per_month, f.availability_365
        FROM warehouse.fact_listings f
        JOIN warehouse.dim_host h          ON f.host_id          = h.host_id
        JOIN warehouse.dim_neighbourhood n ON f.neighbourhood_id = n.neighbourhood_id
        JOIN warehouse.dim_room_type rt    ON f.room_type_id     = rt.room_type_id
        WHERE f.number_of_reviews >= {min_reviews}
        ORDER BY f.number_of_reviews DESC
        LIMIT {limit}
    """)
    st.dataframe(raw_df, use_container_width=True)

st.caption("Data: Inside Airbnb — Toronto (Jan 2026) · Pipeline: Apache Airflow → PostgreSQL → Streamlit")