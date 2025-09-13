import streamlit as st
import pandas as pd
import plotly.express as px
import pycountry
from pathlib import Path

# ---- Helpers ----,
@st.cache_data
def load_data(path=None):
    if path is None:
        script_dir = Path(__file__).resolve().parent
        path = script_dir.parent / "multi_modal" / "keywords_clean.csv"

        # If using docker container, then
        # path = "/app/data/keywords_clean.csv"
    if not path.exists():
        raise FileNotFoundError(f"❌ keywords_clean.csv not found at {path}")
                                
    df = pd.read_csv(path, parse_dates=["detected_at", "posted_at"], low_memory=False)
    df["posted_at"] = pd.to_datetime(df["posted_at"], errors="coerce")
    df["detected_at"] = pd.to_datetime(df["detected_at"], errors="coerce")

    # Ensure numeric columns exist
    for c in ["collectCount", "commentCount", "diggCount", "playCount", "shareCount",
              "engagement_total", "engagement_rate", "virality_index", "discussion_rate", "save_rate"]:
        if c not in df.columns:
            df[c] = 0
    
    # Standardize region names: uppercase and strip
    if "region" in df.columns:
        df["region"] = df["region"].astype(str).str.strip().str.upper()
    else:
        df["region"] = "UNKNOWN"

    # Create iso3 code for mapping (try convert from ISO2 or name)
    def iso2_to_iso3(code):
        try:
            country = pycountry.countries.get(alpha_2=code)
            if country:
                return country.alpha_3
        except:
            try:
                return pycountry.countries.lookup(code).alpha_3
            except Exception:
                return None
    df["iso3"] = df["region"].apply(lambda r: iso2_to_iso3(r) if isinstance(r, str) and len(r) == 2 else iso2_to_iso3(r))  
    return df

def filter_df(df, regions, categories, modalities, start_date, end_date, top_n):
    dff = df.copy()

    if regions:
        dff = dff[dff["region"].isin(regions)]
    if categories:
        dff = dff[dff["category"].isin(categories)]
    if modalities:
        dff = dff[dff["modality"].isin(modalities)]

    # Or simpler: just compare using naive timestamps
    dff = dff[(dff["posted_at"] >= pd.Timestamp(start_date)) & (dff["posted_at"] <= pd.Timestamp(end_date) + pd.Timedelta(days=1))]

    if top_n and "engagement_total" in dff.columns:
        dff = dff.nlargest(top_n, "engagement_total")

    return dff

# ----------  UI -----------
st.set_page_config(page_title="TikTok Trends Dashboard", layout="wide")
st.title("TikTok Keywords — Snapshot Dashboard")

df = load_data()
st.sidebar.header("Filters")

# Region filter
regions = sorted(df["region"].dropna().unique().tolist())
selected_regions = st.sidebar.multiselect("Region (country code)", options=regions, default=None)

# Category 
categories = sorted(df["category"].dropna().unique().tolist())
selected_categories = st.sidebar.multiselect("Category", options=categories, default=None)

# Modality
modalities = sorted(df["modality"].dropna().unique().tolist())
selected_modalities = st.sidebar.multiselect("Modality", options=modalities, default=None)

# Date range
min_date = df["posted_at"].min().date()
max_date = df["posted_at"].max().date()
start_date = st.sidebar.date_input("Start date", value=min_date, min_value=min_date, max_value=max_date)
end_date = st.sidebar.date_input("End date", value=max_date, min_value=min_date, max_value=max_date)

# Convert to UTC-aware Timestamps
start_date_utc = pd.Timestamp(start_date).tz_localize("UTC")
end_date_utc = pd.Timestamp(end_date).tz_localize("UTC")

# Top N parameter
top_n = st.sidebar.slider("Top N keywords", 5, 100, 20)

# Apply filters (convert None/empty to actual filters)
filter_regions = selected_regions if selected_regions else df["region"].unique().tolist()
filter_categories = selected_categories if selected_categories else df["category"].unique().tolist()
filter_modalities = selected_modalities if selected_modalities else df["modality"].unique().tolist()

dff = filter_df(df, filter_regions, filter_categories, filter_modalities, start_date_utc, end_date_utc, top_n)
st.sidebar.markdown(f"**Rows after filter: ** {len(dff)}")

# --------- Layout: 2x2 grid -----------
col1, col2 = st.columns(2)

with col1:
    # Top keywords by engagement_total
    st.subheader("Top Keywords by Engagement")
    if not dff.empty:
        kw_agg = (dff.groupby(["keyword","modality"], as_index=False)
                .agg(total_engagement=("engagement_total","sum"),
                        plays=("playCount","sum"))
                .sort_values("total_engagement", ascending=False))
        top_kw = kw_agg.groupby("keyword", as_index=False).agg(total_engagement=("total_engagement","sum"))
        top_kw = top_kw.sort_values("total_engagement", ascending=False).head(top_n)
        # Merge back modality info to color by most-common modality for keyword (simple heuristic)
        modal_for_kw = (kw_agg.sort_values("total_engagement", ascending=False).drop_duplicates("keyword")[["keyword","modality"]])
        top_kw = top_kw.merge(modal_for_kw, on="keyword", how="left")
        fig1 = px.bar(top_kw, x="total_engagement", y="keyword", orientation="h",
                    color="modality", labels={"total_engagement": "Engagement (sum)"}, hover_data=["total_engagement"])
        fig1.update_layout(yaxis={'categoryorder': 'total ascending'}, height=600)
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.info("No data available for the selected filters.")

with col2:
    # Map: Engagement rate by region
    st.subheader("Avg Engagement Rate by Region")
    if not dff.empty:
        map_df = dff.groupby(["region","iso3"], as_index=False).agg(avg_engagement_rate=("engagement_rate", "mean"),
                                                                sum_plays=("playCount", "sum"),
                                                                count_videos=("video_id", "nunique"))
        map_df = map_df.dropna(subset=["iso3"])
        if not map_df.empty:
            fig2 = px.choropleth(map_df, locations="iso3", color="avg_engagement_rate",
                                hover_name="region", hover_data=["sum_plays", "count_videos"],
                                color_continuous_scale="YlOrRd", labels={"avg_engagement_rate": "Avg engagement rate"})
            fig2.update_layout(height=600)
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No region data available for map visualization.")
    else:
        st.info("No region data available for map visualization.")

# Next row
col3, col4 = st.columns(2)

with col3:
    # Virality vs Discussion scatter
    st.subheader("Virality (share rate) vs Discussion (comment rate)")
    if not dff.empty:
        scatter_df = (dff.groupby(["keyword"], as_index=False)
                    .agg(avg_virality=("virality_index","mean"),
                        avg_discussion=("discussion_rate","mean"),
                        total_engagement=("engagement_total","sum"),
                        plays=("playCount","sum"),
                        category=("category","first")))
        # filter top keywords for readability
        top_keywords_list = top_kw["keyword"].tolist()
        scatter_df = scatter_df[scatter_df["keyword"].isin(top_keywords_list)]
        if scatter_df.empty:
            st.info("Not enough data for scatter plot.")
        else:
            fig3 = px.scatter(scatter_df, x="avg_virality", y="avg_discussion",
                            size="total_engagement", color="category",
                            hover_name="keyword", hover_data=["plays","total_engagement"],
                            labels={"avg_virality":"Virality Index (shares/views)","avg_discussion":"Discussion Rate (comments/views)"})
            fig3.update_layout(height=500)
            st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("No data available for scatter plot.")

with col4:
    # Category distribution (bar) with engagement rate
    if not dff.empty:
        st.subheader("Category Engagement Comparison")
        cat_df = (dff.groupby("category", as_index=False)
                .agg(total_engagement=("engagement_total","sum"),
                    avg_engagement_rate=("engagement_rate","mean"),
                    count_videos=("video_id","nunique")))
        if cat_df.empty:
            st.info("No category data.")
        else:
            cat_df = cat_df.sort_values("total_engagement", ascending=False)
            fig4 = px.bar(cat_df, x="total_engagement", y="category", orientation="h",
                        hover_data=["avg_engagement_rate","count_videos"],
                        labels={"total_engagement":"Total Engagement"})
            fig4.update_layout(height=500)
            st.plotly_chart(fig4, use_container_width=True)
    else:
        st.info("No data available for category chart.")

# ---------- Bottom: data table and download ----------
st.markdown("---")
st.subheader("Data (sample)")
st.dataframe(dff.sample(min(200, len(dff))).reset_index(drop=True))

# CSV download button
@st.cache_data
def to_csv_bytes(df):
    return df.to_csv(index=False).encode("utf-8")

csv_bytes = to_csv_bytes(dff)
st.download_button("Download filtered CSV", csv_bytes, file_name="keywords_filtered.csv", mime="text/csv")
