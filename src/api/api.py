# api.py
from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse
import pandas as pd
import io
from typing import List, Optional
import functools
app = FastAPI(title="TikTok Keywords API")

DATA_PATH = "keywords_clean.csv"

@functools.lru_cache(maxsize=1)
def load_df():
    df = pd.read_csv(DATA_PATH, parse_dates=["detected_at","posted_at"], low_memory=False)
    return df

def apply_filters(df, regions=None, categories=None, modalities=None, start_date=None, end_date=None):
    dff = df.copy()
    if regions:
        dff = dff[dff["region"].isin(regions)]
    if categories:
        dff = dff[dff["category"].isin(categories)]
    if modalities:
        dff = dff[dff["modality"].isin(modalities)]
    if start_date:
        dff = dff[dff["posted_at"] >= pd.to_datetime(start_date)]
    if end_date:
        dff = dff[dff["posted_at"] <= pd.to_datetime(end_date)]
    return dff

@app.get("/api/keywords")
def get_keywords(
    regions: Optional[List[str]] = Query(None),
    categories: Optional[List[str]] = Query(None),
    modalities: Optional[List[str]] = Query(None),
    top_n: int = 50
):
    df = load_df()
    dff = apply_filters(df, regions, categories, modalities)
    # Aggregate by keyword
    agg = dff.groupby("keyword", as_index=False).agg(
        total_engagement=("engagement_total","sum"),
        plays=("playCount","sum"),
        count_videos=("video_id","nunique")
    ).sort_values("total_engagement", ascending=False).head(top_n)
    return agg.to_dict(orient="records")

@app.get("/api/download")
def download_filtered_csv(
    regions: Optional[List[str]] = Query(None),
    categories: Optional[List[str]] = Query(None),
    modalities: Optional[List[str]] = Query(None),
):
    df = load_df()
    dff = apply_filters(df, regions, categories, modalities)
    stream = io.StringIO()
    dff.to_csv(stream, index=False)
    stream.seek(0)
    return StreamingResponse(iter([stream.getvalue()]),
                             media_type="text/csv",
                             headers={"Content-Disposition":"attachment; filename=keywords_filtered.csv"})
