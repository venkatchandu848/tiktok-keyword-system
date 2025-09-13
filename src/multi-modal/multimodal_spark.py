from pyspark.sql import SparkSession
from pyspark import SparkFiles
import json
import os
import tempfile
import shutil
from datetime import datetime, timezone

# Import functions from multimodal_pipeline
from multimodal_pipeline import process_video_entry, save_keywords_bulk

# ---------------------------
# Spark Job
# ---------------------------

def process_partition(entries):
    """
    Runs process_video_entry for each video in this partition.
    Returns list of keyword rows.
    """
    import tempfile, shutil
    tmpdir = tempfile.mkdtemp(prefix="tt_mm_spark_")
    rows = []
    for entry in entries:
        try:
            r = process_video_entry(entry, tmpdir)
            rows.extend(r)
        except Exception as e:
            print(f"[process_partition] failed for {entry.get('id')}: {e}")
    shutil.rmtree(tmpdir, ignore_errors=True)
    return rows

def main():
    spark = SparkSession.builder \
        .appName("TikTokMultimodalPipeline") \
        .getOrCreate()

    # Load metadata JSON (produced by scraper.py)
    SCRAPER_JSON = os.path.join(os.path.dirname(__file__), "..", "scraper", "tiktok_trending.json")
    SCRAPER_JSON = os.path.abspath(SCRAPER_JSON)

    with open(SCRAPER_JSON, "r", encoding="utf-8") as f:
        videos = json.load(f)

    print(f"[spark] total videos: {len(videos)}")

    # Parallelize video entries
    rdd = spark.sparkContext.parallelize(videos, numSlices=4)

    # Process in parallel
    results = rdd.mapPartitions(process_partition).collect()

    print(f"[spark] extracted {len(results)} keyword rows")

    # Save to Postgres (TimescaleDB)
    save_keywords_bulk(results)

    spark.stop()

if __name__ == "__main__":
    main()
