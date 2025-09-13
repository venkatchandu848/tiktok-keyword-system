import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
import os

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "tiktok")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")


def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )


def collect_pipeline_metrics():
    conn = get_db_conn()
    metrics = []
    now = datetime.now(timezone.utc)

    with conn, conn.cursor() as cur:
        # 1) Videos processed in last 24h
        cur.execute("""
            SELECT COUNT(DISTINCT video_id)
            FROM keywords
            WHERE detected_at >= now() - interval '1 day';
        """)
        videos_24h = cur.fetchone()[0] or 0
        metrics.append(("videos_processed_last_24h", videos_24h, now))

        # 2) Keywords extracted in last 24h
        cur.execute("""
            SELECT COUNT(*)
            FROM keywords
            WHERE detected_at >= now() - interval '1 day';
        """)
        kws_24h = cur.fetchone()[0] or 0
        metrics.append(("keywords_extracted_last_24h", kws_24h, now))

        # 3) Unique keywords in last 24h
        cur.execute("""
            SELECT COUNT(DISTINCT keyword)
            FROM keywords
            WHERE detected_at >= now() - interval '1 day';
        """)
        unique_kws = cur.fetchone()[0] or 0
        metrics.append(("unique_keywords_last_24h", unique_kws, now))

        # 4) Failed / missing stats (sanity check)
        cur.execute("""
            SELECT COUNT(*)
            FROM keywords
            WHERE stats IS NULL;
        """)
        missing_stats = cur.fetchone()[0] or 0
        metrics.append(("rows_missing_stats", missing_stats, now))

        # Insert into pipeline_metrics
        execute_values(
            cur,
            "INSERT INTO pipeline_metrics (metric_name, metric_value, metric_time) VALUES %s",
            metrics
        )

    conn.close()
    print(f"[metrics] inserted {len(metrics)} metrics at {now}")
