"""
Growth detection job:
Detect fastest-growing keywords across regions, categories, and engagement thresholds.
Results are saved into the trending_keywords table.
"""

import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone, timedelta
import logging
from tenacity import retry, wait_exponential, stop_after_attempt

# DB config
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "tiktok")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("growth_detection")


@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )


def detect_trending_keywords(
    time_window="hourly",
    growth_threshold=0.5,
    region=None,
    category=None,
    min_likes=0,
    min_comments=0,
    min_shares=0,
    min_views=0,
    limit=20
):
    """
    Detect trending keywords with configurable filters.
    - time_window: "hourly", "daily", "weekly"
    - growth_threshold: minimum relative growth rate (e.g., 0.5 = 50%)
    - region, category: filter (None = all)
    - engagement minimums: discard videos below thresholds
    """

    window_map = {
        "hourly": ("1 hour", timedelta(hours=1)),
        "daily": ("1 day", timedelta(days=1)),
        "weekly": ("7 days", timedelta(days=7)),
    }

    if time_window not in window_map:
        raise ValueError(f"Invalid time_window={time_window}")

    bucket_interval, delta = window_map[time_window]

    now = datetime.now(timezone.utc)
    prev_start = now - 2 * delta
    prev_end = now - delta
    curr_start = now - delta
    curr_end = now

    conn = get_db_conn()
    results = []
    try:
        with conn.cursor() as cur:
            # Base query
            sql = f"""
            WITH prev AS (
                SELECT keyword, region, category, COUNT(*)::float AS mentions
                FROM keywords
                WHERE detected_at >= %s AND detected_at < %s
                  AND (stats->>'diggCount')::int >= %s
                  AND (stats->>'commentCount')::int >= %s
                  AND (stats->>'shareCount')::int >= %s
                  AND (stats->>'playCount')s::int >= %s
                  { "AND region = %s" if region else "" }
                  { "AND category = %s" if category else "" }
                GROUP BY keyword, region, category
            ),
            curr AS (
                SELECT keyword, region, category, COUNT(*)::float AS mentions
                FROM keywords
                WHERE detected_at >= %s AND detected_at < %s
                  AND (stats->>'diggCount')::int >= %s
                  AND (stats->>'commentCount')::int >= %s
                  AND (stats->>'shareCount')::int >= %s
                  AND (stats->>'playCount')s::int >= %s
                  { "AND region = %s" if region else "" }
                  { "AND category = %s" if category else "" }
                GROUP BY keyword, region, category
            )
            SELECT
                c.keyword,
                c.region,
                c.category,
                c.mentions AS current_mentions,
                COALESCE(p.mentions, 0) AS prev_mentions,
                CASE
                    WHEN COALESCE(p.mentions, 0) = 0 AND c.mentions > 1 THEN 999.0
                    WHEN COALESCE(p.mentions, 0) = 0 THEN 0
                    ELSE (c.mentions - p.mentions) / p.mentions
                END AS growth_rate
            FROM curr c
            LEFT JOIN prev p
              ON c.keyword = p.keyword
             AND c.region = p.region
             AND c.category = p.category
            WHERE (
                    CASE
                        WHEN COALESCE(p.mentions, 0) = 0 AND c.mentions > 1 THEN 999.0
                        WHEN COALESCE(p.mentions, 0) = 0
                        ELSE (c.mentions - p.mentions) / p.mentions
                   END
                ) >= %s
            ORDER BY growth_rate DESC
            LIMIT %s;
            """

            params = [
                prev_start, prev_end,
                min_likes, min_comments, min_shares, min_views,
                curr_start, curr_end,
                min_likes, min_comments, min_shares, min_views,
            ]

            if region:
                params.append(region)
                params.append(region)
            if category:
                params.append(category)
                params.append(category)

            params.extend([growth_threshold, limit])

            cur.execute(sql, params)
            results = cur.fetchall()

        # Save results into trending_keywords
        with conn.cursor() as cur:
            rows = [
                (
                    kw, reg, cat,
                    curr_end, mentions, growth_rate,
                    time_window, now
                )
                for (kw, reg, cat, mentions, prev, growth_rate) in results
            ]
            execute_values(
                cur,
                """
                INSERT INTO trending_keywords
                    (keyword, region, category, time_bucket, mentions, growth_rate, time_window, detected_at)
                VALUES %s
                """,
                rows
            )
        conn.commit()

    finally:
        conn.close()

    return results


if __name__ == "__main__":
    # Example run
    trending = detect_trending_keywords(
        time_window="hourly",
        growth_threshold=0.5,
        region="US",
        category="fashion",
        min_likes=100,
        min_comments=10,
        limit=10
    )
    print("Trending keywords:", trending)
