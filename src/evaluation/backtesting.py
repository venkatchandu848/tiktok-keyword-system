from datetime import datetime, timedelta, timezone
import psycopg2

DB_HOST = "localhost"
DB_NAME = "tiktok"
DB_USER = "postgres"
DB_PASS = "postgres"


def run_backtest(days_back=7, horizon_days=3):
    """
    Re-run growth detection as if 'days_back' days ago.
    Then measure actual growth over the next 'horizon_days'.
    """
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    with conn, conn.cursor() as cur:
        fake_now = datetime.now(timezone.utc) - timedelta(days=days_back)

        # Keywords frequency up to fake_now
        cur.execute("""
            SELECT keyword, COUNT(*)
            FROM keywords
            WHERE detected_at <= %s
            GROUP BY keyword;
        """, (fake_now,))
        baseline = dict(cur.fetchall())

        # Keywords frequency horizon later
        horizon = fake_now + timedelta(days=horizon_days)
        cur.execute("""
            SELECT keyword, COUNT(*)
            FROM keywords
            WHERE detected_at <= %s
            GROUP BY keyword;
        """, (horizon,))
        future = dict(cur.fetchall())

        # Compute growth
        results = []
        for kw, count_then in baseline.items():
            count_future = future.get(kw, count_then)
            growth = (count_future - count_then) / max(count_then, 1)
            results.append((kw, count_then, count_future, growth))

    conn.close()
    return sorted(results, key=lambda x: x[3], reverse=True)
