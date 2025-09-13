import pandas as pd
import json
import sys
import os


def preprocess_keywords(input_path: str = "keywords.csv", output_path: str = "keywords_clean.csv"):
    # Load your raw CSV
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"❌ Input file not found: {input_path}")

    df = pd.read_csv(input_path)

    # Parse stats_json safely
    def parse_stats(row):
        try:
            return json.loads(row)
        except:
            return {}

    stats = df["stats_json"].apply(parse_stats)

    # Expand into new columns
    df["collectCount"] = stats.apply(lambda x: x.get("collectCount", 0))
    df["commentCount"] = stats.apply(lambda x: x.get("commentCount", 0))
    df["diggCount"]    = stats.apply(lambda x: x.get("diggCount", 0))
    df["playCount"]    = stats.apply(lambda x: x.get("playCount", 0))
    df["shareCount"]   = stats.apply(lambda x: x.get("shareCount", 0))

    # --------- Derived KPIs ---------
    df["engagement_total"] = (
        df["diggCount"] + df["commentCount"] + df["shareCount"] + df["collectCount"]
    )

    df["engagement_rate"] = df.apply(
        lambda row: row["engagement_total"] / row["playCount"]
        if row["playCount"] > 0 else 0,
        axis=1
    )

    df["virality_index"] = df.apply(
        lambda row: row["shareCount"] / row["playCount"]
        if row["playCount"] > 0 else 0,
        axis=1
    )

    df["discussion_rate"] = df.apply(
        lambda row: row["commentCount"] / row["playCount"]
        if row["playCount"] > 0 else 0,
        axis=1
    )

    df["save_rate"] = df.apply(
        lambda row: row["collectCount"] / row["playCount"]
        if row["playCount"] > 0 else 0,
        axis=1
    )

    # --------- Export ---------
    df.to_csv(output_path, index=False)
    print(f"✅ keywords_clean.csv generated at {output_path}")

# Allow both direct run & Airflow callable
if __name__ == "__main__":
    input_file = sys.argv[1] if len(sys.argv) > 1 else "keywords.csv"
    output_file = sys.argv[2] if len(sys.argv) > 2 else "keywords_clean.csv"
    preprocess_keywords(input_file, output_file)
