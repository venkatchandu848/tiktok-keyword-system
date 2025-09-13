# Tiktok Trending Keywords Extraction Pipeline

This project builds an **end-to-end pipeline** for detecting trending tiktok keywords using
- **Scraping**
- **Multimodal processing (with Spark support)**
- **Database storage (POstgres/TimescaleDB)**
- **Airflow Orchestration**
- **Growth detection & Metrics**
- **API & Dashboard visualization**

It supports running both with Docker (recommended) and without a docker (using a single python environment)
 
----

## 📂 Project Structure

tiktok-trending-pipeline/
│
├── src/
│ ├── scraper/
│ │ ├── scraper1.py
│ │ ├── requirements_scraper.txt
│ │ ├── Dockerfile
│ │ ├── tiktok_trending.json
│ ├── multi_modal/
│ │ ├── multimodal_pipeline.py
│ │ ├── multimodal_spark.py
│ │ ├── preprocess.py
│ │ ├── requirements_spark.txt
│ │ ├── Dockerfile
│ │ └── wait-for-it.sh
│ ├── growth/
│ │ ├── growth_detection.py
│ ├── metrics/
│ │ ├── metrics_collector.py
│ ├── evaluation/
│ │ ├── backtesting.py
│ ├── database/
│ │ ├── full_file.sql
│ │ └── README.md
│ ├── airflow_dags/
│ │ ├── dags/tiktok_pipeline_dag.py
│ │ ├── requirements_dag.txt
│ │ └── Dockerfile
│ ├── api/
│ │ ├── streamlit_app.py
│ │ ├── requirements_api.txt
│ │ └── Dockerfile
│ ├── dashboard/
│ │ ├── Book2.twb
├── docker-compose.yml
├── requirements_all.txt
└── README.md

--- 


## ⚡ Workflow Overview

- **Scraper (`scraper1.py`)**
  - Collects trending TikTok video metadata.
  - Saves as `tiktok_trending.json`.

- **Multimodal Pipeline (`multimodal_pipeline.py` / `multimodal_spark.py`)**
  - Processes video metadata → extracts keywords, embeddings, features.
  - Outputs `keywords.csv`.

- **Preprocessing (`preprocess.py`)**
  - Cleans `keywords.csv`.
  - Saves as `keywords_clean.csv`.

- **Database (TimescaleDB/Postgres)**
  - Stores keywords and metadata.
  - Provides hypertables, indexes, and rollups.

- **Metrics, growth, evaluation (`growth_detection.py`, `backtesting.py`, `metrics_collector.py`)**
  - Backtesting: Testing with historical data.
  - Growth: Growth detection with threshold and engagement variables.
  - Metrics: Unique keywords, keywords extracted metrics extracted.

- **API, Dashboard (Tableau, Streamlit with FastAPI)**
  - Tableau Dashboard is created using `keywords_clean.csv` file.
  - Visualizes top keywords, engagement rates, virality vs discussion, and category metrics.
  - Streamlit also does the same thing to be interactive.



## Running without docker

### 1. Setup virtual environment and install dependencies
```bash
python3 -m venv venv
source venv/bin/activate    # Linux/Mac
venv\Scripts\activate       # Windows
pip install --upgrade pip
pip install -r requirements_all.txt
```

### 2. Run components
- **Scraper**
  Run the scraper to fetch trending tiktok videos:

  ```bash
  python src/scraper/scraper1.py
    
