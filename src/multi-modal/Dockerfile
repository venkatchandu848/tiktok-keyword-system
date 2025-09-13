FROM python:3.11-slim

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    tesseract-ocr \
    libsm6 \
    libxext6 \
    libglib2.0-0 \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /multimodal

# Install Python dependencies
COPY src/multi_modal/requirements_spark.txt .

# Switch to airflow user before pip install (important!)
# USER airflow


RUN pip install --no-cache-dir -r requirements_spark.txt


# Copy multimodal folder contents into container
COPY src/multi_modal/ ./multi_modal/
COPY src/multi_modal/wait-for-it.sh ./wait-for-it.sh
RUN chmod +x ./wait-for-it.sh
# Copy scraper folder contents into container
COPY src/scraper/ ./scraper/

# Default command (can be overridden by docker-compose or spark-submit)
CMD ["bash", "-c", "./wait-for-it.sh tiktok-db:5432 --timeout=30 --strict -- python ./multi_modal/multimodal_pipeline.py && python ./multi_modal/preprocess.py"]


