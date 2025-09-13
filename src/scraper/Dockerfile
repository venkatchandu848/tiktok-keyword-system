# Use the official Playwright base (Chromium already included)
FROM mcr.microsoft.com/playwright/python:v1.48.0-jammy

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxrandr2 \
    libxdamage1 \
    libxfixes3 \
    libxrender1 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    fonts-liberation \
    libasound2 \
    libxshmfence1 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements_scraper.txt .
RUN pip install --no-cache-dir -r requirements_scraper.txt

# Install Chromium for Playwright
RUN playwright install --with-deps chromium

# Copy scraper code
COPY scraper1.py .

CMD ["python", "scraper1.py"]