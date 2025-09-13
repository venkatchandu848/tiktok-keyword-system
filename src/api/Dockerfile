FROM python:3.11-slim
WORKDIR /app

# Install Python dependencies
COPY requirements_api.txt .
RUN pip install --no-cache-dir -r requirements_api.txt

# Copy application code
COPY src/api/streamlit_app.py ./streamlit_app.py
COPY src/api/api.py ./api.py

# Optionally copy the multi_modal directory to import helpers
COPY src/multi_modal ./multi_modal/


EXPOSE 8501


CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]