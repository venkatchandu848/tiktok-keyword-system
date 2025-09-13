FROM python:3.11-slim
WORKDIR /app
COPY requirements_api.txt .
RUN pip install --no-cache-dir -r requirements_api.txt
COPY src/api/streamlit_app.py ./streamlit_app.py
COPY src/multi_modal ./multi_modal/
EXPOSE 8501
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]