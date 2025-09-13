# airflow_dags/Dockerfile
FROM apache/airflow:2.8.1-python3.11

# Install extra Airflow providers
COPY src/airflow_dags/requirements_dag.txt /requirements_dag.txt
RUN pip install --no-cache-dir -r /requirements_dag.txt

# Copy repo into Airflow
COPY src/airflow_dags/dags /opt/airflow/dags/

# COPY growth and metrics into airflow image
COPY src/metrics /opt/airflow/plugins/metrics
COPY src/growth /opt/airflow/plugins/growth

# Copy multi_modal preprocess into airflow image
COPY src/multi_modal /opt/airflow/plugins/multi_modal