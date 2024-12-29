FROM python:3.12.7-slim

WORKDIR /data_pipeline

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY pyproject.toml pyproject.toml
RUN poetry install

COPY . .

# Ensure the environment variables are set correctly
ENV AIRFLOW_HOME=/data_pipeline/src/services/airflow
ENV PYTHONPATH=/data_pipeline

EXPOSE 8080 8000
RUN chmod +x /data_pipeline/entrypoint.sh
ENTRYPOINT [ "bash","/data_pipeline/entrypoint.sh" ]
