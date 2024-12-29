FROM python:3.12.7-slim

ENV AIRFLOW_HOME=/data_pipeline/src/services/airflow

WORKDIR /data_pipeline

RUN pip install poetry
COPY requirements.txt requirements.txt

COPY pyproject.toml pyproject.toml
RUN poetry install \
    && poetry run pip install -r requirements.txt

COPY . .

RUN chmod +x /data_pipeline/entrypoint.sh

EXPOSE 5000 8000
ENV AIRFLOW__CORE__DAGS_FOLDER=/data_pipeline/src/services/airflow/dags

ENTRYPOINT [ "bash","/data_pipeline/entrypoint.sh" ]
