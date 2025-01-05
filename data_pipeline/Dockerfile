FROM python:3.12.7-slim

# Set environment variables
ENV AIRFLOW_HOME=/data_pipeline/src/services/airflow
ENV AIRFLOW__CORE__DAGS_FOLDER=/data_pipeline/src/services/airflow/dags

WORKDIR /data_pipeline


# Install Poetry
RUN pip install poetry

# Set the working directory

# Copy requirements and install dependencies
COPY requirements.txt requirements.txt
COPY pyproject.toml pyproject.toml

RUN poetry install \
    && poetry run pip install -r requirements.txt


# Copy the rest of the application code
COPY . .

# Make the entrypoint script executable
RUN chmod +x /data_pipeline/entrypoint.sh

# Expose the necessary ports
EXPOSE 5000 8000


# Set the entrypoint
ENTRYPOINT [ "bash", "/data_pipeline/entrypoint.sh" ]
