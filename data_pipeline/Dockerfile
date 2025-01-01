FROM python:3.12.7-slim

# Set environment variables
ENV AIRFLOW_HOME=/data_pipeline/src/services/airflow
ENV SSL_CERT_DIR=/etc/ssl/certs
ENV SSL_KEY_DIR=/etc/ssl/private

# Create directories for SSL certificates
RUN mkdir -p $SSL_CERT_DIR $SSL_KEY_DIR

# Install OpenSSL and Poetry
RUN apt-get update && \
    apt-get install -y openssl && \
    pip install poetry

# Set the working directory
WORKDIR /data_pipeline

# Copy requirements and install dependencies
COPY requirements.txt requirements.txt
COPY pyproject.toml pyproject.toml
RUN poetry install && poetry run pip install -r requirements.txt

# Generate a self-signed SSL certificate
RUN openssl req -x509 -newkey rsa:2048 -keyout $SSL_KEY_DIR/private.key -out $SSL_CERT_DIR/certificate.crt -days 365 -nodes \
    -subj "/C=US/ST=State/L=City/O=Organization/OU=Unit/CN=localhost"

# Copy the rest of the application code
COPY . .

# Make the entrypoint script executable
RUN chmod +x /data_pipeline/entrypoint.sh

# Expose the necessary ports
EXPOSE 5000 8000

# Set the Airflow DAGS folder environment variable
ENV AIRFLOW__CORE__DAGS_FOLDER=/data_pipeline/src/services/airflow/dags

# Set the entrypoint
ENTRYPOINT [ "bash", "/data_pipeline/entrypoint.sh" ]
