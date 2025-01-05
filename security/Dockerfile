FROM python:3.12.7-slim

# Set environment variables
ENV SSL_CERT_DIR=/etc/ssl/certs
ENV SSL_KEY_DIR=/etc/ssl/private

# Set the working directory
WORKDIR /ssl_certificates

# Create directories for SSL certificates
RUN mkdir -p $SSL_CERT_DIR $SSL_KEY_DIR

# Install OpenSSL and Poetry
RUN apt-get update \
    && apt-get install -y openssl \
    && openssl req -x509 -newkey rsa:2048 -keyout $SSL_KEY_DIR/private.key -out $SSL_CERT_DIR/certificate.crt -days 365 -nodes \
    -subj "/C=US/ST=State/L=City/O=Organization/OU=Unit/CN=localhost"
