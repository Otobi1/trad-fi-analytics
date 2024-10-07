# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set environment variables
ENV DBT_VERSION=
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gnupg2 \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Google Cloud SDK
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update && apt-get install -y google-cloud-sdk

# Install dbt-core and dbt-bigquery (Latest version)
RUN pip install dbt-bigquery

# Set working directory
WORKDIR /app

# Copy your dbt project files to the container
COPY . /app

# Run dbt commands as needed
CMD ["dbt", "--help"]
