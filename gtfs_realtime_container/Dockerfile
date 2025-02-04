# Use python 3.13.1-slim-bullseye as base
FROM python:3.13.1-slim-bullseye

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir \
    duckdb \
    requests \
    PyYAML \
    gtfs-realtime-bindings \
    pandas

# Set the working directory in the container
WORKDIR /app

# Add the required files into the container at /app
ADD . /app

# Set Python to run unbuffered to show logs correctly
ENV PYTHONUNBUFFERED=1

# Command to run the Python script
CMD ["python3", "gtfs_realtime_etl.py"]