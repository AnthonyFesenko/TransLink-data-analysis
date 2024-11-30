# This dockerfile was adapted from 
# https://www.kdnuggets.com/containerize-python-apps-with-docker-in-5-easy-steps

# Use Python 3.12.5 as base image
FROM python:3.12.5

# Install dependancies
RUN pip install --no-cache-dir --upgrade pip && \ 
pip install --no-cache-dir duckdb requests PyYAML gtfs-realtime-bindings pandas


# Set the working directory in the container
WORKDIR /app

# add the required files into the container at /app
ADD . /app

# Command to run the Python script
CMD ["python3", "gtfs_realtime_etl.py"]