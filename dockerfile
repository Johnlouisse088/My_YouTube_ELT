# Set the Airflow version to use (can be changed when building the image)
ARG AIRFLOW_VERSION=2.9.2

# Set the Python version to use
ARG PYTHON_VERSION=3.10

# Use the official Apache Airflow image with the specified Airflow and Python versions
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Set the default Airflow home directory inside the container
ENV AIRFLOW_HOME=/opt/airflow

# Copy the requirements.txt file from your project into the container
COPY requirements.txt /

# Install the specified Airflow version and all Python packages listed in requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt