# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.9.3

# Set the build argument for the user code path
ARG USER_CODE_PATH=/opt/airflow

# Copy the requirements.txt file into the Docker image
COPY requirements.txt ${USER_CODE_PATH}/requirements.txt

# Copy the .env file into the Docker image
COPY .env ${USER_CODE_PATH}/.env

# Install the Python dependencies
RUN pip install --no-cache-dir -r ${USER_CODE_PATH}/requirements.txt