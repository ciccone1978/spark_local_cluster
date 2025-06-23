# Use a specific version of the Bitnami Spark image for reproducibility
FROM bitnami/spark:3.5

# Switch to the root user to install packages
USER root

# Copy the requirements file into the image's /tmp directory
COPY requirements.txt /tmp/requirements.txt

# Install Python packages using pip
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# Switch back to the non-privileged spark user
#USER spark

# Expose the ports for JupyterLab, Spark Master UI, and Spark's communication
EXPOSE 8888 8080 7077