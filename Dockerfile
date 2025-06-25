FROM bitnami/spark:3.5

ENV POSTGRES_JDBC_VERSION=42.7.7
ENV HADOOP_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.367

USER root

RUN apt-get update && apt-get install -y wget libexpat1 libgdal-dev gdal-bin && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P /opt/bitnami/spark/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -P /opt/bitnami/spark/jars/ && \
    wget https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar -P /opt/bitnami/spark/jars/ && \
    apt-get remove -y wget && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

COPY conf/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
    
COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Switch back to the non-privileged spark user
#USER spark

# Expose the ports for JupyterLab, Spark Master UI, and Spark's communication
EXPOSE 8888 8080 7077