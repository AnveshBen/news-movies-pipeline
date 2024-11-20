# Use official Airflow image as base
FROM apache/airflow:2.8.1-python3.9

# Switch to root user for installing packages
USER root

# Install Java and Python dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk \
    wget \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# Install Chrome and dependencies (add after first apt-get block)
# Add after the first apt-get update block
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    libgbm1 \
    libnss3 \
    libxss1 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

# Set Chromium executable path for pyppeteer
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

# Set up Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

# Download and install Spark
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "/opt/spark" \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Add after the Spark installation
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P /opt/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.261/aws-java-sdk-bundle-1.12.261.jar -P /opt/spark/jars/


# Set PYSPARK_PYTHON environment variable
ENV PYSPARK_PYTHON=/usr/local/bin/python

# Set working directory
WORKDIR /opt/airflow

# Update PYTHONPATH to include /opt/airflow
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

# Switch to airflow user for pip operations
USER airflow

# Copy requirements and install Python packages
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /opt/airflow/requirements.txt

RUN python -c "import nltk; nltk.download('punkt'); nltk.download('stopwords')"

