# Multi-stage build for production optimization
FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    wget \
    unzip \
    procps \
    net-tools \
    openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment for Spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Development stage
FROM base as development

WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt pyproject.toml ./

# Install Python dependencies
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt && \
    pip install -e .

# Copy source code
COPY . .

# Expose ports for various services
EXPOSE 8000 8501 8080 4040 18080

# Default command for development
CMD ["uvicorn", "src.dashboard.api:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

# Production stage
FROM base as production

# Create non-root user
RUN useradd --create-home --shell /bin/bash app

WORKDIR /app

# Copy requirements and install production dependencies
COPY requirements.txt pyproject.toml ./
RUN pip install --upgrade pip setuptools wheel && \
    pip install --no-dev -r requirements.txt && \
    pip install .

# Copy source code and set ownership
COPY --chown=app:app . .

# Switch to non-root user
USER app

# Expose application port
EXPOSE 8000

# Production command
CMD ["uvicorn", "src.dashboard.api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]

# Spark worker stage
FROM base as spark-worker

WORKDIR /app

# Install Spark-specific dependencies
COPY requirements.txt pyproject.toml ./
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

# Download and install Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" /opt/spark && \
    rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Set Spark environment
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*-src.zip

# Copy application code
COPY --chown=app:app . .

# Create non-root user for Spark
RUN useradd --create-home --shell /bin/bash spark
USER spark

# Expose Spark ports
EXPOSE 7077 8080 8081 4040

# Default Spark command
CMD ["/opt/spark/bin/spark-submit", "--class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077"]

# MLflow stage
FROM base as mlflow-server

WORKDIR /app

# Install MLflow and dependencies
COPY requirements.txt ./
RUN pip install --upgrade pip && \
    pip install mlflow psycopg2-binary boto3

# Create MLflow user
RUN useradd --create-home --shell /bin/bash mlflow
USER mlflow

# Expose MLflow port
EXPOSE 5000

# MLflow server command
CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000"]