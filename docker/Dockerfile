# QuickChat Spark ETL Docker Image
FROM apache/spark:3.5.0-scala2.12-java17-ubuntu

USER root

# Install additional dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV SPARK_CONF_DIR=/opt/spark/conf

# Create directories
RUN mkdir -p /opt/spark-etl/lib \
    && mkdir -p /opt/spark-etl/conf \
    && mkdir -p /opt/spark-etl/logs \
    && mkdir -p /data/lake

# Copy Spark configuration
COPY spark-defaults.conf $SPARK_CONF_DIR/spark-defaults.conf
COPY log4j2.properties $SPARK_CONF_DIR/log4j2.properties

# Download Delta Lake and other JARs
RUN wget -q -P $SPARK_HOME/jars/ https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    wget -q -P $SPARK_HOME/jars/ https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar && \
    wget -q -P $SPARK_HOME/jars/ https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.2.1/mongo-spark-connector_2.12-10.2.1.jar && \
    wget -q -P $SPARK_HOME/jars/ https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.10.2/mongodb-driver-sync-4.10.2.jar && \
    wget -q -P $SPARK_HOME/jars/ https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.10.2/mongodb-driver-core-4.10.2.jar && \
    wget -q -P $SPARK_HOME/jars/ https://repo1.maven.org/maven2/org/mongodb/bson/4.10.2/bson-4.10.2.jar && \
    wget -q -P $SPARK_HOME/jars/ https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.33/mysql-connector-java-8.0.33.jar && \
    wget -q -P $SPARK_HOME/jars/ https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    wget -q -P $SPARK_HOME/jars/ https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    wget -q -P $SPARK_HOME/jars/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q -P $SPARK_HOME/jars/ https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.560/aws-java-sdk-bundle-1.12.560.jar

# Copy application JAR (will be built)
COPY target/scala-2.12/quickchat-spark-etl.jar /opt/spark-etl/lib/

# Copy configuration
COPY src/main/resources/application.conf /opt/spark-etl/conf/

# Set working directory
WORKDIR /opt/spark-etl

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:4040/api/v1/applications || exit 1

# Default command
CMD ["spark-submit", \
     "--class", "com.quickchat.etl.jobs.bronze.BronzeMessagesJob", \
     "--master", "local[*]", \
     "--driver-memory", "2g", \
     "--executor-memory", "2g", \
     "/opt/spark-etl/lib/quickchat-spark-etl.jar"]
