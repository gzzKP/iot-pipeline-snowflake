FROM apache/spark:3.5.0

USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Pre-download Spark jars so startup is fast
ENV SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
com.clickhouse:clickhouse-jdbc:0.6.0" 
#,\
# net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.5,\
# net.snowflake:snowflake-jdbc:3.16.0"

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY pipeline.py .

# Give Kafka and ClickHouse time to start
CMD ["bash", "-c", "sleep 30 && spark-submit \
  --master local[*] \
  --packages $SPARK_PACKAGES \
  --conf spark.sql.shuffle.partitions=4 \
  /app/pipeline.py"]