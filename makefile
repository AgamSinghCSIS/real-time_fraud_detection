ENTRYPOINT := src/streaming/stream_ingestion_local_kafka.py

run_stream:
	@echo "Running Spark Streaming Job..."
	@echo "Using PYSPARK_PYTHON: $(PYSPARK_PYTHON)"
	spark-submit \
		--master local[2] \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-azure:3.3.6,org.apache.hadoop:hadoop-common:3.3.6 \
		$(ENTRYPOINT)