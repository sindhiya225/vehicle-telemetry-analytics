from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import yaml
import json
import os

class SparkStreamingSession:
    def __init__(self, app_name: str = "VehicleTelemetryStreaming"):
        """Initialize Spark session with streaming capabilities"""
        
        # Load configuration
        with open('streaming/kafka/kafka_config.yaml', 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Create Spark session
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                   "org.apache.spark:spark-avro_2.12:3.3.0,"
                   "io.delta:delta-core_2.12:2.3.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", 
                   "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.default.parallelism", "8") \
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
            .config("spark.sql.streaming.checkpointLocation", 
                   "/tmp/spark-checkpoints") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kryoserializer.buffer.max", "512m") \
            .getOrCreate()
        
        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Define schemas
        self.define_schemas()
    
    def define_schemas(self):
        """Define Avro schemas for structured streaming"""
        
        # Telemetry event schema
        self.telemetry_schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("timestamp", LongType(), False),
            StructField("vehid", IntegerType(), False),
            StructField("vehicle_type", StringType(), False),
            StructField("vehicle_class", StringType(), False),
            StructField("trip_id", StringType(), False),
            StructField("trip_distance_km", DoubleType(), False),
            StructField("fuel_consumed_l", DoubleType(), False),
            StructField("fuel_efficiency_kmpl", DoubleType(), False),
            StructField("avg_speed_kmph", DoubleType(), False),
            StructField("avg_rpm", IntegerType(), False),
            StructField("avg_engine_temp_c", DoubleType(), False),
            StructField("idle_time_min", DoubleType(), False),
            StructField("has_fault", BooleanType(), False),
            StructField("fault_codes", ArrayType(StringType()), True),
            StructField("maintenance_flag", BooleanType(), False),
            StructField("weather_condition", StringType(), False),
            StructField("road_type", StringType(), False),
            StructField("driver_behavior_score", DoubleType(), False),
            StructField("location_lat", DoubleType(), True),
            StructField("location_lon", DoubleType(), True),
            StructField("altitude", DoubleType(), True)
        ])
        
        # Vehicle master schema
        self.vehicle_schema = StructType([
            StructField("vehid", IntegerType(), False),
            StructField("vehicle_type", StringType(), False),
            StructField("vehicle_class", StringType(), False),
            StructField("engine_configuration", StringType(), True),
            StructField("transmission", StringType(), True),
            StructField("drive_wheels", StringType(), True),
            StructField("generalized_weight", IntegerType(), True),
            StructField("engine_cylinders", IntegerType(), True),
            StructField("engine_type", StringType(), True),
            StructField("displacement_l", DoubleType(), True),
            StructField("is_turbo", BooleanType(), False),
            StructField("is_hybrid", BooleanType(), False),
            StructField("power_index", DoubleType(), True),
            StructField("transmission_type", StringType(), True),
            StructField("transmission_speeds", IntegerType(), True),
            StructField("weight_category", StringType(), True),
            StructField("efficiency_score", DoubleType(), True),
            StructField("efficiency_category", StringType(), True)
        ])
    
    def read_from_kafka(self, topic: str, starting_offsets: str = "latest"):
        """Read stream from Kafka topic"""
        
        kafka_options = {
            "kafka.bootstrap.servers": self.config['kafka']['bootstrap_servers'],
            "subscribe": topic,
            "startingOffsets": starting_offsets,
            "kafka.security.protocol": "PLAINTEXT",
            "failOnDataLoss": "false"
        }
        
        if "schema.registry" in topic:
            kafka_options["value.deserializer"] = \
                "io.confluent.kafka.serializers.KafkaAvroDeserializer"
            kafka_options["schema.registry.url"] = \
                self.config['kafka']['schema_registry_url']
        
        return self.spark.readStream \
            .format("kafka") \
            .options(**kafka_options) \
            .load()
    
    def parse_avro_stream(self, stream_df, schema):
        """Parse Avro encoded Kafka stream"""
        
        from pyspark.sql.avro.functions import from_avro
        
        # Convert binary value to string and parse JSON
        json_df = stream_df.select(
            col("key").cast("string"),
            from_avro(col("value"), json.dumps(schema)).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset")
        )
        
        # Extract all fields
        result_df = json_df.select("data.*", "kafka_timestamp", "partition", "offset")
        
        return result_df
    
    def create_streaming_aggregations(self, telemetry_df):
        """Create streaming aggregations on telemetry data"""
        
        # Watermark for handling late data
        telemetry_df = telemetry_df.withWatermark("timestamp", "5 minutes")
        
        # 1. Vehicle-level aggregations (5-minute windows)
        vehicle_agg = telemetry_df.groupBy(
            window(col("timestamp"), "5 minutes"),
            col("vehid"),
            col("vehicle_type"),
            col("vehicle_class")
        ).agg(
            count("*").alias("trip_count"),
            sum("trip_distance_km").alias("total_distance_km"),
            sum("fuel_consumed_l").alias("total_fuel_consumed_l"),
            avg("fuel_efficiency_kmpl").alias("avg_fuel_efficiency"),
            avg("avg_speed_kmph").alias("avg_speed"),
            sum("idle_time_min").alias("total_idle_min"),
            sum(when(col("has_fault"), 1).otherwise(0)).alias("fault_count"),
            sum(when(col("maintenance_flag"), 1).otherwise(0)).alias("maintenance_count"),
            avg("driver_behavior_score").alias("avg_driver_score"),
            last("location_lat").alias("last_lat"),
            last("location_lon").alias("last_lon")
        )
        
        # 2. Fleet-level aggregations (1-minute windows)
        fleet_agg = telemetry_df.groupBy(
            window(col("timestamp"), "1 minute"),
            col("vehicle_type")
        ).agg(
            approx_count_distinct("vehid").alias("active_vehicles"),
            sum("trip_distance_km").alias("total_distance"),
            sum("fuel_consumed_l").alias("total_fuel"),
            avg("fuel_efficiency_kmpl").alias("avg_efficiency"),
            sum(when(col("has_fault"), 1).otherwise(0)).alias("total_faults"),
            collect_list(
                struct(
                    col("vehid"),
                    col("fuel_efficiency_kmpl"),
                    col("has_fault")
                )
            ).alias("vehicle_details")
        )
        
        # 3. Real-time efficiency monitoring
        efficiency_stream = telemetry_df.groupBy(
            window(col("timestamp"), "10 minutes", "5 minutes"),
            col("vehicle_type"),
            col("vehicle_class")
        ).agg(
            count("*").alias("sample_count"),
            expr("percentile_approx(fuel_efficiency_kmpl, 0.5)").alias("median_efficiency"),
            expr("percentile_approx(fuel_efficiency_kmpl, 0.25)").alias("p25_efficiency"),
            expr("percentile_approx(fuel_efficiency_kmpl, 0.75)").alias("p75_efficiency"),
            min("fuel_efficiency_kmpl").alias("min_efficiency"),
            max("fuel_efficiency_kmpl").alias("max_efficiency"),
            stddev("fuel_efficiency_kmpl").alias("efficiency_stddev")
        ).withColumn("efficiency_range", 
                    col("p75_efficiency") - col("p25_efficiency"))
        
        return {
            "vehicle_aggregations": vehicle_agg,
            "fleet_aggregations": fleet_agg,
            "efficiency_monitoring": efficiency_stream
        }
    
    def detect_anomalies(self, telemetry_df):
        """Real-time anomaly detection using streaming ML"""
        
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.clustering import StreamingKMeans
        
        # Prepare features for anomaly detection
        feature_cols = [
            "fuel_efficiency_kmpl",
            "avg_speed_kmph",
            "avg_rpm",
            "avg_engine_temp_c",
            "idle_time_min"
        ]
        
        # Assemble features
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        feature_df = assembler.transform(telemetry_df)
        
        # Initialize Streaming K-Means for anomaly detection
        streaming_kmeans = StreamingKMeans(
            k=3,  # Number of clusters
            decayFactor=0.5,  # Forgetting factor
            timeUnit="seconds"
        )
        
        # Train model on stream
        model = streaming_kmeans.fit(feature_df.select("features"))
        
        # Predict clusters and calculate distances
        predictions = model.transform(feature_df)
        
        # Calculate anomaly score (distance to centroid)
        anomalies = predictions.withColumn(
            "anomaly_score",
            expr("""
                CASE 
                    WHEN prediction = 0 THEN 1 - (features[0] / 30)
                    WHEN prediction = 1 THEN 1 - (features[1] / 120)
                    WHEN prediction = 2 THEN 1 - (features[2] / 3000)
                    ELSE 0.5
                END
            """)
        ).filter(col("anomaly_score") > 0.8)  # Threshold for anomalies
        
        return anomalies
    
    def calculate_cost_savings(self, telemetry_df):
        """Calculate real-time cost savings opportunities"""
        
        # Current fuel price (could be from external API)
        fuel_price_per_liter = 1.2  # USD
        
        cost_savings = telemetry_df.groupBy(
            window(col("timestamp"), "15 minutes"),
            col("vehid"),
            col("vehicle_type")
        ).agg(
            sum("fuel_consumed_l").alias("fuel_consumed_l"),
            sum("trip_distance_km").alias("distance_km"),
            avg("fuel_efficiency_kmpl").alias("current_efficiency"),
            sum(when(col("idle_time_min") > 5, col("idle_time_min")).otherwise(0)).alias("excess_idle_min")
        ).withColumn(
            "fuel_cost",
            col("fuel_consumed_l") * fuel_price_per_liter
        ).withColumn(
            "potential_savings",
            expr("""
                (fuel_consumed_l * 0.15) * {} +  -- 15% efficiency improvement
                (excess_idle_min / 60 * 0.5 * {})  -- Idle time reduction
            """.format(fuel_price_per_liter, fuel_price_per_liter)
            )
        )
        
        return cost_savings
    
    def write_to_kafka(self, df, topic, checkpoint_location=None):
        """Write streaming DataFrame to Kafka"""
        
        kafka_options = {
            "kafka.bootstrap.servers": self.config['kafka']['bootstrap_servers'],
            "topic": topic
        }
        
        query = df.writeStream \
            .format("kafka") \
            .outputMode("update") \
            .options(**kafka_options)
        
        if checkpoint_location:
            query = query.option("checkpointLocation", checkpoint_location)
        
        return query.start()
    
    def write_to_delta(self, df, table_path, checkpoint_location=None):
        """Write streaming DataFrame to Delta Lake"""
        
        query = df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("path", table_path) \
            .option("checkpointLocation", 
                   checkpoint_location or f"/tmp/delta-checkpoints/{table_path}")
        
        return query.start()
    
    def write_to_console(self, df, truncate=False):
        """Write streaming DataFrame to console (for debugging)"""
        
        return df.writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", str(truncate)) \
            .start()
    
    def await_termination(self, queries, timeout=None):
        """Wait for streaming queries to terminate"""
        
        from threading import Thread
        
        # Start all queries
        for query in queries:
            Thread(target=query.awaitTermination).start()
        
        # Wait for termination
        try:
            self.spark.streams.awaitAnyTermination(timeout)
        except KeyboardInterrupt:
            print("Stopping streaming queries...")
            for query in queries:
                query.stop()
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()