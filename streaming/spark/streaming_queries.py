"""
Spark Streaming Queries for Vehicle Telemetry Analytics Platform.
Real-time processing and analytics using Apache Spark Structured Streaming.
"""
import logging
import json
import time
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, window, 
    avg, max, min, sum, count, stddev, percentile_approx,
    udf, when, lit, expr, from_unixtime, unix_timestamp,
    date_format, hour, minute, second, dayofweek, month, year,
    lag, lead, row_number, rank, dense_rank, ntile,
    sha2, md5, concat, concat_ws, split, explode,
    array, array_contains, size, posexplode,
    regexp_extract, regexp_replace, trim, lower, upper,
    cos, sin, radians, atan2, sqrt, pow,
    current_timestamp, current_date, datediff
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, DoubleType, BooleanType, TimestampType,
    ArrayType, MapType, LongType, DateType,
    DecimalType, BinaryType
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import numpy as np
from scipy import stats
import pandas as pd

logger = logging.getLogger(__name__)


class StreamingQueryType(Enum):
    """Types of streaming queries."""
    REAL_TIME_AGGREGATION = "real_time_aggregation"
    ANOMALY_DETECTION = "anomaly_detection"
    PATTERN_MATCHING = "pattern_matching"
    JOIN_OPERATIONS = "join_operations"
    WINDOW_OPERATIONS = "window_operations"
    STATEFUL_PROCESSING = "stateful_processing"


@dataclass
class StreamingQueryConfig:
    """Streaming query configuration."""
    query_type: StreamingQueryType
    input_topics: List[str]
    output_topic: str
    watermark_duration: str = "10 minutes"
    window_duration: str = "5 minutes"
    sliding_duration: str = "1 minute"
    checkpoint_location: str = "/tmp/spark-checkpoints"
    processing_time: str = "0 seconds"  # Trigger interval
    output_mode: str = "update"  # append, update, complete
    query_name: str = "streaming_query"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'query_type': self.query_type.value,
            'input_topics': self.input_topics,
            'output_topic': self.output_topic,
            'watermark_duration': self.watermark_duration,
            'window_duration': self.window_duration,
            'sliding_duration': self.sliding_duration,
            'checkpoint_location': self.checkpoint_location,
            'processing_time': self.processing_time,
            'output_mode': self.output_mode,
            'query_name': self.query_name
        }


class TelemetrySchema:
    """Schema definitions for telemetry data."""
    
    @staticmethod
    def get_telemetry_schema() -> StructType:
        """Get telemetry data schema."""
        return StructType([
            StructField("message_id", StringType(), True),
            StructField("vehicle_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("speed_kmh", DoubleType(), True),
            StructField("rpm", DoubleType(), True),
            StructField("engine_temperature_c", DoubleType(), True),
            StructField("coolant_temperature_c", DoubleType(), True),
            StructField("oil_pressure_kpa", DoubleType(), True),
            StructField("fuel_level_percent", DoubleType(), True),
            StructField("battery_voltage", DoubleType(), True),
            StructField("throttle_position_percent", DoubleType(), True),
            StructField("brake_pressure_bar", DoubleType(), True),
            StructField("gear_position", IntegerType(), True),
            StructField("odometer_km", DoubleType(), True),
            StructField("ambient_temperature_c", DoubleType(), True),
            StructField("tire_pressure_front_left", DoubleType(), True),
            StructField("tire_pressure_front_right", DoubleType(), True),
            StructField("tire_pressure_rear_left", DoubleType(), True),
            StructField("tire_pressure_rear_right", DoubleType(), True),
            StructField("acceleration_x", DoubleType(), True),
            StructField("acceleration_y", DoubleType(), True),
            StructField("acceleration_z", DoubleType(), True),
            StructField("fault_codes", ArrayType(StringType()), True),
            StructField("warnings", ArrayType(StringType()), True),
            StructField("processing_time_ms", DoubleType(), True),
            StructField("source_system", StringType(), True),
            StructField("raw_data", StringType(), True)
        ])
    
    @staticmethod
    def get_aggregate_schema() -> StructType:
        """Get aggregate data schema."""
        return StructType([
            StructField("vehicle_id", StringType(), True),
            StructField("window_start", TimestampType(), True),
            StructField("window_end", TimestampType(), True),
            StructField("metric", StringType(), True),
            StructField("count", LongType(), True),
            StructField("sum_value", DoubleType(), True),
            StructField("avg_value", DoubleType(), True),
            StructField("min_value", DoubleType(), True),
            StructField("max_value", DoubleType(), True),
            StructField("std_value", DoubleType(), True),
            StructField("p25", DoubleType(), True),
            StructField("p50", DoubleType(), True),
            StructField("p75", DoubleType(), True),
            StructField("p95", DoubleType(), True)
        ])
    
    @staticmethod
    def get_anomaly_schema() -> StructType:
        """Get anomaly detection schema."""
        return StructType([
            StructField("vehicle_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("metric", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("expected_min", DoubleType(), True),
            StructField("expected_max", DoubleType(), True),
            StructField("anomaly_score", DoubleType(), True),
            StructField("is_anomaly", BooleanType(), True),
            StructField("severity", StringType(), True),
            StructField("description", StringType(), True)
        ])


class StreamingQueryBuilder:
    """Builder for Spark streaming queries."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.queries = {}
    
    def create_streaming_df(self, kafka_bootstrap_servers: str, 
                           topics: List[str], schema: StructType) -> DataFrame:
        """Create streaming DataFrame from Kafka."""
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", ",".join(topics)) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 10000) \
            .load()
        
        # Parse JSON value
        df = df.selectExpr("CAST(value AS STRING) as json_string")
        df = df.select(from_json(col("json_string"), schema).alias("data"))
        df = df.select("data.*")
        
        return df
    
    def build_real_time_aggregation_query(self, config: StreamingQueryConfig,
                                         kafka_bootstrap_servers: str) -> DataFrame:
        """Build real-time aggregation query."""
        # Create streaming DataFrame
        telemetry_df = self.create_streaming_df(
            kafka_bootstrap_servers,
            config.input_topics,
            TelemetrySchema.get_telemetry_schema()
        )
        
        # Add watermark
        telemetry_df = telemetry_df \
            .withWatermark("timestamp", config.watermark_duration)
        
        # Define aggregation metrics
        # Speed aggregation
        speed_agg = telemetry_df \
            .groupBy(
                window(col("timestamp"), config.window_duration, config.sliding_duration),
                col("vehicle_id")
            ) \
            .agg(
                count("speed_kmh").alias("count"),
                avg("speed_kmh").alias("avg_speed"),
                min("speed_kmh").alias("min_speed"),
                max("speed_kmh").alias("max_speed"),
                stddev("speed_kmh").alias("std_speed"),
                percentile_approx("speed_kmh", 0.25).alias("p25_speed"),
                percentile_approx("speed_kmh", 0.50).alias("p50_speed"),
                percentile_approx("speed_kmh", 0.75).alias("p75_speed"),
                percentile_approx("speed_kmh", 0.95).alias("p95_speed")
            ) \
            .select(
                col("vehicle_id"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                lit("speed_kmh").alias("metric"),
                col("count"),
                col("avg_speed").alias("avg_value"),
                col("min_speed").alias("min_value"),
                col("max_speed").alias("max_value"),
                col("std_speed").alias("std_value"),
                col("p25_speed").alias("p25"),
                col("p50_speed").alias("p50"),
                col("p75_speed").alias("p75"),
                col("p95_speed").alias("p95")
            )
        
        # Temperature aggregation
        temp_agg = telemetry_df \
            .groupBy(
                window(col("timestamp"), config.window_duration, config.sliding_duration),
                col("vehicle_id")
            ) \
            .agg(
                count("engine_temperature_c").alias("count"),
                avg("engine_temperature_c").alias("avg_temp"),
                min("engine_temperature_c").alias("min_temp"),
                max("engine_temperature_c").alias("max_temp"),
                stddev("engine_temperature_c").alias("std_temp")
            ) \
            .select(
                col("vehicle_id"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                lit("engine_temperature_c").alias("metric"),
                col("count"),
                col("avg_temp").alias("avg_value"),
                col("min_temp").alias("min_value"),
                col("max_temp").alias("max_value"),
                col("std_temp").alias("std_value")
            )
        
        # Combine aggregations
        result_df = speed_agg.union(temp_agg)
        
        return result_df
    
    def build_anomaly_detection_query(self, config: StreamingQueryConfig,
                                     kafka_bootstrap_servers: str) -> DataFrame:
        """Build anomaly detection query."""
        # Create streaming DataFrame
        telemetry_df = self.create_streaming_df(
            kafka_bootstrap_servers,
            config.input_topics,
            TelemetrySchema.get_telemetry_schema()
        )
        
        # Define UDF for anomaly detection
        @udf(returnType=StructType([
            StructField("is_anomaly", BooleanType()),
            StructField("anomaly_score", DoubleType()),
            StructField("severity", StringType()),
            StructField("description", StringType())
        ]))
        def detect_anomaly(current_value: float, historical_values: List[float]) -> Dict[str, Any]:
            """Detect anomaly using statistical methods."""
            if not historical_values or len(historical_values) < 10:
                return {
                    "is_anomaly": False,
                    "anomaly_score": 0.0,
                    "severity": "low",
                    "description": "Insufficient data"
                }
            
            # Calculate statistics
            mean = np.mean(historical_values)
            std = np.std(historical_values)
            
            if std == 0:
                std = 0.001
            
            # Calculate z-score
            z_score = abs((current_value - mean) / std)
            
            # Determine anomaly
            is_anomaly = z_score > 3.0
            anomaly_score = min(z_score / 6.0, 1.0)  # Normalized to 0-1
            
            # Determine severity
            if z_score > 5.0:
                severity = "critical"
            elif z_score > 4.0:
                severity = "high"
            elif z_score > 3.0:
                severity = "medium"
            else:
                severity = "low"
            
            # Generate description
            if is_anomaly:
                direction = "above" if current_value > mean else "below"
                description = f"Value is {direction} expected range (z-score: {z_score:.2f})"
            else:
                description = "Value is within expected range"
            
            return {
                "is_anomaly": is_anomaly,
                "anomaly_score": anomaly_score,
                "severity": severity,
                "description": description
            }
        
        # Create window for historical data
        window_spec = Window \
            .partitionBy("vehicle_id") \
            .orderBy("timestamp") \
            .rowsBetween(-100, -1)  # Last 100 values
        
        # Add historical values column
        telemetry_df = telemetry_df \
            .withColumn("historical_speeds", 
                       F.collect_list("speed_kmh").over(window_spec)) \
            .withColumn("historical_temps", 
                       F.collect_list("engine_temperature_c").over(window_spec))
        
        # Detect anomalies for speed
        speed_anomalies = telemetry_df \
            .withColumn("speed_anomaly", 
                       detect_anomaly(col("speed_kmh"), col("historical_speeds"))) \
            .select(
                col("vehicle_id"),
                col("timestamp"),
                lit("speed_kmh").alias("metric"),
                col("speed_kmh").alias("value"),
                col("speed_anomaly.is_anomaly"),
                col("speed_anomaly.anomaly_score"),
                col("speed_anomaly.severity"),
                col("speed_anomaly.description")
            ) \
            .where(col("is_anomaly") == True)
        
        # Detect anomalies for temperature
        temp_anomalies = telemetry_df \
            .withColumn("temp_anomaly", 
                       detect_anomaly(col("engine_temperature_c"), col("historical_temps"))) \
            .select(
                col("vehicle_id"),
                col("timestamp"),
                lit("engine_temperature_c").alias("metric"),
                col("engine_temperature_c").alias("value"),
                col("temp_anomaly.is_anomaly"),
                col("temp_anomaly.anomaly_score"),
                col("temp_anomaly.severity"),
                col("temp_anomaly.description")
            ) \
            .where(col("is_anomaly") == True)
        
        # Combine anomalies
        result_df = speed_anomalies.union(temp_anomalies)
        
        # Add expected ranges (simplified)
        result_df = result_df \
            .withColumn("expected_min", lit(0.0)) \
            .withColumn("expected_max", 
                       when(col("metric") == "speed_kmh", lit(120.0))
                       .when(col("metric") == "engine_temperature_c", lit(100.0))
                       .otherwise(lit(0.0)))
        
        return result_df
    
    def build_pattern_matching_query(self, config: StreamingQueryConfig,
                                    kafka_bootstrap_servers: str) -> DataFrame:
        """Build pattern matching query for detecting driving patterns."""
        # Create streaming DataFrame
        telemetry_df = self.create_streaming_df(
            kafka_bootstrap_servers,
            config.input_topics,
            TelemetrySchema.get_telemetry_schema()
        )
        
        # Define patterns using window operations
        
        # Pattern 1: Harsh braking
        # Sudden drop in speed with high brake pressure
        harsh_braking = telemetry_df \
            .withWatermark("timestamp", config.watermark_duration) \
            .groupBy(
                window(col("timestamp"), "30 seconds", "10 seconds"),
                col("vehicle_id")
            ) \
            .agg(
                min("speed_kmh").alias("min_speed"),
                max("speed_kmh").alias("max_speed"),
                max("brake_pressure_bar").alias("max_brake"),
                avg("acceleration_x").alias("avg_accel_x")
            ) \
            .where(
                (col("max_speed") - col("min_speed") > 20) &  # Speed drop > 20 km/h
                (col("max_brake") > 30) &                     # High brake pressure
                (col("avg_accel_x") < -2.0)                   # Negative acceleration
            ) \
            .select(
                col("vehicle_id"),
                col("window.start").alias("pattern_start"),
                col("window.end").alias("pattern_end"),
                lit("harsh_braking").alias("pattern_type"),
                lit("Sudden braking detected").alias("description"),
                col("max_speed").alias("speed_before"),
                col("min_speed").alias("speed_after"),
                col("max_brake").alias("brake_pressure")
            )
        
        # Pattern 2: Rapid acceleration
        rapid_acceleration = telemetry_df \
            .withWatermark("timestamp", config.watermark_duration) \
            .groupBy(
                window(col("timestamp"), "30 seconds", "10 seconds"),
                col("vehicle_id")
            ) \
            .agg(
                min("speed_kmh").alias("min_speed"),
                max("speed_kmh").alias("max_speed"),
                max("throttle_position_percent").alias("max_throttle"),
                avg("acceleration_x").alias("avg_accel_x")
            ) \
            .where(
                (col("max_speed") - col("min_speed") > 30) &  # Speed increase > 30 km/h
                (col("max_throttle") > 80) &                  # High throttle position
                (col("avg_accel_x") > 3.0)                    # High acceleration
            ) \
            .select(
                col("vehicle_id"),
                col("window.start").alias("pattern_start"),
                col("window.end").alias("pattern_end"),
                lit("rapid_acceleration").alias("pattern_type"),
                lit("Rapid acceleration detected").alias("description"),
                col("min_speed").alias("speed_before"),
                col("max_speed").alias("speed_after"),
                col("max_throttle").alias("throttle_position")
            )
        
        # Pattern 3: Sharp cornering
        sharp_cornering = telemetry_df \
            .withWatermark("timestamp", config.watermark_duration) \
            .groupBy(
                window(col("timestamp"), "10 seconds", "5 seconds"),
                col("vehicle_id")
            ) \
            .agg(
                avg("acceleration_y").alias("avg_accel_y"),
                stddev("acceleration_y").alias("std_accel_y"),
                avg("speed_kmh").alias("avg_speed")
            ) \
            .where(
                (abs(col("avg_accel_y")) > 2.0) &      # High lateral acceleration
                (col("std_accel_y") > 1.0) &           # Variable acceleration
                (col("avg_speed") > 40)                # At higher speeds
            ) \
            .select(
                col("vehicle_id"),
                col("window.start").alias("pattern_start"),
                col("window.end").alias("pattern_end"),
                lit("sharp_cornering").alias("pattern_type"),
                lit("Sharp cornering detected").alias("description"),
                col("avg_accel_y").alias("lateral_acceleration"),
                col("avg_speed").alias("speed_during")
            )
        
        # Combine patterns
        result_df = harsh_braking \
            .union(rapid_acceleration) \
            .union(sharp_cornering)
        
        return result_df
    
    def build_join_query(self, config: StreamingQueryConfig,
                        kafka_bootstrap_servers: str,
                        static_data: DataFrame) -> DataFrame:
        """Build streaming-static join query."""
        # Create streaming DataFrame
        telemetry_df = self.create_streaming_df(
            kafka_bootstrap_servers,
            config.input_topics,
            TelemetrySchema.get_telemetry_schema()
        )
        
        # Add watermark for stream
        telemetry_df = telemetry_df \
            .withWatermark("timestamp", config.watermark_duration)
        
        # Join with static vehicle data
        # Assuming static_data has columns: vehicle_id, make, model, year, etc.
        result_df = telemetry_df.join(
            static_data,
            telemetry_df.vehicle_id == static_data.vehicle_id,
            "left_outer"
        )
        
        return result_df
    
    def build_stateful_processing_query(self, config: StreamingQueryConfig,
                                       kafka_bootstrap_servers: str) -> DataFrame:
        """Build stateful processing query with sessionization."""
        # Create streaming DataFrame
        telemetry_df = self.create_streaming_df(
            kafka_bootstrap_servers,
            config.input_topics,
            TelemetrySchema.get_telemetry_schema()
        )
        
        # Sessionization: Group events into sessions based on time gaps
        # Define session window based on 15 minutes of inactivity
        session_window = Window \
            .partitionBy("vehicle_id") \
            .orderBy("timestamp")
        
        # Calculate time difference between consecutive events
        telemetry_df = telemetry_df \
            .withColumn("prev_timestamp", 
                       lag("timestamp", 1).over(session_window)) \
            .withColumn("time_diff", 
                       unix_timestamp("timestamp") - unix_timestamp("prev_timestamp"))
        
        # Define session: new session if gap > 15 minutes (900 seconds)
        telemetry_df = telemetry_df \
            .withColumn("new_session", 
                       when(col("time_diff").isNull() | (col("time_diff") > 900), 1)
                       .otherwise(0)) \
            .withColumn("session_id", 
                       sum("new_session").over(session_window))
        
        # Aggregate by session
        session_agg = telemetry_df \
            .withWatermark("timestamp", config.watermark_duration) \
            .groupBy(
                col("vehicle_id"),
                col("session_id"),
                window(col("timestamp"), "1 hour")
            ) \
            .agg(
                count("*").alias("event_count"),
                min("timestamp").alias("session_start"),
                max("timestamp").alias("session_end"),
                avg("speed_kmh").alias("avg_speed"),
                max("speed_kmh").alias("max_speed"),
                sum(when(col("speed_kmh") > 120, 1).otherwise(0)).alias("speeding_count"),
                avg("engine_temperature_c").alias("avg_engine_temp"),
                max("engine_temperature_c").alias("max_engine_temp"),
                sum(when(col("engine_temperature_c") > 100, 1).otherwise(0)).alias("overheating_count")
            ) \
            .select(
                col("vehicle_id"),
                col("session_id"),
                col("session_start"),
                col("session_end"),
                col("event_count"),
                col("avg_speed"),
                col("max_speed"),
                col("speeding_count"),
                col("avg_engine_temp"),
                col("max_engine_temp"),
                col("overheating_count"),
                ((unix_timestamp(col("session_end")) - 
                  unix_timestamp(col("session_start"))) / 3600).alias("session_duration_hours")
            )
        
        return session_agg


class StreamingQueryManager:
    """Manager for Spark streaming queries."""
    
    def __init__(self, spark: SparkSession, kafka_bootstrap_servers: str):
        self.spark = spark
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.query_builder = StreamingQueryBuilder(spark)
        self.active_queries = {}
        
        # Set Spark configuration for streaming
        self._configure_spark()
    
    def _configure_spark(self):
        """Configure Spark for streaming."""
        self.spark.conf.set("spark.sql.streaming.checkpointLocation", 
                           "/tmp/spark-checkpoints")
        self.spark.conf.set("spark.sql.streaming.schemaInference", "true")
        self.spark.conf.set("spark.sql.shuffle.partitions", "200")
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
    
    def create_query(self, config: StreamingQueryConfig) -> Optional[DataFrame]:
        """Create a streaming query based on configuration."""
        try:
            if config.query_type == StreamingQueryType.REAL_TIME_AGGREGATION:
                result_df = self.query_builder.build_real_time_aggregation_query(
                    config, self.kafka_bootstrap_servers
                )
            
            elif config.query_type == StreamingQueryType.ANOMALY_DETECTION:
                result_df = self.query_builder.build_anomaly_detection_query(
                    config, self.kafka_bootstrap_servers
                )
            
            elif config.query_type == StreamingQueryType.PATTERN_MATCHING:
                result_df = self.query_builder.build_pattern_matching_query(
                    config, self.kafka_bootstrap_servers
                )
            
            elif config.query_type == StreamingQueryType.JOIN_OPERATIONS:
                # For join operations, we need static data
                static_df = self._load_static_data()
                result_df = self.query_builder.build_join_query(
                    config, self.kafka_bootstrap_servers, static_df
                )
            
            elif config.query_type == StreamingQueryType.STATEFUL_PROCESSING:
                result_df = self.query_builder.build_stateful_processing_query(
                    config, self.kafka_bootstrap_servers
                )
            
            else:
                logger.error(f"Unknown query type: {config.query_type}")
                return None
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error creating query: {e}")
            return None
    
    def _load_static_data(self) -> DataFrame:
        """Load static data for join operations."""
        # Example: Load vehicle information from database or file
        # This is a placeholder - in production, you'd load real data
        data = [
            ("VH001", "Toyota", "Camry", 2020, "Sedan", "Petrol"),
            ("VH002", "Honda", "Civic", 2021, "Sedan", "Petrol"),
            ("VH003", "Ford", "F-150", 2019, "Truck", "Diesel"),
            ("VH004", "Tesla", "Model 3", 2022, "Sedan", "Electric"),
            ("VH005", "BMW", "X5", 2020, "SUV", "Petrol")
        ]
        
        schema = StructType([
            StructField("vehicle_id", StringType(), True),
            StructField("make", StringType(), True),
            StructField("model", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("type", StringType(), True),
            StructField("fuel_type", StringType(), True)
        ])
        
        return self.spark.createDataFrame(data, schema)
    
    def start_query(self, config: StreamingQueryConfig, 
                   output_format: str = "console") -> Optional[Any]:
        """Start a streaming query."""
        result_df = self.create_query(config)
        
        if not result_df:
            return None
        
        try:
            # Configure write stream
            if output_format == "console":
                query = result_df \
                    .writeStream \
                    .outputMode(config.output_mode) \
                    .format("console") \
                    .option("truncate", "false") \
                    .option("numRows", 20) \
                    .trigger(processingTime=config.processing_time) \
                    .start()
            
            elif output_format == "kafka":
                query = result_df \
                    .select(to_json(struct("*")).alias("value")) \
                    .writeStream \
                    .outputMode(config.output_mode) \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                    .option("topic", config.output_topic) \
                    .option("checkpointLocation", 
                           f"{config.checkpoint_location}/{config.query_name}") \
                    .trigger(processingTime=config.processing_time) \
                    .start()
            
            elif output_format == "memory":
                query = result_df \
                    .writeStream \
                    .outputMode(config.output_mode) \
                    .format("memory") \
                    .queryName(config.query_name) \
                    .trigger(processingTime=config.processing_time) \
                    .start()
            
            else:
                logger.error(f"Unknown output format: {output_format}")
                return None
            
            # Store query reference
            self.active_queries[config.query_name] = {
                'query': query,
                'config': config,
                'start_time': datetime.now()
            }
            
            logger.info(f"Started query: {config.query_name}")
            return query
            
        except Exception as e:
            logger.error(f"Error starting query: {e}")
            return None
    
    def stop_query(self, query_name: str):
        """Stop a streaming query."""
        if query_name in self.active_queries:
            try:
                query_info = self.active_queries[query_name]
                query_info['query'].stop()
                query_info['query'].awaitTermination()
                
                logger.info(f"Stopped query: {query_name}")
                del self.active_queries[query_name]
                
            except Exception as e:
                logger.error(f"Error stopping query: {e}")
        else:
            logger.warning(f"Query not found: {query_name}")
    
    def stop_all_queries(self):
        """Stop all active streaming queries."""
        for query_name in list(self.active_queries.keys()):
            self.stop_query(query_name)
    
    def get_query_status(self, query_name: str) -> Dict[str, Any]:
        """Get status of a streaming query."""
        if query_name not in self.active_queries:
            return {"status": "not_found"}
        
        query_info = self.active_queries[query_name]
        query = query_info['query']
        
        status = {
            "query_name": query_name,
            "status": query.status["message"],
            "is_active": query.isActive,
            "last_progress": query.lastProgress,
            "recent_progress": query.recentProgress,
            "start_time": query_info['start_time'].isoformat(),
            "config": query_info['config'].to_dict()
        }
        
        return status
    
    def get_all_query_status(self) -> Dict[str, Any]:
        """Get status of all active queries."""
        status = {}
        for query_name in self.active_queries:
            status[query_name] = self.get_query_status(query_name)
        return status


class StreamingMetricsCollector:
    """Collect metrics from streaming queries."""
    
    def __init__(self, query_manager: StreamingQueryManager):
        self.query_manager = query_manager
        self.metrics_history = {}
    
    def collect_metrics(self):
        """Collect metrics from all active queries."""
        metrics = {}
        
        for query_name, query_info in self.query_manager.active_queries.items():
            query = query_info['query']
            
            if query.lastProgress:
                progress = query.lastProgress
                
                query_metrics = {
                    'input_rows_per_second': progress.get('inputRowsPerSecond', 0),
                    'processed_rows_per_second': progress.get('processedRowsPerSecond', 0),
                    'num_input_rows': progress.get('numInputRows', 0),
                    'batch_id': progress.get('batchId', 0),
                    'timestamp': progress.get('timestamp', ''),
                    'sink': progress.get('sink', {}).get('description', '')
                }
                
                metrics[query_name] = query_metrics
        
        # Store in history
        timestamp = datetime.now()
        self.metrics_history[timestamp] = metrics
        
        # Keep only recent history
        if len(self.metrics_history) > 100:
            oldest = min(self.metrics_history.keys())
            del self.metrics_history[oldest]
        
        return metrics
    
    def get_performance_report(self) -> Dict[str, Any]:
        """Generate performance report."""
        if not self.metrics_history:
            return {}
        
        # Calculate averages
        all_metrics = list(self.metrics_history.values())
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'active_queries': len(self.query_manager.active_queries),
            'query_performance': {}
        }
        
        for query_name in self.query_manager.active_queries.keys():
            query_metrics = [m.get(query_name, {}) for m in all_metrics 
                            if query_name in m]
            
            if query_metrics:
                avg_input_rate = np.mean([m.get('input_rows_per_second', 0) 
                                         for m in query_metrics])
                avg_processed_rate = np.mean([m.get('processed_rows_per_second', 0) 
                                             for m in query_metrics])
                total_input = sum([m.get('num_input_rows', 0) 
                                  for m in query_metrics])
                
                report['query_performance'][query_name] = {
                    'avg_input_rows_per_second': avg_input_rate,
                    'avg_processed_rows_per_second': avg_processed_rate,
                    'total_input_rows': total_input,
                    'sample_count': len(query_metrics)
                }
        
        return report


def create_spark_session(app_name: str = "VehicleTelemetryAnalytics") -> SparkSession:
    """Create and configure Spark session."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.cores", "1") \
        .config("spark.default.parallelism", "200") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Spark Streaming Queries")
    parser.add_argument("--kafka-servers", default="localhost:9092",
                       help="Kafka bootstrap servers")
    parser.add_argument("--input-topic", default="vehicle-telemetry",
                       help="Input Kafka topic")
    parser.add_argument("--output-topic", default="telemetry-aggregates",
                       help="Output Kafka topic")
    parser.add_argument("--query-type", default="real_time_aggregation",
                       choices=["real_time_aggregation", "anomaly_detection", 
                                "pattern_matching", "stateful_processing"],
                       help="Type of streaming query")
    parser.add_argument("--output-format", default="console",
                       choices=["console", "kafka", "memory"],
                       help="Output format")
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session("StreamingQueries")
    
    # Create query manager
    query_manager = StreamingQueryManager(spark, args.kafka_servers)
    
    # Create query configuration
    config = StreamingQueryConfig(
        query_type=StreamingQueryType(args.query_type),
        input_topics=[args.input_topic],
        output_topic=args.output_topic,
        window_duration="5 minutes",
        sliding_duration="1 minute",
        watermark_duration="10 minutes",
        checkpoint_location="/tmp/spark-checkpoints",
        processing_time="10 seconds",
        output_mode="update",
        query_name=f"{args.query_type}_query"
    )
    
    # Start query
    query = query_manager.start_query(config, args.output_format)
    
    if not query:
        logger.error("Failed to start query")
        return
    
    # Wait for termination
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
        query_manager.stop_all_queries()
    except Exception as e:
        logger.error(f"Query terminated with error: {e}")


if __name__ == "__main__":
    main()