from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import yaml
import json
from datetime import datetime

class StructuredStreamingProcessor:
    """Advanced Structured Streaming with stateful operations"""
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("VehicleTelemetryStructuredStreaming") \
            .master("local[*]") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
            .config("spark.sql.streaming.checkpointLocation", 
                   "/tmp/structured-streaming-checkpoints") \
            .config("spark.sql.streaming.stateStore.providerClass",
                   "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
            .config("spark.sql.streaming.stateStore.minDeltasForSnapshot", 10) \
            .config("spark.sql.shuffle.partitions", 4) \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        with open('streaming/kafka/kafka_config.yaml', 'r') as f:
            self.config = yaml.safe_load(f)
    
    def create_streaming_queries(self):
        """Create multiple streaming queries with different processing patterns"""
        
        # 1. Read from Kafka
        kafka_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config['kafka']['bootstrap_servers']) \
            .option("subscribe", self.config['kafka']['topics']['raw_telemetry']) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON
        schema = StructType([
            StructField("vehid", IntegerType()),
            StructField("vehicle_type", StringType()),
            StructField("trip_distance_km", DoubleType()),
            StructField("fuel_consumed_l", DoubleType()),
            StructField("fuel_efficiency_kmpl", DoubleType()),
            StructField("avg_speed_kmph", DoubleType()),
            StructField("has_fault", BooleanType()),
            StructField("timestamp", LongType())
        ])
        
        parsed_stream = kafka_stream.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        # Add processing timestamp
        parsed_stream = parsed_stream.withColumn(
            "processing_timestamp", current_timestamp()
        )
        
        # 2. Create different streaming aggregations
        
        # Query 1: Tumbling window aggregations (5 minutes)
        tumbling_window_agg = parsed_stream \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(from_unixtime(col("timestamp") / 1000), "5 minutes"),
                "vehicle_type"
            ).agg(
                count("*").alias("event_count"),
                sum("trip_distance_km").alias("total_distance"),
                avg("fuel_efficiency_kmpl").alias("avg_efficiency"),
                sum(when(col("has_fault"), 1).otherwise(0)).alias("fault_count")
            )
        
        # Query 2: Sliding window aggregations (10 minutes sliding every 2 minutes)
        sliding_window_agg = parsed_stream \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(from_unixtime(col("timestamp") / 1000), "10 minutes", "2 minutes"),
                "vehicle_type"
            ).agg(
                approx_count_distinct("vehid").alias("active_vehicles"),
                avg("avg_speed_kmph").alias("avg_speed"),
                expr("percentile_approx(fuel_efficiency_kmpl, 0.5)").alias("median_efficiency")
            )
        
        # Query 3: Session window aggregations (5 minute gap)
        session_window_agg = parsed_stream \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                session_window(from_unixtime(col("timestamp") / 1000), "5 minutes"),
                "vehid",
                "vehicle_type"
            ).agg(
                count("*").alias("trip_count"),
                sum("trip_distance_km").alias("session_distance"),
                sum("fuel_consumed_l").alias("session_fuel"),
                avg("fuel_efficiency_kmpl").alias("session_efficiency")
            )
        
        # Query 4: Stateful aggregation with mapGroupsWithState
        def update_vehicle_state(key, new_events, state):
            """Custom state update function"""
            from pyspark.sql.streaming import GroupState
            
            if state.hasTimedOut:
                # State timed out, reset
                state.remove()
                yield (key[0], key[1], 0, 0.0, 0, datetime.now().isoformat())
            else:
                # Update state
                event_count = state.getOption.map(lambda s: s[0]).getOrElse(0) + len(new_events)
                total_distance = state.getOption.map(lambda s: s[1]).getOrElse(0.0) + \
                               sum([e.trip_distance_km for e in new_events])
                fault_count = state.getOption.map(lambda s: s[2]).getOrElse(0) + \
                            sum([1 for e in new_events if e.has_fault])
                
                state.update((event_count, total_distance, fault_count, datetime.now().isoformat()))
                state.setTimeoutDuration("10 minutes")
                
                yield (key[0], key[1], event_count, total_distance, fault_count, 
                      datetime.now().isoformat())
        
        vehicle_state_schema = StructType([
            StructField("event_count", LongType()),
            StructField("total_distance", DoubleType()),
            StructField("fault_count", IntegerType()),
            StructField("last_update", StringType())
        ])
        
        output_schema = StructType([
            StructField("vehid", IntegerType()),
            StructField("vehicle_type", StringType()),
            StructField("event_count", LongType()),
            StructField("total_distance", DoubleType()),
            StructField("fault_count", IntegerType()),
            StructField("last_update", StringType())
        ])
        
        stateful_agg = parsed_stream \
            .groupByKey(lambda x: (x.vehid, x.vehicle_type)) \
            .mapGroupsWithState(
                GroupStateTimeout.ProcessingTimeTimeout(),
                vehicle_state_schema,
                output_schema,
                update_vehicle_state
            )
        
        # Query 5: Continuous processing with trigger
        continuous_agg = parsed_stream \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(from_unixtime(col("timestamp") / 1000), "1 minute"),
                "vehicle_type"
            ).agg(
                count("*").alias("events_per_minute"),
                avg("fuel_efficiency_kmpl").alias("minute_avg_efficiency")
            )
        
        # 3. Start all streaming queries
        queries = []
        
        # Tumbling window to console
        queries.append(
            tumbling_window_agg.writeStream \
                .outputMode("update") \
                .format("console") \
                .option("truncate", False) \
                .trigger(processingTime="30 seconds") \
                .start()
        )
        
        # Sliding window to Kafka
        queries.append(
            sliding_window_agg.select(
                to_json(struct("*")).alias("value")
            ).writeStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.config['kafka']['bootstrap_servers']) \
                .option("topic", self.config['kafka']['topics']['efficiency_metrics']) \
                .outputMode("update") \
                .option("checkpointLocation", "/tmp/checkpoints/sliding_window") \
                .trigger(processingTime="1 minute") \
                .start()
        )
        
        # Session window to memory
        queries.append(
            session_window_agg.writeStream \
                .outputMode("update") \
                .format("memory") \
                .queryName("session_aggregations") \
                .trigger(processingTime="1 minute") \
                .start()
        )
        
        # Stateful aggregation to Delta Lake
        queries.append(
            stateful_agg.writeStream \
                .outputMode("update") \
                .format("delta") \
                .option("path", "/delta/vehicle_state") \
                .option("checkpointLocation", "/tmp/checkpoints/vehicle_state") \
                .trigger(processingTime="2 minutes") \
                .start()
        )
        
        # Continuous processing to console
        queries.append(
            continuous_agg.writeStream \
                .outputMode("update") \
                .format("console") \
                .option("truncate", False) \
                .trigger(continuous="1 second") \
                .start()
        )
        
        print("All streaming queries started!")
        print("Session aggregations available as table 'session_aggregations'")
        
        # Wait for termination
        self.spark.streams.awaitAnyTermination()
    
    def run_watermark_demo(self):
        """Demonstrate watermark and late data handling"""
        
        # Create stream with watermark
        stream = self.spark.readStream \
            .format("rate") \
            .option("rowsPerSecond", 10) \
            .load() \
            .selectExpr(
                "value as vehid",
                "timestamp as event_time",
                "(rand() * 100) as fuel_efficiency",
                "cast(rand() > 0.9 as boolean) as has_fault"
            )
        
        # Apply watermark of 10 seconds
        watermarked_stream = stream.withWatermark("event_time", "10 seconds")
        
        # Window aggregation
        windowed_agg = watermarked_stream.groupBy(
            window(col("event_time"), "5 seconds"),
            col("vehid")
        ).agg(
            avg("fuel_efficiency").alias("avg_efficiency"),
            sum(when(col("has_fault"), 1).otherwise(0)).alias("fault_count")
        )
        
        # Start query
        query = windowed_agg.writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime="5 seconds") \
            .start()
        
        query.awaitTermination()
    
    def run_join_streams(self):
        """Demonstrate stream-stream joins"""
        
        # Create two simulated streams
        stream1 = self.spark.readStream \
            .format("rate") \
            .option("rowsPerSecond", 5) \
            .load() \
            .selectExpr(
                "value as vehid",
                "timestamp as telemetry_time",
                "(rand() * 20 + 10) as fuel_efficiency"
            )
        
        stream2 = self.spark.readStream \
            .format("rate") \
            .option("rowsPerSecond", 2) \
            .load() \
            .selectExpr(
                "value as vehid",
                "timestamp as fault_time",
                "cast(rand() > 0.5 as boolean) as is_critical"
            )
        
        # Apply watermarks
        stream1_watermarked = stream1.withWatermark("telemetry_time", "1 minute")
        stream2_watermarked = stream2.withWatermark("fault_time", "2 minutes")
        
        # Inner join with time range condition
        joined_stream = stream1_watermarked.join(
            stream2_watermarked,
            expr("""
                vehid = vehid AND
                fault_time >= telemetry_time AND
                fault_time <= telemetry_time + interval 5 minutes
            """),
            "inner"
        )
        
        # Start query
        query = joined_stream.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime="10 seconds") \
            .start()
        
        query.awaitTermination()
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Structured Streaming Processor')
    parser.add_argument('--demo', choices=['watermark', 'join', 'full'], default='full',
                       help='Demo to run')
    
    args = parser.parse_args()
    
    processor = StructuredStreamingProcessor()
    
    try:
        if args.demo == 'watermark':
            processor.run_watermark_demo()
        elif args.demo == 'join':
            processor.run_join_streams()
        else:
            processor.create_streaming_queries()
    except KeyboardInterrupt:
        print("Stopping processor...")
    finally:
        processor.stop()