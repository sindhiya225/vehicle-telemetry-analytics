from spark_session import SparkStreamingSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import yaml
import time
from datetime import datetime, timedelta

class VehicleTelemetryStreamingProcessor:
    def __init__(self):
        """Initialize streaming processor"""
        self.spark_session = SparkStreamingSession()
        self.spark = self.spark_session.spark
        self.config = self.spark_session.config
        
        # Load vehicle master data
        self.vehicle_data = self.load_vehicle_data()
        
        # Register UDFs
        self.register_udfs()
    
    def load_vehicle_data(self):
        """Load static vehicle data as broadcast variable"""
        try:
            vehicle_df = self.spark.read \
                .option("header", True) \
                .csv("data/processed/cleaned_vehicle_data.csv")
            
            # Cache for efficient joins
            vehicle_df.cache()
            print(f"Loaded {vehicle_df.count()} vehicles")
            
            return vehicle_df
        except Exception as e:
            print(f"Error loading vehicle data: {e}")
            return None
    
    def register_udfs(self):
        """Register User Defined Functions"""
        
        # Fault severity classifier
        @udf(StringType())
        def classify_fault_severity(fault_codes):
            if not fault_codes:
                return "NONE"
            
            critical_faults = ['P0300', 'P0301', 'P0302', 'P0303', 'P0304']
            warning_faults = ['P0420', 'P0171', 'P0172', 'P0442']
            
            for code in fault_codes:
                if code in critical_faults:
                    return "CRITICAL"
                elif code in warning_faults:
                    return "WARNING"
            
            return "INFO"
        
        self.spark.udf.register("classify_fault_severity", classify_fault_severity)
        
        # Calculate maintenance urgency
        @udf(StringType())
        def calculate_maintenance_urgency(fault_count, efficiency_degradation, idle_time):
            score = 0
            
            if fault_count > 0:
                score += fault_count * 10
            
            if efficiency_degradation > 10:
                score += (efficiency_degradation - 10) * 2
            
            if idle_time > 10:
                score += (idle_time - 10) * 0.5
            
            if score > 30:
                return "IMMEDIATE"
            elif score > 15:
                return "HIGH"
            elif score > 5:
                return "MEDIUM"
            else:
                return "LOW"
        
        self.spark.udf.register("calculate_maintenance_urgency", calculate_maintenance_urgency)
        
        # Calculate CO2 emissions
        @udf(DoubleType())
        def calculate_co2_emissions(fuel_consumed_l, vehicle_type):
            # CO2 emission factors (kg CO2 per liter)
            factors = {
                'Gasoline': 2.31,
                'Diesel': 2.68,
                'Hybrid': 1.85,
                'HEV': 1.85,
                'ICE': 2.31
            }
            
            factor = factors.get(vehicle_type, 2.31)
            return fuel_consumed_l * factor
        
        self.spark.udf.register("calculate_co2_emissions", calculate_co2_emissions)
    
    def process_telemetry_stream(self):
        """Main streaming processing pipeline"""
        
        print("Starting telemetry streaming processor...")
        
        # 1. Read raw telemetry from Kafka
        kafka_stream = self.spark_session.read_from_kafka(
            self.config['kafka']['topics']['raw_telemetry'],
            starting_offsets="latest"
        )
        
        # 2. Parse Avro data
        telemetry_df = self.spark_session.parse_avro_stream(
            kafka_stream,
            self.spark_session.telemetry_schema
        )
        
        # Add processing timestamp
        telemetry_df = telemetry_df.withColumn(
            "processing_timestamp", current_timestamp()
        )
        
        # 3. Join with vehicle master data
        if self.vehicle_data is not None:
            telemetry_df = telemetry_df.join(
                broadcast(self.vehicle_data),
                "vehid",
                "left"
            )
        
        # 4. Enrich with derived metrics
        enriched_df = telemetry_df \
            .withColumn("fault_severity", 
                       expr("classify_fault_severity(fault_codes)")) \
            .withColumn("co2_emissions_kg", 
                       expr("calculate_co2_emissions(fuel_consumed_l, vehicle_type)")) \
            .withColumn("cost_per_km", 
                       col("fuel_consumed_l") * 1.2 / col("trip_distance_km")) \
            .withColumn("trip_efficiency_score", 
                       expr("fuel_efficiency_kmpl / 20 * 100"))  # Normalized to 100
        
        # 5. Create streaming aggregations
        aggregations = self.spark_session.create_streaming_aggregations(enriched_df)
        
        # 6. Detect anomalies
        anomalies = self.spark_session.detect_anomalies(enriched_df)
        
        # 7. Calculate cost savings
        cost_savings = self.spark_session.calculate_cost_savings(enriched_df)
        
        # 8. Generate alerts
        alerts = self.generate_alerts(enriched_df)
        
        # 9. Start streaming queries
        queries = []
        
        # Write processed telemetry to Kafka
        queries.append(
            self.spark_session.write_to_kafka(
                enriched_df.select(
                    to_json(struct("*")).alias("value")
                ),
                self.config['kafka']['topics']['processed_telemetry'],
                checkpoint_location="/tmp/checkpoints/processed_telemetry"
            )
        )
        
        # Write anomalies to Kafka
        queries.append(
            self.spark_session.write_to_kafka(
                anomalies.select(
                    to_json(struct("*")).alias("value")
                ),
                self.config['kafka']['topics']['anomalies'],
                checkpoint_location="/tmp/checkpoints/anomalies"
            )
        )
        
        # Write alerts to Kafka
        queries.append(
            self.spark_session.write_to_kafka(
                alerts.select(
                    to_json(struct("*")).alias("value")
                ),
                self.config['kafka']['topics']['maintenance_alerts'],
                checkpoint_location="/tmp/checkpoints/alerts"
            )
        )
        
        # Write vehicle aggregations to Delta Lake
        queries.append(
            self.spark_session.write_to_delta(
                aggregations["vehicle_aggregations"],
                "/delta/vehicle_aggregations",
                checkpoint_location="/tmp/checkpoints/vehicle_agg"
            )
        )
        
        # Write fleet aggregations to Delta Lake
        queries.append(
            self.spark_session.write_to_delta(
                aggregations["fleet_aggregations"],
                "/delta/fleet_aggregations",
                checkpoint_location="/tmp/checkpoints/fleet_agg"
            )
        )
        
        # Write efficiency metrics to console (for debugging)
        queries.append(
            self.spark_session.write_to_console(
                aggregations["efficiency_monitoring"].select(
                    "window", "vehicle_type", "median_efficiency", "efficiency_range"
                ),
                truncate=False
            )
        )
        
        # Write cost savings to console
        queries.append(
            self.spark_session.write_to_console(
                cost_savings.select(
                    "window", "vehid", "fuel_cost", "potential_savings"
                ),
                truncate=False
            )
        )
        
        print("All streaming queries started successfully!")
        
        # Wait for termination
        self.spark_session.await_termination(queries)
    
    def generate_alerts(self, telemetry_df):
        """Generate real-time maintenance and safety alerts"""
        
        # Window for trend analysis
        window_spec = Window.partitionBy("vehid").orderBy("timestamp").rowsBetween(-10, 0)
        
        alerts = telemetry_df \
            .withColumn("efficiency_trend", 
                       avg("fuel_efficiency_kmpl").over(window_spec)) \
            .withColumn("temp_trend", 
                       avg("avg_engine_temp_c").over(window_spec)) \
            .withColumn("fault_count_trend", 
                       sum(when(col("has_fault"), 1).otherwise(0)).over(window_spec)) \
            .withColumn("efficiency_degradation", 
                       (first("fuel_efficiency_kmpl").over(window_spec) - 
                        last("fuel_efficiency_kmpl").over(window_spec)) / 
                       first("fuel_efficiency_kmpl").over(window_spec) * 100) \
            .filter(
                (col("has_fault") == True) |
                (col("maintenance_flag") == True) |
                (col("efficiency_degradation") > 15) |
                (col("avg_engine_temp_c") > 110) |
                (col("fault_count_trend") >= 3)
            ) \
            .withColumn("alert_id", 
                       expr("uuid()")) \
            .withColumn("alert_timestamp", 
                       current_timestamp()) \
            .withColumn("alert_type",
                       expr("""
                           CASE 
                               WHEN has_fault THEN 'FAULT_DETECTED'
                               WHEN maintenance_flag THEN 'MAINTENANCE_NEEDED'
                               WHEN efficiency_degradation > 15 THEN 'EFFICIENCY_DEGRADATION'
                               WHEN avg_engine_temp_c > 110 THEN 'OVERHEATING'
                               WHEN fault_count_trend >= 3 THEN 'RECURRING_FAULTS'
                               ELSE 'OTHER'
                           END
                       """)) \
            .withColumn("priority",
                       expr("""
                           CASE 
                               WHEN avg_engine_temp_c > 115 THEN 'CRITICAL'
                               WHEN has_fault AND fault_severity = 'CRITICAL' THEN 'CRITICAL'
                               WHEN efficiency_degradation > 20 THEN 'HIGH'
                               WHEN has_fault THEN 'MEDIUM'
                               ELSE 'LOW'
                           END
                       """)) \
            .withColumn("description",
                       expr("""
                           CASE 
                               WHEN has_fault THEN CONCAT('Fault detected: ', array_join(fault_codes, ', '))
                               WHEN maintenance_flag THEN 'Maintenance required based on usage patterns'
                               WHEN efficiency_degradation > 15 THEN CONCAT('Fuel efficiency degraded by ', round(efficiency_degradation, 1), '%')
                               WHEN avg_engine_temp_c > 110 THEN CONCAT('Engine overheating: ', round(avg_engine_temp_c, 1), '°C')
                               WHEN fault_count_trend >= 3 THEN 'Multiple faults detected in recent trips'
                               ELSE 'Alert triggered'
                           END
                       """)) \
            .withColumn("recommended_action",
                       expr("""
                           CASE 
                               WHEN has_fault THEN 'Schedule diagnostic check immediately'
                               WHEN maintenance_flag THEN 'Schedule preventive maintenance within 7 days'
                               WHEN efficiency_degradation > 15 THEN 'Check air filter, tire pressure, and fuel system'
                               WHEN avg_engine_temp_c > 110 THEN 'Check coolant level and radiator immediately'
                               WHEN fault_count_trend >= 3 THEN 'Comprehensive diagnostic check needed'
                               ELSE 'Monitor vehicle performance'
                           END
                       """)) \
            .select(
                "alert_id",
                "alert_timestamp",
                "vehid",
                "vehicle_type",
                "vehicle_class",
                "alert_type",
                "priority",
                "description",
                "recommended_action",
                "trip_id",
                "trip_distance_km",
                "fuel_efficiency_kmpl",
                "avg_engine_temp_c",
                "fault_codes",
                "efficiency_degradation"
            )
        
        return alerts
    
    def run_batch_analysis(self, start_time, end_time):
        """Run batch analysis on historical streaming data"""
        
        # Read from Delta Lake
        vehicle_agg_df = self.spark.read.format("delta").load("/delta/vehicle_aggregations")
        fleet_agg_df = self.spark.read.format("delta").load("/delta/fleet_aggregations")
        
        # Filter by time range
        vehicle_agg_df = vehicle_agg_df.filter(
            (col("window.start") >= start_time) &
            (col("window.end") <= end_time)
        )
        
        fleet_agg_df = fleet_agg_df.filter(
            (col("window.start") >= start_time) &
            (col("window.end") <= end_time)
        )
        
        # Run comprehensive analysis
        analysis_results = {
            "vehicle_performance": self.analyze_vehicle_performance(vehicle_agg_df),
            "fleet_efficiency": self.analyze_fleet_efficiency(fleet_agg_df),
            "cost_analysis": self.analyze_costs(vehicle_agg_df),
            "maintenance_patterns": self.analyze_maintenance_patterns(vehicle_agg_df)
        }
        
        return analysis_results
    
    def analyze_vehicle_performance(self, vehicle_agg_df):
        """Analyze individual vehicle performance"""
        
        analysis = vehicle_agg_df.groupBy("vehid", "vehicle_type", "vehicle_class").agg(
            sum("total_distance_km").alias("total_distance"),
            sum("total_fuel_consumed_l").alias("total_fuel"),
            avg("avg_fuel_efficiency").alias("avg_efficiency"),
            sum("fault_count").alias("total_faults"),
            sum("maintenance_count").alias("total_maintenance_alerts"),
            avg("avg_driver_score").alias("avg_driver_score"),
            expr("percentile_approx(avg_fuel_efficiency, 0.5)").alias("median_efficiency")
        ).withColumn(
            "cost_per_km",
            col("total_fuel") * 1.2 / col("total_distance")
        ).withColumn(
            "reliability_score",
            expr("100 - (total_faults * 10)")
        ).orderBy(col("total_distance").desc())
        
        return analysis
    
    def analyze_fleet_efficiency(self, fleet_agg_df):
        """Analyze fleet-wide efficiency trends"""
        
        analysis = fleet_agg_df.groupBy("vehicle_type").agg(
            avg("active_vehicles").alias("avg_active_vehicles"),
            sum("total_distance").alias("total_distance"),
            sum("total_fuel").alias("total_fuel"),
            avg("avg_efficiency").alias("avg_efficiency"),
            sum("total_faults").alias("total_faults"),
            expr("percentile_approx(avg_efficiency, 0.25)").alias("p25_efficiency"),
            expr("percentile_approx(avg_efficiency, 0.75)").alias("p75_efficiency")
        ).withColumn(
            "fleet_efficiency_score",
            expr("(avg_efficiency / 20) * 100")  # Normalize to 100
        ).withColumn(
            "fault_rate",
            col("total_faults") / col("total_distance") * 1000  # Faults per 1000 km
        ).orderBy(col("fleet_efficiency_score").desc())
        
        return analysis
    
    def analyze_costs(self, vehicle_agg_df):
        """Analyze operational costs"""
        
        # Current prices
        fuel_price_per_liter = 1.2  # USD
        maintenance_cost_per_alert = 150  # USD
        idle_cost_per_hour = 2.0  # USD
        
        analysis = vehicle_agg_df.groupBy("vehicle_type", "vehicle_class").agg(
            sum("total_distance_km").alias("total_distance"),
            sum("total_fuel_consumed_l").alias("total_fuel"),
            sum("total_idle_min").alias("total_idle_min"),
            sum("maintenance_count").alias("total_maintenance_alerts"),
            sum("fault_count").alias("total_faults")
        ).withColumn(
            "fuel_cost",
            col("total_fuel") * fuel_price_per_liter
        ).withColumn(
            "maintenance_cost",
            col("total_maintenance_alerts") * maintenance_cost_per_alert
        ).withColumn(
            "idle_cost",
            col("total_idle_min") / 60 * idle_cost_per_hour
        ).withColumn(
            "total_operating_cost",
            col("fuel_cost") + col("maintenance_cost") + col("idle_cost")
        ).withColumn(
            "cost_per_km",
            col("total_operating_cost") / col("total_distance")
        ).withColumn(
            "potential_savings",
            expr("""
                (fuel_cost * 0.15) +  -- 15% fuel efficiency improvement
                (maintenance_cost * 0.3) +  -- 30% reduction through preventive maintenance
                (idle_cost * 0.5)  -- 50% idle time reduction
            """)
        ).orderBy(col("cost_per_km").desc())
        
        return analysis
    
    def analyze_maintenance_patterns(self, vehicle_agg_df):
        """Analyze maintenance patterns and predict future needs"""
        
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.regression import LinearRegression
        from pyspark.ml import Pipeline
        
        # Prepare features for maintenance prediction
        features_df = vehicle_agg_df.select(
            "vehid",
            "total_distance_km",
            "total_fuel_consumed_l",
            "avg_fuel_efficiency",
            "total_idle_min",
            "fault_count",
            "maintenance_count",
            "avg_driver_score"
        )
        
        # Assemble features
        feature_cols = [
            "total_distance_km",
            "total_fuel_consumed_l",
            "avg_fuel_efficiency",
            "total_idle_min",
            "fault_count",
            "avg_driver_score"
        ]
        
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        # Split data
        train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)
        
        # Create and train model
        lr = LinearRegression(
            featuresCol="features",
            labelCol="maintenance_count",
            maxIter=10,
            regParam=0.3,
            elasticNetParam=0.8
        )
        
        pipeline = Pipeline(stages=[assembler, lr])
        model = pipeline.fit(train_df)
        
        # Make predictions
        predictions = model.transform(test_df)
        
        # Calculate maintenance prediction metrics
        predictions = predictions.withColumn(
            "next_maintenance_km",
            expr("""
                CASE 
                    WHEN prediction > 0.5 THEN total_distance_km * 1.2
                    ELSE total_distance_km * 1.5
                END
            """)
        )
        
        return predictions.select(
            "vehid",
            "total_distance_km",
            "maintenance_count",
            col("prediction").alias("predicted_maintenance_need"),
            "next_maintenance_km"
        )
    
    def shutdown(self):
        """Graceful shutdown"""
        self.spark_session.stop()
        print("Streaming processor shut down successfully.")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Vehicle Telemetry Streaming Processor')
    parser.add_argument('--mode', choices=['stream', 'batch'], default='stream',
                       help='Processing mode: stream (real-time) or batch (historical)')
    parser.add_argument('--start-time', type=str,
                       help='Start time for batch analysis (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--end-time', type=str,
                       help='End time for batch analysis (YYYY-MM-DD HH:MM:SS)')
    
    args = parser.parse_args()
    
    processor = VehicleTelemetryStreamingProcessor()
    
    try:
        if args.mode == 'stream':
            processor.process_telemetry_stream()
        else:
            if not args.start_time or not args.end_time:
                print("Error: Batch mode requires --start-time and --end-time")
                exit(1)
            
            results = processor.run_batch_analysis(args.start_time, args.end_time)
            
            # Print results
            for analysis_name, df in results.items():
                print(f"\n{'='*60}")
                print(f"Analysis: {analysis_name}")
                print('='*60)
                df.show(10, truncate=False)
                
    except KeyboardInterrupt:
        print("\nStopping processor...")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        processor.shutdown()