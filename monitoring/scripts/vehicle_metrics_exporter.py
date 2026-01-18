#!/usr/bin/env python3
"""
Vehicle Telemetry Metrics Exporter for Prometheus
Exports vehicle metrics from Kafka/Spark to Prometheus format
"""
import time
import json
import asyncio
from prometheus_client import start_http_server, Gauge, Counter, Histogram
from kafka import KafkaConsumer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define Prometheus metrics
VEHICLE_SPEED = Gauge('vehicle_speed', 'Current vehicle speed in km/h', 
                     ['vehicle_id', 'fleet_id', 'model'])
ENGINE_TEMP = Gauge('engine_temperature', 'Engine temperature in Celsius',
                   ['vehicle_id', 'fleet_id'])
FUEL_LEVEL = Gauge('fuel_level', 'Fuel level percentage',
                  ['vehicle_id', 'fleet_id'])
FUEL_EFFICIENCY = Gauge('fuel_efficiency', 'Fuel efficiency in km/l',
                       ['vehicle_id', 'fleet_id'])
RPM = Gauge('rpm', 'Engine RPM', ['vehicle_id', 'fleet_id'])
OIL_PRESSURE = Gauge('oil_pressure', 'Oil pressure in kPa',
                    ['vehicle_id', 'fleet_id'])
BATTERY_VOLTAGE = Gauge('battery_voltage', 'Battery voltage',
                       ['vehicle_id', 'fleet_id'])
THROTTLE_POSITION = Gauge('throttle_position', 'Throttle position percentage',
                         ['vehicle_id', 'fleet_id'])
FAULT_COUNT = Counter('fault_count', 'Total fault count',
                     ['vehicle_id', 'fleet_id', 'fault_code'])
WARNING_COUNT = Counter('warning_count', 'Total warning count',
                       ['vehicle_id', 'fleet_id', 'warning_type'])
TOTAL_DISTANCE = Counter('total_distance', 'Total distance traveled in km',
                        ['vehicle_id', 'fleet_id'])
DAILY_DISTANCE = Gauge('daily_distance', 'Daily distance in km',
                      ['vehicle_id', 'fleet_id'])
IDLE_TIME = Gauge('idle_time', 'Idle time percentage',
                 ['vehicle_id', 'fleet_id'])
DRIVING_TIME = Gauge('driving_time', 'Driving time percentage',
                    ['vehicle_id', 'fleet_id'])

# Spark metrics
SPARK_JOBS_ACTIVE = Gauge('spark_jobs_active', 'Active Spark jobs',
                         ['application_id', 'job_type'])
SPARK_TASKS_RUNNING = Gauge('spark_tasks_running', 'Running Spark tasks',
                           ['application_id'])
SPARK_MEMORY_USED = Gauge('spark_memory_used', 'Spark memory used in MB',
                         ['application_id', 'memory_type'])
SPARK_PROCESSING_LATENCY = Histogram('spark_processing_latency', 
                                    'Spark processing latency in ms',
                                    ['application_id', 'stage'])

class VehicleMetricsExporter:
    def __init__(self, kafka_brokers='localhost:9092', topics=None):
        self.kafka_brokers = kafka_brokers
        self.topics = topics or ['vehicle-telemetry', 'spark-metrics']
        self.consumer = None
        
    async def start(self):
        """Start the metrics exporter"""
        logger.info("Starting Vehicle Metrics Exporter...")
        
        # Connect to Kafka
        self.consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=self.kafka_brokers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='prometheus-exporter',
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        # Process messages
        await self.process_messages()
    
    async def process_messages(self):
        """Process Kafka messages and update metrics"""
        for message in self.consumer:
            try:
                data = message.value
                topic = message.topic
                
                if topic == 'vehicle-telemetry':
                    await self.process_vehicle_telemetry(data)
                elif topic == 'spark-metrics':
                    await self.process_spark_metrics(data)
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
    
    async def process_vehicle_telemetry(self, data):
        """Process vehicle telemetry data"""
        vehicle_id = data.get('vehicle_id', 'unknown')
        fleet_id = data.get('fleet_id', 'unknown')
        
        # Update metrics
        if 'speed' in data:
            VEHICLE_SPEED.labels(
                vehicle_id=vehicle_id,
                fleet_id=fleet_id,
                model=data.get('model', 'unknown')
            ).set(data['speed'])
        
        if 'engine_temp' in data:
            ENGINE_TEMP.labels(
                vehicle_id=vehicle_id,
                fleet_id=fleet_id
            ).set(data['engine_temp'])
        
        if 'fuel_level' in data:
            FUEL_LEVEL.labels(
                vehicle_id=vehicle_id,
                fleet_id=fleet_id
            ).set(data['fuel_level'])
        
        if 'fuel_efficiency' in data:
            FUEL_EFFICIENCY.labels(
                vehicle_id=vehicle_id,
                fleet_id=fleet_id
            ).set(data['fuel_efficiency'])
        
        if 'rpm' in data:
            RPM.labels(
                vehicle_id=vehicle_id,
                fleet_id=fleet_id
            ).set(data['rpm'])
        
        if 'oil_pressure' in data:
            OIL_PRESSURE.labels(
                vehicle_id=vehicle_id,
                fleet_id=fleet_id
            ).set(data['oil_pressure'])
        
        if 'battery_voltage' in data:
            BATTERY_VOLTAGE.labels(
                vehicle_id=vehicle_id,
                fleet_id=fleet_id
            ).set(data['battery_voltage'])
        
        if 'throttle_position' in data:
            THROTTLE_POSITION.labels(
                vehicle_id=vehicle_id,
                fleet_id=fleet_id
            ).set(data['throttle_position'])
        
        if 'distance' in data:
            TOTAL_DISTANCE.labels(
                vehicle_id=vehicle_id,
                fleet_id=fleet_id
            ).inc(data['distance'])
        
        if 'daily_distance' in data:
            DAILY_DISTANCE.labels(
                vehicle_id=vehicle_id,
                fleet_id=fleet_id
            ).set(data['daily_distance'])
        
        if 'idle_time' in data:
            IDLE_TIME.labels(
                vehicle_id=vehicle_id,
                fleet_id=fleet_id
            ).set(data['idle_time'])
        
        if 'driving_time' in data:
            DRIVING_TIME.labels(
                vehicle_id=vehicle_id,
                fleet_id=fleet_id
            ).set(data['driving_time'])
        
        # Process faults and warnings
        if 'faults' in data:
            for fault in data['faults']:
                FAULT_COUNT.labels(
                    vehicle_id=vehicle_id,
                    fleet_id=fleet_id,
                    fault_code=fault.get('code', 'unknown')
                ).inc()
        
        if 'warnings' in data:
            for warning in data['warnings']:
                WARNING_COUNT.labels(
                    vehicle_id=vehicle_id,
                    fleet_id=fleet_id,
                    warning_type=warning.get('type', 'unknown')
                ).inc()
    
    async def process_spark_metrics(self, data):
        """Process Spark metrics"""
        app_id = data.get('application_id', 'unknown')
        
        if 'jobs_active' in data:
            SPARK_JOBS_ACTIVE.labels(
                application_id=app_id,
                job_type=data.get('job_type', 'batch')
            ).set(data['jobs_active'])
        
        if 'tasks_running' in data:
            SPARK_TASKS_RUNNING.labels(
                application_id=app_id
            ).set(data['tasks_running'])
        
        if 'memory_used' in data:
            SPARK_MEMORY_USED.labels(
                application_id=app_id,
                memory_type=data.get('memory_type', 'heap')
            ).set(data['memory_used'])
        
        if 'processing_latency' in data:
            SPARK_PROCESSING_LATENCY.labels(
                application_id=app_id,
                stage=data.get('stage', 'unknown')
            ).observe(data['processing_latency'])

def main():
    """Main entry point"""
    # Start Prometheus HTTP server
    start_http_server(8000)
    logger.info("Prometheus metrics server started on port 8000")
    
    # Start metrics exporter
    exporter = VehicleMetricsExporter(
        kafka_brokers='kafka-service:9092',
        topics=['vehicle-telemetry', 'spark-metrics']
    )
    
    # Run async event loop
    loop = asyncio.get_event_loop()
    loop.run_until_complete(exporter.start())

if __name__ == '__main__':
    main()