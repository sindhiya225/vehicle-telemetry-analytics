import json
import time
import random
from datetime import datetime
from typing import Dict, List
import asyncio
from dataclasses import dataclass
from dataclasses_json import dataclass_json

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

import yaml
import pandas as pd
import numpy as np
from faker import Faker

@dataclass_json
@dataclass
class TelemetryEvent:
    event_id: str
    timestamp: int
    vehid: int
    vehicle_type: str
    vehicle_class: str
    trip_id: str
    trip_distance_km: float
    fuel_consumed_l: float
    fuel_efficiency_kmpl: float
    avg_speed_kmph: float
    avg_rpm: int
    avg_engine_temp_c: float
    idle_time_min: float
    has_fault: bool
    fault_codes: List[str]
    maintenance_flag: bool
    weather_condition: str
    road_type: str
    driver_behavior_score: float
    location_lat: float = None
    location_lon: float = None
    altitude: float = None

class VehicleTelemetryProducer:
    def __init__(self, config_path: str = 'streaming/kafka/kafka_config.yaml'):
        """Initialize Kafka producer with Avro serialization"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize Kafka producer
        producer_config = {
            'bootstrap.servers': self.config['kafka']['bootstrap_servers'],
            'client.id': 'vehicle-telemetry-producer',
            'acks': 'all',
            'retries': 3,
            'compression.type': self.config['kafka']['compression'],
            'batch.size': 16384,
            'linger.ms': 5,
            'partitioner': 'consistent_random'
        }
        
        self.producer = Producer(producer_config)
        
        # Initialize Schema Registry
        schema_registry_config = {
            'url': self.config['kafka']['schema_registry_url']
        }
        self.schema_registry_client = SchemaRegistryClient(schema_registry_config)
        
        # Load Avro schemas
        self.telemetry_schema = self.config['avro_schemas']['telemetry_event']
        self.anomaly_schema = self.config['avro_schemas']['anomaly_event']
        self.maintenance_schema = self.config['avro_schemas']['maintenance_alert']
        
        # Create serializers
        self.telemetry_serializer = AvroSerializer(
            self.schema_registry_client,
            self.telemetry_schema,
            lambda obj, ctx: obj.to_dict()
        )
        
        self.anomaly_serializer = AvroSerializer(
            self.schema_registry_client,
            self.anomaly_schema,
            lambda obj, ctx: obj
        )
        
        self.maintenance_serializer = AvroSerializer(
            self.schema_registry_client,
            self.maintenance_schema,
            lambda obj, ctx: obj
        )
        
        self.fake = Faker()
        self.vehicle_data = None
        self.load_vehicle_data()
    
    def load_vehicle_data(self):
        """Load vehicle master data"""
        try:
            self.vehicle_data = pd.read_csv('data/processed/cleaned_vehicle_data.csv')
            print(f"Loaded {len(self.vehicle_data)} vehicles")
        except FileNotFoundError:
            print("Vehicle data not found, using simulated data")
            self.vehicle_data = self.generate_simulated_vehicles(100)
    
    def generate_simulated_vehicles(self, count: int) -> pd.DataFrame:
        """Generate simulated vehicle data"""
        vehicles = []
        for i in range(count):
            vehicle = {
                'vehid': i + 1,
                'vehicle_type': random.choice(['ICE', 'HEV']),
                'vehicle_class': random.choice(['Compact', 'Midsize', 'SUV', 'Truck']),
                'engine_type': random.choice(['Gasoline', 'Diesel', 'Hybrid']),
                'displacement_l': round(random.uniform(1.0, 6.0), 1),
                'is_turbo': random.choice([True, False]),
                'is_hybrid': random.choice([True, False]),
                'weight_category': random.choice(['Light', 'Medium', 'Heavy']),
                'transmission_type': random.choice(['Automatic', 'Manual', 'CVT']),
                'efficiency_category': random.choice(['Low', 'Medium', 'High'])
            }
            vehicles.append(vehicle)
        return pd.DataFrame(vehicles)
    
    def generate_telemetry_event(self) -> TelemetryEvent:
        """Generate a single telemetry event"""
        # Randomly select a vehicle
        vehicle = self.vehicle_data.sample(1).iloc[0]
        
        # Generate realistic telemetry data
        base_efficiency = 15.0 if vehicle['is_hybrid'] else 10.0
        if vehicle['is_turbo']:
            base_efficiency *= 0.9
        
        trip_distance = max(0.5, np.random.exponential(scale=15))
        fuel_consumed = trip_distance / base_efficiency * random.uniform(0.8, 1.2)
        
        # Generate fault with probability
        has_fault = random.random() < 0.05
        fault_codes = []
        if has_fault:
            fault_codes = random.sample(['P0300', 'P0420', 'P0171', 'P0442'], 
                                       random.randint(1, 2))
        
        # Generate maintenance flag
        maintenance_flag = random.random() < 0.03
        
        # Generate location data
        location = self.fake.local_latlng()
        
        event = TelemetryEvent(
            event_id=f"event_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            timestamp=int(time.time() * 1000),
            vehid=int(vehicle['vehid']),
            vehicle_type=vehicle['vehicle_type'],
            vehicle_class=vehicle['vehicle_class'],
            trip_id=f"trip_{int(time.time())}_{random.randint(100, 999)}",
            trip_distance_km=round(trip_distance, 2),
            fuel_consumed_l=round(fuel_consumed, 2),
            fuel_efficiency_kmpl=round(trip_distance / max(fuel_consumed, 0.1), 2),
            avg_speed_kmph=round(random.uniform(20, 100), 1),
            avg_rpm=random.randint(1500, 3500),
            avg_engine_temp_c=round(random.uniform(85, 105), 1),
            idle_time_min=round(trip_distance / 30 * random.uniform(0.05, 0.2) * 60, 1),
            has_fault=has_fault,
            fault_codes=fault_codes,
            maintenance_flag=maintenance_flag,
            weather_condition=random.choice(['Clear', 'Rain', 'Snow', 'Fog']),
            road_type=random.choice(['Urban', 'Highway', 'Mixed', 'Rural']),
            driver_behavior_score=round(random.uniform(0.6, 1.0), 2),
            location_lat=float(location[0]),
            location_lon=float(location[1]),
            altitude=round(random.uniform(0, 2000), 1)
        )
        
        return event
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def produce_telemetry(self, events_per_second: int = 10):
        """Produce telemetry events to Kafka"""
        print(f"Starting telemetry producer at {events_per_second} events/second")
        
        try:
            while True:
                for _ in range(events_per_second):
                    event = self.generate_telemetry_event()
                    
                    # Serialize with Avro
                    serialized_event = self.telemetry_serializer(
                        event,
                        SerializationContext(
                            self.config['kafka']['topics']['raw_telemetry'],
                            MessageField.VALUE
                        )
                    )
                    
                    # Produce to Kafka
                    self.producer.produce(
                        topic=self.config['kafka']['topics']['raw_telemetry'],
                        value=serialized_event,
                        key=str(event.vehid).encode('utf-8'),
                        callback=self.delivery_report
                    )
                
                # Flush messages
                self.producer.flush()
                
                # Wait for next second
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("Stopping producer...")
        finally:
            self.producer.flush()
    
    def produce_batch(self, num_events: int):
        """Produce a batch of telemetry events"""
        print(f"Producing {num_events} telemetry events...")
        
        for i in range(num_events):
            event = self.generate_telemetry_event()
            
            serialized_event = self.telemetry_serializer(
                event,
                SerializationContext(
                    self.config['kafka']['topics']['raw_telemetry'],
                    MessageField.VALUE
                )
            )
            
            self.producer.produce(
                topic=self.config['kafka']['topics']['raw_telemetry'],
                value=serialized_event,
                key=str(event.vehid).encode('utf-8')
            )
            
            if i % 1000 == 0:
                print(f"Produced {i} events...")
        
        self.producer.flush()
        print(f"Finished producing {num_events} events")
    
    def create_topics(self):
        """Create Kafka topics if they don't exist"""
        from confluent_kafka.admin import AdminClient, NewTopic
        
        admin_client = AdminClient({
            'bootstrap.servers': self.config['kafka']['bootstrap_servers']
        })
        
        topics = []
        for topic_name in self.config['kafka']['topics'].values():
            topics.append(NewTopic(
                topic_name,
                num_partitions=self.config['kafka']['partitions'],
                replication_factor=self.config['kafka']['replication_factor']
            ))
        
        # Create topics
        fs = admin_client.create_topics(topics)
        
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Topic {topic} created")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Vehicle Telemetry Kafka Producer')
    parser.add_argument('--mode', choices=['stream', 'batch'], default='stream',
                       help='Production mode: stream (continuous) or batch (one-time)')
    parser.add_argument('--rate', type=int, default=10,
                       help='Events per second for streaming mode')
    parser.add_argument('--batch-size', type=int, default=10000,
                       help='Number of events for batch mode')
    parser.add_argument('--create-topics', action='store_true',
                       help='Create Kafka topics before producing')
    
    args = parser.parse_args()
    
    producer = VehicleTelemetryProducer()
    
    if args.create_topics:
        producer.create_topics()
    
    if args.mode == 'stream':
        producer.produce_telemetry(args.rate)
    else:
        producer.produce_batch(args.batch_size)