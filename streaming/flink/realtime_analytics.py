"""
Real-time Analytics with Apache Flink for Vehicle Telemetry Analytics Platform.
Processes streaming vehicle data for real-time insights and anomaly detection.
"""
import logging
import json
import time
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from enum import Enum
import numpy as np
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import MapFunction, FilterFunction, KeyedProcessFunction
from pyflink.common import WatermarkStrategy, Duration, Types, Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.datastream.window import TimeWindow
import pickle
from collections import deque
import hashlib
from scipy import stats

logger = logging.getLogger(__name__)


class TelemetryEventType(Enum):
    """Telemetry event types."""
    SPEED = "speed"
    TEMPERATURE = "temperature"
    FUEL = "fuel"
    RPM = "rpm"
    LOCATION = "location"
    FAULT = "fault"
    MAINTENANCE = "maintenance"


@dataclass
class TelemetryEvent:
    """Telemetry event data class."""
    event_id: str
    vehicle_id: str
    event_type: TelemetryEventType
    timestamp: datetime
    value: float
    unit: str
    location: Optional[Tuple[float, float]] = None  # (latitude, longitude)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'event_id': self.event_id,
            'vehicle_id': self.vehicle_id,
            'event_type': self.event_type.value,
            'timestamp': self.timestamp.isoformat(),
            'value': self.value,
            'unit': self.unit,
            'location': self.location,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TelemetryEvent':
        """Create from dictionary."""
        return cls(
            event_id=data['event_id'],
            vehicle_id=data['vehicle_id'],
            event_type=TelemetryEventType(data['event_type']),
            timestamp=datetime.fromisoformat(data['timestamp']),
            value=data['value'],
            unit=data['unit'],
            location=data.get('location'),
            metadata=data.get('metadata', {})
        )


@dataclass
class AnomalyDetectionResult:
    """Anomaly detection result."""
    vehicle_id: str
    timestamp: datetime
    metric: str
    value: float
    expected_range: Tuple[float, float]
    anomaly_score: float
    is_anomaly: bool
    severity: str  # low, medium, high, critical
    description: str
    recommendations: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'vehicle_id': self.vehicle_id,
            'timestamp': self.timestamp.isoformat(),
            'metric': self.metric,
            'value': self.value,
            'expected_range': self.expected_range,
            'anomaly_score': self.anomaly_score,
            'is_anomaly': self.is_anomaly,
            'severity': self.severity,
            'description': self.description,
            'recommendations': self.recommendations
        }


@dataclass
class RealTimeAggregate:
    """Real-time aggregate metrics."""
    vehicle_id: str
    window_start: datetime
    window_end: datetime
    metric: str
    count: int
    sum_value: float
    avg_value: float
    min_value: float
    max_value: float
    std_value: float
    percentiles: Dict[str, float] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'vehicle_id': self.vehicle_id,
            'window_start': self.window_start.isoformat(),
            'window_end': self.window_end.isoformat(),
            'metric': self.metric,
            'count': self.count,
            'sum_value': self.sum_value,
            'avg_value': self.avg_value,
            'min_value': self.min_value,
            'max_value': self.max_value,
            'std_value': self.std_value,
            'percentiles': self.percentiles
        }


class TelemetryDeserializer(MapFunction):
    """Deserialize JSON telemetry data."""
    
    def map(self, value: str) -> TelemetryEvent:
        """Deserialize JSON string to TelemetryEvent."""
        try:
            data = json.loads(value)
            return TelemetryEvent.from_dict(data)
        except Exception as e:
            logger.error(f"Failed to deserialize telemetry data: {e}")
            raise


class TelemetrySerializer(MapFunction):
    """Serialize TelemetryEvent to JSON."""
    
    def map(self, event: TelemetryEvent) -> str:
        """Serialize TelemetryEvent to JSON string."""
        return json.dumps(event.to_dict())


class AnomalyDetector(KeyedProcessFunction):
    """Anomaly detection for telemetry data."""
    
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.history = {}
        self.models = {}
    
    def process_element(self, event: TelemetryEvent, ctx: 'KeyedProcessFunction.Context'):
        """Process telemetry event for anomaly detection."""
        vehicle_id = event.vehicle_id
        metric = event.event_type.value
        
        # Initialize history for this vehicle/metric
        key = f"{vehicle_id}:{metric}"
        if key not in self.history:
            self.history[key] = deque(maxlen=self.window_size)
        
        # Add to history
        self.history[key].append(event.value)
        
        # Check if we have enough data
        if len(self.history[key]) < 10:  # Minimum samples for detection
            return
        
        # Detect anomalies
        values = list(self.history[key])
        anomaly_result = self._detect_anomaly(vehicle_id, metric, event, values)
        
        if anomaly_result.is_anomaly:
            # Emit anomaly event
            yield anomaly_result
    
    def _detect_anomaly(self, vehicle_id: str, metric: str, event: TelemetryEvent, 
                       values: List[float]) -> AnomalyDetectionResult:
        """Detect anomalies using statistical methods."""
        current_value = event.value
        
        # Calculate statistics
        mean = np.mean(values)
        std = np.std(values)
        
        if std == 0:
            std = 0.001  # Avoid division by zero
        
        # Z-score
        z_score = abs((current_value - mean) / std)
        
        # Determine if anomaly
        is_anomaly = z_score > 3.0  # 3 sigma rule
        
        # Calculate anomaly score (0-1)
        anomaly_score = min(z_score / 6.0, 1.0)  # Normalize to 0-1
        
        # Determine severity
        if z_score > 5.0:
            severity = "critical"
        elif z_score > 4.0:
            severity = "high"
        elif z_score > 3.0:
            severity = "medium"
        else:
            severity = "low"
        
        # Expected range (3 sigma)
        expected_min = mean - 3 * std
        expected_max = mean + 3 * std
        
        # Generate description
        if is_anomaly:
            direction = "above" if current_value > mean else "below"
            description = f"{metric} is {direction} expected range (z-score: {z_score:.2f})"
        else:
            description = f"{metric} is within expected range"
        
        # Generate recommendations
        recommendations = []
        if is_anomaly:
            if metric == "engine_temperature" and current_value > mean:
                recommendations.append("Check cooling system")
                recommendations.append("Reduce engine load")
            elif metric == "speed" and current_value > mean:
                recommendations.append("Reduce speed for safety")
            elif metric == "fuel_level" and current_value < mean:
                recommendations.append("Consider refueling soon")
        
        return AnomalyDetectionResult(
            vehicle_id=vehicle_id,
            timestamp=event.timestamp,
            metric=metric,
            value=current_value,
            expected_range=(expected_min, expected_max),
            anomaly_score=anomaly_score,
            is_anomaly=is_anomaly,
            severity=severity,
            description=description,
            recommendations=recommendations
        )


class RealTimeAggregator(KeyedProcessFunction):
    """Real-time aggregation of telemetry data."""
    
    def __init__(self, window_duration: Duration = Duration.seconds(60)):
        self.window_duration = window_duration
        self.aggregates = {}
    
    def process_element(self, event: TelemetryEvent, ctx: 'KeyedProcessFunction.Context'):
        """Process telemetry event for aggregation."""
        vehicle_id = event.vehicle_id
        metric = event.event_type.value
        
        # Get current window
        window_start = self._get_window_start(event.timestamp)
        window_key = f"{vehicle_id}:{metric}:{window_start.isoformat()}"
        
        # Initialize aggregate
        if window_key not in self.aggregates:
            self.aggregates[window_key] = {
                'vehicle_id': vehicle_id,
                'window_start': window_start,
                'window_end': window_start + self.window_duration,
                'metric': metric,
                'values': []
            }
        
        # Add value
        self.aggregates[window_key]['values'].append(event.value)
        
        # Emit aggregate if window is complete
        current_time = datetime.now()
        if current_time >= self.aggregates[window_key]['window_end']:
            aggregate = self._calculate_aggregate(self.aggregates[window_key])
            
            # Clean up
            del self.aggregates[window_key]
            
            # Emit aggregate
            yield aggregate
    
    def _get_window_start(self, timestamp: datetime) -> datetime:
        """Get window start time."""
        # Round down to nearest window duration
        seconds = int(timestamp.timestamp())
        window_seconds = int(self.window_duration.to_milliseconds() / 1000)
        rounded_seconds = (seconds // window_seconds) * window_seconds
        return datetime.fromtimestamp(rounded_seconds)
    
    def _calculate_aggregate(self, data: Dict[str, Any]) -> RealTimeAggregate:
        """Calculate aggregate metrics."""
        values = data['values']
        
        if not values:
            return None
        
        # Calculate basic statistics
        count = len(values)
        sum_value = sum(values)
        avg_value = sum_value / count
        min_value = min(values)
        max_value = max(values)
        
        # Calculate standard deviation
        if count > 1:
            std_value = np.std(values)
        else:
            std_value = 0.0
        
        # Calculate percentiles
        percentiles = {}
        if count >= 5:
            for p in [25, 50, 75, 95]:
                percentiles[f'p{p}'] = np.percentile(values, p)
        
        return RealTimeAggregate(
            vehicle_id=data['vehicle_id'],
            window_start=data['window_start'],
            window_end=data['window_end'],
            metric=data['metric'],
            count=count,
            sum_value=sum_value,
            avg_value=avg_value,
            min_value=min_value,
            max_value=max_value,
            std_value=std_value,
            percentiles=percentiles
        )


class PredictiveMaintenanceModel:
    """Predictive maintenance model for vehicle components."""
    
    def __init__(self):
        self.component_health = {}  # Component health scores
        self.failure_predictions = {}
    
    def update_health(self, vehicle_id: str, component: str, 
                     telemetry_data: Dict[str, float]) -> float:
        """Update component health score."""
        key = f"{vehicle_id}:{component}"
        
        if key not in self.component_health:
            self.component_health[key] = {
                'health_score': 100.0,
                'last_update': datetime.now(),
                'telemetry_history': []
            }
        
        # Calculate health degradation
        health_degradation = self._calculate_health_degradation(component, telemetry_data)
        
        # Update health score (0-100)
        current_health = self.component_health[key]['health_score']
        new_health = max(0.0, min(100.0, current_health - health_degradation))
        
        # Update history
        self.component_health[key]['health_score'] = new_health
        self.component_health[key]['last_update'] = datetime.now()
        self.component_health[key]['telemetry_history'].append(telemetry_data)
        
        # Keep only recent history
        if len(self.component_health[key]['telemetry_history']) > 1000:
            self.component_health[key]['telemetry_history'] = \
                self.component_health[key]['telemetry_history'][-1000:]
        
        # Check for failure prediction
        if new_health < 30.0:
            self._predict_failure(vehicle_id, component, new_health)
        
        return new_health
    
    def _calculate_health_degradation(self, component: str, 
                                     telemetry_data: Dict[str, float]) -> float:
        """Calculate health degradation based on telemetry."""
        degradation = 0.0
        
        if component == "engine":
            # Engine health based on temperature, oil pressure, RPM
            temp = telemetry_data.get('engine_temperature', 90)
            oil_pressure = telemetry_data.get('oil_pressure', 200)
            rpm = telemetry_data.get('rpm', 2000)
            
            # Temperature effect
            if temp > 120:
                degradation += 0.5
            elif temp > 100:
                degradation += 0.2
            
            # Oil pressure effect
            if oil_pressure < 150:
                degradation += 0.3
            elif oil_pressure < 180:
                degradation += 0.1
            
            # RPM effect (sustained high RPM)
            if rpm > 4000:
                degradation += 0.1
        
        elif component == "brakes":
            # Brake health based on temperature and pressure
            brake_temp = telemetry_data.get('brake_temperature', 50)
            brake_pressure = telemetry_data.get('brake_pressure', 20)
            
            if brake_temp > 200:
                degradation += 0.8
            elif brake_temp > 150:
                degradation += 0.3
            
            if brake_pressure < 15:
                degradation += 0.2
        
        elif component == "battery":
            # Battery health based on voltage
            voltage = telemetry_data.get('battery_voltage', 12.6)
            
            if voltage < 11.5:
                degradation += 1.0
            elif voltage < 12.0:
                degradation += 0.5
            elif voltage < 12.4:
                degradation += 0.1
        
        # Add small random degradation
        degradation += np.random.uniform(0.0, 0.05)
        
        return degradation
    
    def _predict_failure(self, vehicle_id: str, component: str, health_score: float):
        """Predict component failure."""
        key = f"{vehicle_id}:{component}"
        
        # Calculate time to failure (simplified)
        if health_score < 10:
            days_to_failure = 1
        elif health_score < 20:
            days_to_failure = 3
        elif health_score < 30:
            days_to_failure = 7
        else:
            return
        
        # Store prediction
        predicted_date = datetime.now() + timedelta(days=days_to_failure)
        
        self.failure_predictions[key] = {
            'vehicle_id': vehicle_id,
            'component': component,
            'current_health': health_score,
            'predicted_failure_date': predicted_date,
            'days_to_failure': days_to_failure,
            'confidence': max(0.0, min(1.0, (30 - health_score) / 30))
        }
    
    def get_predictions(self, vehicle_id: str = None) -> List[Dict[str, Any]]:
        """Get failure predictions."""
        if vehicle_id:
            predictions = [p for k, p in self.failure_predictions.items() 
                          if k.startswith(f"{vehicle_id}:")]
        else:
            predictions = list(self.failure_predictions.values())
        
        return predictions


class FlinkRealtimeAnalytics:
    """
    Apache Flink-based real-time analytics for vehicle telemetry.
    """
    
    def __init__(self, kafka_bootstrap_servers: List[str], 
                 checkpoint_interval: int = 60000):
        """
        Initialize Flink analytics.
        
        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers
            checkpoint_interval: Checkpoint interval in milliseconds
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.checkpoint_interval = checkpoint_interval
        
        # Create execution environment
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(4)
        
        # Enable checkpointing
        self.env.enable_checkpointing(checkpoint_interval)
        
        # Initialize models
        self.predictive_model = PredictiveMaintenanceModel()
        
        # Kafka topics
        self.input_topic = "vehicle-telemetry"
        self.anomaly_topic = "telemetry-anomalies"
        self.aggregate_topic = "telemetry-aggregates"
        self.prediction_topic = "predictive-maintenance"
    
    def create_telemetry_stream(self):
        """Create telemetry data stream from Kafka."""
        # Define Kafka consumer properties
        kafka_props = {
            'bootstrap.servers': ','.join(self.kafka_bootstrap_servers),
            'group.id': 'flink-telemetry-analytics',
            'auto.offset.reset': 'latest'
        }
        
        # Create Kafka consumer
        kafka_consumer = FlinkKafkaConsumer(
            self.input_topic,
            TelemetryDeserializer(),
            kafka_props
        )
        
        # Set watermark strategy
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()
        
        # Create data stream
        telemetry_stream = self.env.add_source(kafka_consumer) \
            .assign_timestamps_and_watermarks(watermark_strategy)
        
        return telemetry_stream
    
    def process_telemetry_stream(self, telemetry_stream):
        """Process telemetry stream with multiple analytics."""
        # 1. Filter valid events
        valid_events = telemetry_stream.filter(self._filter_valid_events)
        
        # 2. Key events by vehicle_id for parallel processing
        keyed_events = valid_events.key_by(lambda event: event.vehicle_id)
        
        # 3. Anomaly detection
        anomaly_detector = AnomalyDetector(window_size=100)
        anomaly_stream = keyed_events.process(anomaly_detector)
        
        # 4. Real-time aggregation
        aggregator = RealTimeAggregator(window_duration=Duration.seconds(60))
        aggregate_stream = keyed_events.process(aggregator)
        
        # 5. Predictive maintenance
        # This would be implemented as a separate process
        
        return anomaly_stream, aggregate_stream
    
    def _filter_valid_events(self, event: TelemetryEvent) -> bool:
        """Filter valid telemetry events."""
        # Check for required fields
        if not event.vehicle_id or not event.event_type:
            return False
        
        # Check timestamp (not too old)
        age = (datetime.now() - event.timestamp).total_seconds()
        if age > 3600:  # 1 hour
            return False
        
        # Check value range based on event type
        if event.event_type == TelemetryEventType.SPEED:
            return 0 <= event.value <= 300  # km/h
        elif event.event_type == TelemetryEventType.TEMPERATURE:
            return -50 <= event.value <= 150 # °C
        elif event.event_type == TelemetryEventType.FUEL:
            return 0 <= event.value <= 100  # percentage
        elif event.event_type == TelemetryEventType.RPM:
            return 0 <= event.value <= 10000
        
        return True
    
    def sink_to_kafka(self, stream, topic: str):
        """Sink stream to Kafka topic."""
        # Define Kafka producer properties
        kafka_props = {
            'bootstrap.servers': ','.join(self.kafka_bootstrap_servers),
            'transaction.timeout.ms': str(5 * 60 * 1000)  # 5 minutes
        }
        
        # Create Kafka producer
        kafka_producer = FlinkKafkaProducer(
            topic,
            TelemetrySerializer(),
            kafka_props
        )
        
        # Add sink
        stream.add_sink(kafka_producer)
    
    def run_predictive_maintenance(self, telemetry_stream):
        """Run predictive maintenance analytics."""
        # Process telemetry for predictive maintenance
        def process_for_maintenance(event: TelemetryEvent):
            # Extract component data from telemetry
            component_data = {}
            
            if event.event_type == TelemetryEventType.TEMPERATURE:
                if 'engine' in event.metadata.get('component', ''):
                    component_data['engine_temperature'] = event.value
                elif 'brake' in event.metadata.get('component', ''):
                    component_data['brake_temperature'] = event.value
            
            elif event.event_type == TelemetryEventType.RPM:
                component_data['rpm'] = event.value
            
            elif event.event_type == TelemetryEventType.FUEL:
                component_data['fuel_level'] = event.value
            
            # Update health for each component
            for component in ['engine', 'brakes', 'battery']:
                if component_data:
                    health = self.predictive_model.update_health(
                        event.vehicle_id,
                        component,
                        component_data
                    )
            
            return event
        
        # Apply processing
        maintenance_stream = telemetry_stream.map(process_for_maintenance)
        
        return maintenance_stream
    
    def execute(self, job_name: str = "VehicleTelemetryAnalytics"):
        """Execute the Flink job."""
        try:
            # Create telemetry stream
            telemetry_stream = self.create_telemetry_stream()
            
            # Process stream
            anomaly_stream, aggregate_stream = self.process_telemetry_stream(telemetry_stream)
            
            # Run predictive maintenance
            maintenance_stream = self.run_predictive_maintenance(telemetry_stream)
            
            # Sink to Kafka topics
            self.sink_to_kafka(anomaly_stream, self.anomaly_topic)
            self.sink_to_kafka(aggregate_stream, self.aggregate_topic)
            
            # Execute job
            logger.info(f"Starting Flink job: {job_name}")
            self.env.execute(job_name)
            
        except Exception as e:
            logger.error(f"Flink job execution failed: {e}")
            raise
    
    def get_realtime_metrics(self) -> Dict[str, Any]:
        """Get real-time metrics (for monitoring)."""
        # This would typically come from Flink metrics system
        # For now, return sample metrics
        
        predictions = self.predictive_model.get_predictions()
        
        return {
            'active_predictions': len(predictions),
            'critical_predictions': len([p for p in predictions 
                                        if p['current_health'] < 20]),
            'components_monitored': len(self.predictive_model.component_health),
            'last_update': datetime.now().isoformat()
        }


class RealtimeDashboard:
    """Real-time analytics dashboard."""
    
    def __init__(self, flink_analytics: FlinkRealtimeAnalytics):
        self.flink_analytics = flink_analytics
        self.metrics_history = deque(maxlen=100)
    
    def update_metrics(self):
        """Update real-time metrics."""
        metrics = self.flink_analytics.get_realtime_metrics()
        self.metrics_history.append({
            'timestamp': datetime.now(),
            **metrics
        })
    
    def create_dashboard(self) -> Dict[str, Any]:
        """Create dashboard data."""
        if not self.metrics_history:
            return {}
        
        latest = self.metrics_history[-1]
        
        # Get predictions
        predictions = self.flink_analytics.predictive_model.get_predictions()
        
        dashboard = {
            'timestamp': latest['timestamp'].isoformat(),
            'metrics': latest,
            'predictions': predictions[:10],  # Top 10 predictions
            'component_health': self._get_component_health_summary(),
            'history': list(self.metrics_history)
        }
        
        return dashboard
    
    def _get_component_health_summary(self) -> Dict[str, Any]:
        """Get component health summary."""
        health_data = self.flink_analytics.predictive_model.component_health
        
        summary = {
            'total_components': len(health_data),
            'healthy': 0,
            'warning': 0,
            'critical': 0
        }
        
        for key, data in health_data.items():
            health = data['health_score']
            
            if health >= 70:
                summary['healthy'] += 1
            elif health >= 30:
                summary['warning'] += 1
            else:
                summary['critical'] += 1
        
        return summary


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Flink Real-time Analytics")
    parser.add_argument("--kafka-servers", default="localhost:9092",
                       help="Kafka bootstrap servers (comma-separated)")
    parser.add_argument("--input-topic", default="vehicle-telemetry",
                       help="Input Kafka topic")
    parser.add_argument("--checkpoint-interval", type=int, default=60000,
                       help="Checkpoint interval in milliseconds")
    parser.add_argument("--job-name", default="VehicleTelemetryAnalytics",
                       help="Flink job name")
    
    args = parser.parse_args()
    
    # Parse Kafka servers
    kafka_servers = args.kafka_servers.split(',')
    
    # Create and run Flink analytics
    analytics = FlinkRealtimeAnalytics(
        kafka_bootstrap_servers=kafka_servers,
        checkpoint_interval=args.checkpoint_interval
    )
    
    analytics.input_topic = args.input_topic
    
    try:
        analytics.execute(args.job_name)
    except KeyboardInterrupt:
        logger.info("Shutting down Flink analytics...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")


if __name__ == "__main__":
    main()