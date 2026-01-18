"""
Kafka Consumer for Vehicle Telemetry Analytics Platform.
Consumes vehicle telemetry data from Kafka topics and processes it in real-time.
"""
import logging
import json
import time
import signal
import sys
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import numpy as np
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError, NoBrokersAvailable
from kafka.admin import KafkaAdminClient, NewTopic
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict, deque
import asyncio
import aiohttp
import aiokafka
from prometheus_client import Counter, Gauge, Histogram, start_http_server
import msgpack
import avro.schema
from avro.io import DatumReader, DatumWriter, BinaryDecoder, BinaryEncoder
import io

logger = logging.getLogger(__name__)


class ConsumerStrategy(Enum):
    """Consumer strategies for different use cases."""
    REAL_TIME = "real_time"      # Lowest latency
    BATCH = "batch"              # High throughput
    EXACTLY_ONCE = "exactly_once" # Exactly-once semantics
    PATTERN = "pattern"          # Pattern subscription


class DeserializationFormat(Enum):
    """Data serialization formats."""
    JSON = "json"
    AVRO = "avro"
    PROTOBUF = "protobuf"
    MSGPACK = "msgpack"
    PLAINTEXT = "plaintext"


@dataclass
class ConsumerConfig:
    """Kafka consumer configuration."""
    bootstrap_servers: List[str] = field(default_factory=lambda: ["localhost:9092"])
    group_id: str = "vehicle-analytics-group"
    auto_offset_reset: str = "latest"  # earliest, latest, none
    enable_auto_commit: bool = False   # Manual commit for better control
    auto_commit_interval_ms: int = 5000
    session_timeout_ms: int = 30000
    max_poll_records: int = 500
    max_poll_interval_ms: int = 300000
    fetch_max_bytes: int = 52428800    # 50MB
    fetch_max_wait_ms: int = 500
    fetch_min_bytes: int = 1
    heartbeat_interval_ms: int = 3000
    request_timeout_ms: int = 40000
    retry_backoff_ms: int = 100
    max_partition_fetch_bytes: int = 1048576  # 1MB
    security_protocol: str = "PLAINTEXT"
    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    sasl_mechanism: Optional[str] = None
    sasl_plain_username: Optional[str] = None
    sasl_plain_password: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        config = {
            'bootstrap_servers': self.bootstrap_servers,
            'group_id': self.group_id,
            'auto_offset_reset': self.auto_offset_reset,
            'enable_auto_commit': self.enable_auto_commit,
            'auto_commit_interval_ms': self.auto_commit_interval_ms,
            'session_timeout_ms': self.session_timeout_ms,
            'max_poll_records': self.max_poll_records,
            'max_poll_interval_ms': self.max_poll_interval_ms,
            'fetch_max_bytes': self.fetch_max_bytes,
            'fetch_max_wait_ms': self.fetch_max_wait_ms,
            'fetch_min_bytes': self.fetch_min_bytes,
            'heartbeat_interval_ms': self.heartbeat_interval_ms,
            'request_timeout_ms': self.request_timeout_ms,
            'retry_backoff_ms': self.retry_backoff_ms,
            'max_partition_fetch_bytes': self.max_partition_fetch_bytes
        }
        
        # Add security configuration
        if self.security_protocol != "PLAINTEXT":
            config['security_protocol'] = self.security_protocol
            
            if self.sasl_mechanism:
                config['sasl_mechanism'] = self.sasl_mechanism
                config['sasl_plain_username'] = self.sasl_plain_username
                config['sasl_plain_password'] = self.sasl_plain_password
            
            if self.ssl_cafile:
                config['ssl_cafile'] = self.ssl_cafile
                config['ssl_certfile'] = self.ssl_certfile
                config['ssl_keyfile'] = self.ssl_keyfile
        
        return config


@dataclass
class MessageMetadata:
    """Message metadata for tracking and monitoring."""
    topic: str
    partition: int
    offset: int
    timestamp: datetime
    key: Optional[bytes] = None
    headers: Optional[List[tuple]] = None
    size_bytes: int = 0
    processing_start_time: Optional[datetime] = None
    processing_end_time: Optional[datetime] = None
    
    @property
    def processing_time_ms(self) -> Optional[float]:
        """Get processing time in milliseconds."""
        if self.processing_start_time and self.processing_end_time:
            return (self.processing_end_time - self.processing_start_time).total_seconds() * 1000
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'topic': self.topic,
            'partition': self.partition,
            'offset': self.offset,
            'timestamp': self.timestamp.isoformat(),
            'key': self.key.decode() if self.key else None,
            'size_bytes': self.size_bytes,
            'processing_time_ms': self.processing_time_ms
        }


class Deserializer:
    """Base deserializer class."""
    
    @staticmethod
    def deserialize(data: bytes, format: DeserializationFormat = DeserializationFormat.JSON) -> Any:
        """Deserialize data based on format."""
        if format == DeserializationFormat.JSON:
            return json.loads(data.decode('utf-8'))
        elif format == DeserializationFormat.MSGPACK:
            return msgpack.unpackb(data, raw=False)
        elif format == DeserializationFormat.PLAINTEXT:
            return data.decode('utf-8')
        else:
            raise ValueError(f"Unsupported format: {format}")


class MessageProcessor:
    """Base message processor class."""
    
    def __init__(self, name: str = "default"):
        self.name = name
        self.processed_count = 0
        self.error_count = 0
        self.total_processing_time = 0.0
    
    def process(self, message: Any, metadata: MessageMetadata) -> Any:
        """Process a single message."""
        start_time = time.time()
        
        try:
            result = self._process_impl(message, metadata)
            self.processed_count += 1
            return result
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error processing message: {e}")
            raise
        finally:
            processing_time = time.time() - start_time
            self.total_processing_time += processing_time
    
    def _process_impl(self, message: Any, metadata: MessageMetadata) -> Any:
        """Implementation of message processing."""
        raise NotImplementedError
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processor statistics."""
        avg_time = self.total_processing_time / self.processed_count if self.processed_count > 0 else 0
        
        return {
            'name': self.name,
            'processed_count': self.processed_count,
            'error_count': self.error_count,
            'total_processing_time': self.total_processing_time,
            'avg_processing_time_ms': avg_time * 1000,
            'error_rate': self.error_count / max(self.processed_count, 1)
        }


class TelemetryProcessor(MessageProcessor):
    """Telemetry data processor."""
    
    def __init__(self):
        super().__init__(name="telemetry_processor")
        self.vehicle_stats = defaultdict(lambda: {
            'message_count': 0,
            'last_seen': None,
            'total_speed': 0.0,
            'max_speed': 0.0
        })
    
    def _process_impl(self, message: Dict[str, Any], metadata: MessageMetadata) -> Dict[str, Any]:
        """Process telemetry message."""
        vehicle_id = message.get('vehicle_id')
        timestamp = datetime.fromisoformat(message.get('timestamp', datetime.now().isoformat()))
        
        if vehicle_id:
            stats = self.vehicle_stats[vehicle_id]
            stats['message_count'] += 1
            stats['last_seen'] = timestamp
            
            # Update speed statistics
            speed = message.get('speed_kmh')
            if speed is not None:
                stats['total_speed'] += speed
                if speed > stats['max_speed']:
                    stats['max_speed'] = speed
        
        # Add processing metadata
        processed_message = message.copy()
        processed_message['processed_at'] = datetime.now().isoformat()
        processed_message['processor'] = self.name
        
        # Calculate derived metrics
        if 'speed_kmh' in message and 'rpm' in message:
            processed_message['speed_rpm_ratio'] = message['speed_kmh'] / max(message['rpm'], 1)
        
        if 'engine_temperature_c' in message and 'coolant_temperature_c' in message:
            processed_message['temp_difference'] = (
                message['engine_temperature_c'] - message['coolant_temperature_c']
            )
        
        return processed_message


class BatchProcessor:
    """Batch message processor for high-throughput scenarios."""
    
    def __init__(self, batch_size: int = 1000, timeout_seconds: int = 5):
        self.batch_size = batch_size
        self.timeout_seconds = timeout_seconds
        self.current_batch = []
        self.batch_lock = threading.Lock()
        self.last_batch_time = time.time()
        self.total_batches = 0
        self.total_messages = 0
    
    def add_message(self, message: Any, metadata: MessageMetadata) -> bool:
        """Add message to batch. Returns True if batch is ready."""
        with self.batch_lock:
            self.current_batch.append((message, metadata))
            self.total_messages += 1
            
            # Check if batch is ready
            batch_ready = len(self.current_batch) >= self.batch_size
            time_ready = (time.time() - self.last_batch_time) >= self.timeout_seconds
            
            return batch_ready or time_ready
    
    def get_batch(self) -> List[tuple]:
        """Get current batch and reset."""
        with self.batch_lock:
            batch = self.current_batch.copy()
            self.current_batch = []
            self.last_batch_time = time.time()
            self.total_batches += 1
            return batch
    
    def process_batch(self, processor: MessageProcessor) -> List[Any]:
        """Process a batch of messages."""
        batch = self.get_batch()
        results = []
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for message, metadata in batch:
                future = executor.submit(processor.process, message, metadata)
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error processing batch message: {e}")
        
        return results


class KafkaTelemetryConsumer:
    """
    High-performance Kafka consumer for vehicle telemetry data.
    """
    
    def __init__(self, config: ConsumerConfig, topics: List[str],
                 processor: MessageProcessor = None,
                 strategy: ConsumerStrategy = ConsumerStrategy.REAL_TIME):
        """
        Initialize Kafka consumer.
        
        Args:
            config: Consumer configuration
            topics: List of topics to subscribe to
            processor: Message processor
            strategy: Consumer strategy
        """
        self.config = config
        self.topics = topics
        self.processor = processor or TelemetryProcessor()
        self.strategy = strategy
        
        # Initialize metrics
        self.metrics = {
            'messages_received': Counter('kafka_messages_received_total', 
                                        'Total messages received'),
            'messages_processed': Counter('kafka_messages_processed_total', 
                                         'Total messages processed'),
            'processing_errors': Counter('kafka_processing_errors_total', 
                                        'Total processing errors'),
            'processing_latency': Histogram('kafka_processing_latency_seconds',
                                           'Message processing latency',
                                           buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
            'consumer_lag': Gauge('kafka_consumer_lag', 
                                 'Consumer lag per partition',
                                 ['topic', 'partition']),
            'consumer_throughput': Gauge('kafka_consumer_throughput_mps',
                                        'Consumer throughput in messages per second')
        }
        
        # State management
        self.is_running = False
        self.consumer = None
        self.producer = None  # For exactly-once semantics
        self.batch_processor = None
        
        # For batch processing
        if strategy == ConsumerStrategy.BATCH:
            self.batch_processor = BatchProcessor(batch_size=1000, timeout_seconds=5)
        
        # Offset management
        self.committed_offsets = {}
        
        # Start metrics server
        self._start_metrics_server()
    
    def _start_metrics_server(self, port: int = 9091):
        """Start Prometheus metrics server."""
        try:
            start_http_server(port)
            logger.info(f"Metrics server started on port {port}")
        except Exception as e:
            logger.warning(f"Failed to start metrics server: {e}")
    
    def connect(self) -> bool:
        """Connect to Kafka cluster."""
        try:
            # Create consumer
            self.consumer = KafkaConsumer(
                *self.topics,
                **self.config.to_dict()
            )
            
            logger.info(f"Connected to Kafka cluster at {self.config.bootstrap_servers}")
            logger.info(f"Subscribed to topics: {self.topics}")
            
            # Create producer for exactly-once semantics
            if self.strategy == ConsumerStrategy.EXACTLY_ONCE:
                producer_config = self.config.to_dict()
                producer_config.pop('group_id', None)
                producer_config.pop('enable_auto_commit', None)
                
                self.producer = KafkaProducer(**producer_config)
                logger.info("Producer created for exactly-once semantics")
            
            return True
            
        except NoBrokersAvailable as e:
            logger.error(f"No Kafka brokers available: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from Kafka."""
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting consumer: {e}")
        
        if self.producer:
            try:
                self.producer.close()
                logger.info("Kafka producer disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting producer: {e}")
    
    def _process_message_real_time(self, message) -> bool:
        """Process single message in real-time mode."""
        try:
            # Deserialize message
            value = Deserializer.deserialize(message.value, DeserializationFormat.JSON)
            
            # Create metadata
            metadata = MessageMetadata(
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
                timestamp=datetime.fromtimestamp(message.timestamp / 1000) if message.timestamp else datetime.now(),
                key=message.key,
                headers=message.headers,
                size_bytes=len(message.value),
                processing_start_time=datetime.now()
            )
            
            # Process message
            result = self.processor.process(value, metadata)
            
            # Update metadata
            metadata.processing_end_time = datetime.now()
            
            # Update metrics
            self.metrics['messages_processed'].inc()
            processing_time = metadata.processing_time_ms / 1000 if metadata.processing_time_ms else 0
            self.metrics['processing_latency'].observe(processing_time)
            
            logger.debug(f"Processed message from {message.topic}:{message.partition}:{message.offset}")
            
            return True
            
        except Exception as e:
            self.metrics['processing_errors'].inc()
            logger.error(f"Error processing message: {e}")
            return False
    
    def _process_message_exactly_once(self, message) -> bool:
        """Process message with exactly-once semantics."""
        # This is a simplified implementation
        # In production, you'd use Kafka transactions
        
        try:
            # Deserialize message
            value = Deserializer.deserialize(message.value, DeserializationFormat.JSON)
            
            # Create metadata
            metadata = MessageMetadata(
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
                timestamp=datetime.fromtimestamp(message.timestamp / 1000) if message.timestamp else datetime.now(),
                key=message.key,
                headers=message.headers,
                size_bytes=len(message.value),
                processing_start_time=datetime.now()
            )
            
            # Process message
            result = self.processor.process(value, metadata)
            
            # Produce result to output topic
            output_topic = f"{message.topic}-processed"
            future = self.producer.send(
                output_topic,
                key=message.key,
                value=json.dumps(result).encode('utf-8')
            )
            future.get(timeout=10)  # Wait for send
            
            # Update metadata
            metadata.processing_end_time = datetime.now()
            
            # Commit offset
            self.consumer.commit()
            
            # Update metrics
            self.metrics['messages_processed'].inc()
            
            logger.debug(f"Processed message with exactly-once semantics: {message.offset}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error in exactly-once processing: {e}")
            return False
    
    def _process_batch(self) -> bool:
        """Process batch of messages."""
        if not self.batch_processor:
            return False
        
        try:
            # Get batch
            batch = self.batch_processor.get_batch()
            if not batch:
                return True
            
            # Process batch
            results = self.batch_processor.process_batch(self.processor)
            
            # Update metrics
            self.metrics['messages_processed'].inc(len(results))
            
            logger.info(f"Processed batch of {len(results)} messages")
            
            return True
            
        except Exception as e:
            self.metrics['processing_errors'].inc()
            logger.error(f"Error processing batch: {e}")
            return False
    
    def consume(self, max_messages: Optional[int] = None):
        """Start consuming messages."""
        if not self.consumer:
            if not self.connect():
                logger.error("Failed to connect to Kafka")
                return
        
        self.is_running = True
        message_count = 0
        
        logger.info(f"Starting consumption with strategy: {self.strategy.value}")
        
        try:
            while self.is_running:
                try:
                    # Poll for messages
                    msg_batch = self.consumer.poll(timeout_ms=1000, max_records=self.config.max_poll_records)
                    
                    if not msg_batch:
                        # Check if batch needs processing (for batch strategy)
                        if self.strategy == ConsumerStrategy.BATCH:
                            self._process_batch()
                        continue
                    
                    for tp, messages in msg_batch.items():
                        for message in messages:
                            # Update metrics
                            self.metrics['messages_received'].inc()
                            
                            # Update consumer lag
                            topic = tp.topic
                            partition = tp.partition
                            self.metrics['consumer_lag'].labels(topic=topic, partition=partition).set(
                                message.highwatermark - message.offset
                            )
                            
                            # Process based on strategy
                            success = False
                            
                            if self.strategy == ConsumerStrategy.REAL_TIME:
                                success = self._process_message_real_time(message)
                                if success:
                                    self.consumer.commit()
                            
                            elif self.strategy == ConsumerStrategy.EXACTLY_ONCE:
                                success = self._process_message_exactly_once(message)
                            
                            elif self.strategy == ConsumerStrategy.BATCH:
                                # Deserialize and add to batch
                                value = Deserializer.deserialize(message.value, DeserializationFormat.JSON)
                                
                                metadata = MessageMetadata(
                                    topic=message.topic,
                                    partition=message.partition,
                                    offset=message.offset,
                                    timestamp=datetime.fromtimestamp(message.timestamp / 1000) if message.timestamp else datetime.now(),
                                    key=message.key,
                                    headers=message.headers,
                                    size_bytes=len(message.value)
                                )
                                
                                batch_ready = self.batch_processor.add_message(value, metadata)
                                
                                if batch_ready:
                                    self._process_batch()
                                    self.consumer.commit()
                                
                                success = True
                            
                            # Update message count
                            if success:
                                message_count += 1
                            
                            # Check max messages
                            if max_messages and message_count >= max_messages:
                                logger.info(f"Reached max messages limit: {max_messages}")
                                self.is_running = False
                                break
                        
                        if not self.is_running:
                            break
                    
                    # Update throughput metric
                    throughput = len(msg_batch) / 1.0  # Messages per second (simplified)
                    self.metrics['consumer_throughput'].set(throughput)
                
                except KeyboardInterrupt:
                    logger.info("Received interrupt signal")
                    self.is_running = False
                    break
                except Exception as e:
                    logger.error(f"Error in consumption loop: {e}")
                    time.sleep(1)  # Back off on error
        
        finally:
            # Process any remaining batch
            if self.strategy == ConsumerStrategy.BATCH and self.batch_processor:
                self._process_batch()
            
            # Commit offsets
            try:
                self.consumer.commit()
                logger.info("Committed final offsets")
            except Exception as e:
                logger.error(f"Error committing offsets: {e}")
            
            self.disconnect()
            logger.info("Consumption stopped")
    
    def seek_to_offset(self, topic: str, partition: int, offset: int):
        """Seek to specific offset."""
        if not self.consumer:
            logger.error("Consumer not connected")
            return
        
        try:
            tp = TopicPartition(topic, partition)
            self.consumer.seek(tp, offset)
            logger.info(f"Seeked to offset {offset} for {topic}:{partition}")
        except Exception as e:
            logger.error(f"Error seeking to offset: {e}")
    
    def get_consumer_lag(self) -> Dict[str, Any]:
        """Get consumer lag information."""
        if not self.consumer:
            return {}
        
        lag_info = {}
        
        for tp in self.consumer.assignment():
            committed = self.consumer.committed(tp)
            if committed is not None:
                self.consumer.seek_to_end(tp)
                end_offset = self.consumer.position(tp)
                lag = end_offset - committed
                
                lag_info[f"{tp.topic}:{tp.partition}"] = {
                    'committed_offset': committed,
                    'end_offset': end_offset,
                    'lag': lag
                }
        
        return lag_info
    
    def get_stats(self) -> Dict[str, Any]:
        """Get consumer statistics."""
        stats = {
            'consumer_strategy': self.strategy.value,
            'topics': self.topics,
            'group_id': self.config.group_id,
            'processor_stats': self.processor.get_stats(),
            'consumer_lag': self.get_consumer_lag(),
            'metrics': {}
        }
        
        # Add metrics
        for name, metric in self.metrics.items():
            if hasattr(metric, '_value'):
                stats['metrics'][name] = metric._value.get()
        
        return stats


class AsyncKafkaConsumer:
    """Asynchronous Kafka consumer using aiokafka."""
    
    def __init__(self, bootstrap_servers: List[str], topics: List[str], group_id: str):
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.group_id = group_id
        self.consumer = None
        self.is_running = False
    
    async def connect(self):
        """Connect to Kafka asynchronously."""
        try:
            self.consumer = aiokafka.AIOKafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='latest',
                enable_auto_commit=False
            )
            
            await self.consumer.start()
            logger.info(f"Async consumer connected to {self.bootstrap_servers}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect async consumer: {e}")
            return False
    
    async def consume(self, callback: Callable):
        """Consume messages asynchronously."""
        if not self.consumer:
            if not await self.connect():
                return
        
        self.is_running = True
        
        try:
            async for message in self.consumer:
                if not self.is_running:
                    break
                
                try:
                    # Deserialize message
                    value = json.loads(message.value.decode('utf-8'))
                    
                    # Call callback
                    await callback(value, message)
                    
                    # Commit offset
                    await self.consumer.commit()
                    
                except Exception as e:
                    logger.error(f"Error processing async message: {e}")
        
        except Exception as e:
            logger.error(f"Error in async consumption: {e}")
        
        finally:
            await self.disconnect()
    
    async def disconnect(self):
        """Disconnect async consumer."""
        if self.consumer:
            await self.consumer.stop()
            logger.info("Async consumer disconnected")


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, shutting down...")
    sys.exit(0)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka Telemetry Consumer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                       help="Kafka bootstrap servers (comma-separated)")
    parser.add_argument("--topics", default="vehicle-telemetry",
                       help="Kafka topics to consume (comma-separated)")
    parser.add_argument("--group-id", default="vehicle-analytics-group",
                       help="Consumer group ID")
    parser.add_argument("--strategy", default="real_time",
                       choices=["real_time", "batch", "exactly_once"],
                       help="Consumer strategy")
    parser.add_argument("--max-messages", type=int,
                       help="Maximum number of messages to consume")
    parser.add_argument("--metrics-port", type=int, default=9091,
                       help="Prometheus metrics port")
    
    args = parser.parse_args()
    
    # Parse arguments
    bootstrap_servers = args.bootstrap_servers.split(',')
    topics = args.topics.split(',')
    strategy = ConsumerStrategy(args.strategy)
    
    # Setup signal handling
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create consumer configuration
    config = ConsumerConfig(
        bootstrap_servers=bootstrap_servers,
        group_id=args.group_id,
        auto_offset_reset="latest",
        enable_auto_commit=False
    )
    
    # Create consumer
    processor = TelemetryProcessor()
    consumer = KafkaTelemetryConsumer(
        config=config,
        topics=topics,
        processor=processor,
        strategy=strategy
    )
    
    # Start consumption
    try:
        consumer.consume(max_messages=args.max_messages)
    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()