# scripts/start-kafka.sh
#!/bin/bash

set -e

echo "Starting Kafka cluster..."

# Set environment variables with defaults
KAFKA_BROKER_ID=${KAFKA_BROKER_ID:-1}
KAFKA_LISTENERS=${KAFKA_LISTENERS:-PLAINTEXT://0.0.0.0:9092}
KAFKA_ADVERTISED_LISTENERS=${KAFKA_ADVERTISED_LISTENERS:-PLAINTEXT://localhost:9092}
KAFKA_NUM_PARTITIONS=${KAFKA_NUM_PARTITIONS:-6}
KAFKA_DEFAULT_REPLICATION_FACTOR=${KAFKA_DEFAULT_REPLICATION_FACTOR:-3}
KAFKA_MIN_INSYNC_REPLICAS=${KAFKA_MIN_INSYNC_REPLICAS:-2}
KAFKA_ZOOKEEPER_CONNECT=${KAFKA_ZOOKEEPER_CONNECT:-localhost:2181}
KAFKA_AUTO_CREATE_TOPICS_ENABLE=${KAFKA_AUTO_CREATE_TOPICS_ENABLE:-false}
KAFKA_LOG_RETENTION_HOURS=${KAFKA_LOG_RETENTION_HOURS:-168}
KAFKA_LOG_RETENTION_BYTES=${KAFKA_LOG_RETENTION_BYTES:-1073741824}

# Create configuration file
cat > /etc/kafka/server.properties << EOF
# Kafka Server Configuration
broker.id=${KAFKA_BROKER_ID}
listeners=${KAFKA_LISTENERS}
advertised.listeners=${KAFKA_ADVERTISED_LISTENERS}
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
num.partitions=${KAFKA_NUM_PARTITIONS}
default.replication.factor=${KAFKA_DEFAULT_REPLICATION_FACTOR}
min.insync.replicas=${KAFKA_MIN_INSYNC_REPLICAS}
num.recovery.threads.per.data.dir=1
log.dirs=/opt/kafka/data
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=${KAFKA_ZOOKEEPER_CONNECT}
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=${KAFKA_AUTO_CREATE_TOPICS_ENABLE}
log.retention.hours=${KAFKA_LOG_RETENTION_HOURS}
log.retention.bytes=${KAFKA_LOG_RETENTION_BYTES}
log.cleanup.policy=delete
delete.topic.enable=true
compression.type=producer
message.max.bytes=10485760
replica.fetch.max.bytes=10485760
EOF

# Start Zookeeper if not provided
if [ -z "${EXTERNAL_ZOOKEEPER}" ] || [ "${EXTERNAL_ZOOKEEPER}" = "false" ]; then
    echo "Starting internal Zookeeper..."
    nohup ${KAFKA_HOME}/bin/zookeeper-server-start.sh /etc/kafka/zookeeper.properties > /opt/kafka/logs/zookeeper.log 2>&1 &
    sleep 10
fi

# Start Kafka
echo "Starting Kafka broker..."
exec ${KAFKA_HOME}/bin/kafka-server-start.sh /etc/kafka/server.properties