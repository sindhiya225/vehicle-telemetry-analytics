# scripts/health-check.sh
#!/bin/bash

# Health check for Kafka broker

# Check if Kafka process is running
if ! pgrep -f "kafka.Kafka" > /dev/null; then
    echo "Kafka process not found"
    exit 1
fi

# Check if Kafka is listening on the broker port
if ! netstat -tlnp | grep :9092 > /dev/null; then
    echo "Kafka not listening on port 9092"
    exit 1
fi

# Check Kafka metrics endpoint (if JMX is enabled)
if curl -s "http://localhost:8080/metrics" > /dev/null; then
    # Try to get broker state
    BROKER_STATE=$(echo "dump" | nc localhost 2181 2>/dev/null | grep brokers | wc -l)
    if [ "$BROKER_STATE" -eq "0" ]; then
        echo "Broker not registered in Zookeeper"
        exit 1
    fi
fi

# Test topic creation
TEST_TOPIC="_health_check_$(date +%s)"
${KAFKA_HOME}/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic $TEST_TOPIC --partitions 1 --replication-factor 1 --if-not-exists > /dev/null 2>&1

if [ $? -ne 0 ]; then
    echo "Failed to create test topic"
    exit 1
fi

# Clean up test topic
${KAFKA_HOME}/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --delete --topic $TEST_TOPIC > /dev/null 2>&1

echo "Kafka is healthy"
exit 0