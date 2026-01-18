# scripts/start-spark.sh
#!/bin/bash

set -e

echo "Starting Spark cluster..."

# Set environment variables with defaults
SPARK_MODE=${SPARK_MODE:-master}
SPARK_MASTER_HOST=${SPARK_MASTER_HOST:-localhost}
SPARK_MASTER_PORT=${SPARK_MASTER_PORT:-7077}
SPARK_MASTER_URL=${SPARK_MASTER_URL:-spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}}
SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY:-2G}
SPARK_WORKER_CORES=${SPARK_WORKER_CORES:-2}
SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-1G}
SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-1G}
SPARK_EXECUTOR_CORES=${SPARK_EXECUTOR_CORES:-1}
SPARK_EXECUTOR_INSTANCES=${SPARK_EXECUTOR_INSTANCES:-2}

# Export environment variables
export SPARK_MASTER_HOST
export SPARK_MASTER_PORT
export SPARK_MASTER_URL
export SPARK_WORKER_MEMORY
export SPARK_WORKER_CORES
export SPARK_DRIVER_MEMORY
export SPARK_EXECUTOR_MEMORY
export SPARK_EXECUTOR_CORES
export SPARK_EXECUTOR_INSTANCES

# Create spark-defaults.conf
cat > ${SPARK_HOME}/conf/spark-defaults.conf << EOF
# Spark Configuration
spark.master                     ${SPARK_MASTER_URL}
spark.eventLog.enabled           true
spark.eventLog.dir               file:///opt/spark/logs
spark.history.fs.logDirectory    file:///opt/spark/logs
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.sql.extensions             io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog  org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.sql.streaming.checkpointLocation /opt/spark/checkpoints
spark.driver.memory              ${SPARK_DRIVER_MEMORY}
spark.executor.memory            ${SPARK_EXECUTOR_MEMORY}
spark.executor.cores             ${SPARK_EXECUTOR_CORES}
spark.executor.instances         ${SPARK_EXECUTOR_INSTANCES}
spark.dynamicAllocation.enabled  true
spark.dynamicAllocation.minExecutors 1
spark.dynamicAllocation.maxExecutors 10
spark.dynamicAllocation.initialExecutors ${SPARK_EXECUTOR_INSTANCES}
spark.streaming.backpressure.enabled true
spark.streaming.kafka.maxRatePerPartition 1000
spark.jars.packages             org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VERSION},io.delta:delta-core_2.13:2.4.0
EOF

# Start based on mode
case ${SPARK_MODE} in
    "master")
        echo "Starting Spark Master..."
        exec ${SPARK_HOME}/sbin/start-master.sh \
            --host ${SPARK_MASTER_HOST} \
            --port ${SPARK_MASTER_PORT} \
            --webui-port ${SPARK_MASTER_WEBUI_PORT}
        ;;
    "worker")
        echo "Starting Spark Worker..."
        if [ -z "${SPARK_MASTER_URL}" ]; then
            echo "SPARK_MASTER_URL must be set for worker mode"
            exit 1
        fi
        exec ${SPARK_HOME}/sbin/start-worker.sh \
            --webui-port ${SPARK_WORKER_WEBUI_PORT} \
            ${SPARK_MASTER_URL}
        ;;
    "history")
        echo "Starting Spark History Server..."
        exec ${SPARK_HOME}/sbin/start-history-server.sh
        ;;
    "submit")
        echo "Submitting Spark job..."
        if [ -z "${SPARK_JOB}" ]; then
            echo "SPARK_JOB must be set for submit mode"
            exit 1
        fi
        exec ${SPARK_HOME}/bin/spark-submit ${SPARK_JOB}
        ;;
    *)
        echo "Unknown SPARK_MODE: ${SPARK_MODE}"
        echo "Available modes: master, worker, history, submit"
        exit 1
        ;;
esac