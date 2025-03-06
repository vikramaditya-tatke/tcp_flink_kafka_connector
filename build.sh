#!/bin/bash
set -e

# Simplified build script with host IP hardcoded
HOST_IP="172.21.40.99"
PORT="9999"
KAFKA_BROKERS="kafka:9092"
KAFKA_TOPIC="windows-events"
FLINK_ADDRESS="localhost:8081"

case $1 in
    build-all)
        # Build DataProducer
        mkdir -p DataProducer/build
        javac -d DataProducer/build DataProducer/src/DataProducer.java
        
        # Build DataConsumerFlink
        cd DataConsumerFlink
        mvn clean package -q
        cd ..
        ;;
        
    run-producer)
        # Build if needed and run
        if [ ! -f "DataProducer/build/DataProducer.class" ]; then
            mkdir -p DataProducer/build
            javac -d DataProducer/build DataProducer/src/DataProducer.java
        fi
        java -cp DataProducer/build DataProducer
        ;;
        
    submit-flink-job)
        # Check and build if needed
        if [ ! -f "DataConsumerFlink/target/data-consumer-flink-1.0-SNAPSHOT.jar" ]; then
            cd DataConsumerFlink
            mvn clean package -q
            cd ..
        fi
        
        # Copy JAR and run Flink job
        JAR_FILE="$(pwd)/DataConsumerFlink/target/data-consumer-flink-1.0-SNAPSHOT.jar"
        CONTAINER_JAR="/tmp/data-consumer-flink.jar"
        docker cp "$JAR_FILE" apache_streampark-jobmanager-1:"$CONTAINER_JAR"
        
        docker exec apache_streampark-jobmanager-1 flink run \
            -d -m "$FLINK_ADDRESS" \
            -c com.example.DataConsumerFlink \
            "$CONTAINER_JAR" \
            --hostname "$HOST_IP" \
            --port "$PORT" \
            --kafka-brokers "$KAFKA_BROKERS" \
            --kafka-topic "$KAFKA_TOPIC"
        ;;
        
    *)
        echo "Usage: $0 {build-all|run-producer|submit-flink-job}"
        exit 1
        ;;
esac