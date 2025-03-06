#!/bin/bash
set -e

echo "===== Stopping any existing containers ====="
docker-compose down

echo "===== Starting Docker containers (Kafka, Flink) ====="
docker-compose up -d

echo "===== Waiting for Kafka to be ready ====="
# Wait for Kafka to be ready (using the healthcheck from docker-compose)
echo "Waiting for Kafka to be ready..."
attempt=1
max_attempts=30
until docker-compose exec -T kafka kafka-topics.sh --bootstrap-server kafka:9092 --list > /dev/null 2>&1
do
    if [ $attempt -eq $max_attempts ]; then
        echo "Kafka not ready after $max_attempts attempts, exiting."
        exit 1
    fi
    
    echo "Waiting for Kafka to be ready (attempt $attempt/$max_attempts)..."
    sleep 2
    attempt=$((attempt+1))
done

echo "Kafka is ready!"

echo "===== Creating Kafka topic if needed ====="
docker-compose exec -T kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic windows-events --partitions 1 --replication-factor 1

echo "===== Building all components ====="
./build.sh build-all

echo "===== Starting DataProducer in background ====="
# Run DataProducer in the background
java -cp DataProducer/build DataProducer &
PRODUCER_PID=$!
echo "DataProducer started with PID: $PRODUCER_PID"

# Save PID for cleanup
echo $PRODUCER_PID > .producer.pid

# Give the producer time to start
sleep 2

echo "===== Submitting DataConsumerFlink job to Flink ====="
# Submit job to Flink - set kafka address to the container name 'kafka'
./build.sh submit-flink-job -h localhost -k kafka:9092 -f jobmanager:8081

echo "===== Setup Complete ====="
echo "Flink UI available at: http://localhost:8081"
echo "Kafka UI available at: http://localhost:8080"
echo ""
echo "To stop everything, run: docker-compose down && kill \$(cat .producer.pid)"