
#!/bin/sh

echo "Creating Kafka topics..."

docker exec shop_kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic orders --partitions 3 --replication-factor 1

docker exec shop_kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic orders.DLQ --partitions 1 --replication-factor 1

echo "Kafka topics created successfully!"

echo "Available topics:"
docker exec shop_kafka kafka-topics --bootstrap-server localhost:9092 --list
