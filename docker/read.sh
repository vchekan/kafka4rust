#!/usr/bin/bash

docker-compose run broker1 /opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server=broker1:9092 \
  --topic=topic1