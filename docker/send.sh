#!/usr/bin/bash

docker-compose exec broker1 /opt/bitnami/kafka/bin/kafka-console-producer.sh --bootstrap-server=desktop:9092 \
  --topic=topic2

#docker-compose exec broker1 /opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server=desktop:9092 \
#  --topic=topic2