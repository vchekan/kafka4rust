#!/usr/bin/bash

docker-compose exec broker1 /opt/bitnami/kafka/bin/kafka-topics.sh \
 --bootstrap-server=desktop:9092 --topic=topic2 --list
