FROM azul/zulu-openjdk-debian:8

RUN apt-get install -y curl
# TODO: how to redirect to the nearest download and not hardcode a concrete mirror?
RUN curl -o /tmp/kafka.tgz http://www.gtlib.gatech.edu/pub/apache/kafka/2.3.1/kafka_2.12-2.3.1.tgz
# TODO: verify checksum
RUN tar -C /opt -xzf /tmp/kafka.tgz
RUN echo '' >> /opt/kafka_2.12-2.3.1/config/server.properties
RUN echo advertised.listeners=PLAINTEXT://127.0.0.1:9092 >> /opt/kafka_2.12-2.3.1/config/server.properties

EXPOSE 2181 9092

CMD /opt/kafka_2.12-2.3.1/bin/zookeeper-server-start.sh -daemon /opt/kafka_2.12-2.3.1/config/zookeeper.properties && \
    /opt/kafka_2.12-2.3.1/bin/kafka-server-start.sh /opt/kafka_2.12-2.3.1/config/server.properties