## 1. Create network

```shell
docker network create streaming-network --driver bridge
```

## 2. Run kafka

```shell
docker compose up -d
```

**Check Status & Logs**

```shell
docker compose ps
docker compose logs kafka-0 -f -n 100
```

**Testing**

Run inside kafka's containers

```shell
docker exec -ti kafka-kafa-0-1 bash
```

Producer

```shell
kafka-console-producer --producer.config /etc/kafka/producer.properties --bootstrap-server kafka-0:9092,kafka-1:9092,kafka-2:9092 --topic test
```

Consumer

```shell
kafka-console-consumer --consumer.config /etc/kafka/consumer.properties --bootstrap-server kafka-0:9092,kafka-1:9092,kafka-2:9092 --topic test --from-beginning
```

## 3. Monitor

[akqh](http://localhost:8180)

## References

[Quick Start for Confluent Platform](https://docs.confluent.io/platform/current/platform-quickstart.html#quick-start-for-cp)

[Docker Image Reference for Confluent Platform](https://docs.confluent.io/platform/current/installation/docker/image-reference.html#docker-image-reference-for-cp)

[akhq configuration](https://akhq.io/docs/configuration/brokers.html)

[Docker Image Configuration Reference for Confluent Platform](https://docs.confluent.io/platform/current/installation/docker/config-reference.html)