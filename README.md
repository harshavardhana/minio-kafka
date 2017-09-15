# minio-kafka

## TL;DR
How to setup testbed Minio + Apache Kafka (using Docker) for testing bucket notifications.

## Prerequisites
- Docker installed (Docker version 17.05.0-ce on Linux)
- docker-compose installed (1.8.0)

## Setup Minio and Apache Kafka with docker-compose
The setup will contain two containers:

- Minio server latest using local volumes for persistence
- Apache Kafka container (with Zookeeper built-in)

In order to join those two containers, we will use docker-compose. Create `docker-compose.yml` file and put the following content:

```yml
version: '2'

services:
  kafka:
    image: spotify/kafka
    environment:
      - ADVERTISED_HOST=kafka
      - ADVERTISED_PORT=9092
    ports:
      - "2181:2181"
      - "9092:9092"

  minio:
    image: minio/minio
    depends_on:
      - kafka
    ports:
      - "9000:9000"
    volumes:
      - minio-data:/export
      - minio-config:/root/.minio
    entrypoint: >
      /bin/sh -c "
      curl https://raw.githubusercontent.com/harshavardhana/minio-kafka/master/wait-for.sh -o wait-for.sh;
      [ ! -f /root/.minio/config.json ] && curl https://raw.githubusercontent.com/harshavardhana/minio-kafka/master/config.json -o /root/.minio/config.json;
      chmod +x wait-for.sh;
      ./wait-for.sh kafka:9092 -- /usr/bin/docker-entrypoint.sh minio server /export;
      "

volumes:
  minio-data:
  minio-config:
```

This docker-compose file declares two containers. First one, `kafka` runs Kafka image developed by Spotify team (it includes Zookeeper). It exposes itself under `kafka` hostname. Two environment variables need to be set:

```
ADVERTISED_HOST: kafka
ADVERTISED_PORT: 9092
```

Those two describe to which hostname the Kafka server should respond from within the container.

Second container, minio runs newest (at the time of writing this post) Minio server. It needs to connect to the running Kafka server, that's why there is the `depends_on` entry in docker-compose:

```yml
depends_on:
    - kafka
```

## Starting the environment
Once the `docker-compose.yml` file is ready, open your favorite terminal in the folder which contains `docker-compose.yml` and run:
```
docker-compose up
```

After running this command, you can check status of the containers by invoking:
```
docker-compose ps
```

## Stopping the environment
When the work is done, you can easily turn off running containers, by invoking following command in the folder where you have your `docker-compose.yml` file.
```
docker-compose down
```
