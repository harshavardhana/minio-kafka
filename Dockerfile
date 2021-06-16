FROM alpine

MAINTAINER MinIO Development "dev@min.io"

EXPOSE 4222

COPY http-kafka /http-kafka
COPY kafka-consumer /kafka-consumer

RUN apk update && apk add curl

ENTRYPOINT ["/http-kafka"]
