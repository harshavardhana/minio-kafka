#!/bin/bash

CGO_ENABLED=0 go build --ldflags "-s -w"
CGO_ENABLED=0 go build --ldflags "-s -w" --tags consumer -o kafka-consumer

docker build . -t y4m4/http-kafka
docker push y4m4/http-kafka
