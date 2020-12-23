#!/bin/bash

docker-compose stop $@
# docker-compose rm -f -v $@
docker-compose create --force-recreate $1
sudo chmod -R 777 ../dags && sudo chmod -R 777 ../logs && sudo chmod -R 777 ../plugins
docker-compose start $1
