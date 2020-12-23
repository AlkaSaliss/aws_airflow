#!/bin/bash
sudo mkdir -p ../dags && sudo chmod -R 777 ../dags && mkdir -p ../logs && sudo chmod -R 777 ../logs && mkdir -p ../plugins && sudo chmod -R 777 ../plugins  && \
	docker-compose up --build webserver scheduler