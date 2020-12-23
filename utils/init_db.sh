#!/bin/bash
sudo chmod -R 777 ../data
docker-compose up --build initdb
sudo chmod -R 777 ../data