#!/bin/bash

cd /home/jason/urbanpulse

source venv/bin/activate

nohup python -m producers.weather_producer > logs/weather.out 2>&1 &
nohup python -m producers.air_quality_producer > logs/air.out 2>&1 &
nohup python -m producers.traffic_producer > logs/traffic.out 2>&1 &

wait

