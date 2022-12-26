#!/usr/bin/env bash


#get python env
source /home/cris/myvenv/bin/activate

pip install findspark

cd /home/cris/intel

echo "# start example test"
start=$(date "+%s")

python sapphirerapids-100.py

now=$(date "+%s")
time1=$((now - start))

echo "Sapphire Rapids epoch=100, time used: $time1 seconds"

