#!/usr/bin/env bash


#get python env
source /home/cris/myvenv/bin/activate

pip install findspark

cd /home/cris/intel

echo "# start example test"
start=$(date "+%s")

python icelake-250.py

now=$(date "+%s")
time1=$((now - start))

echo "Ice Lake epoch=250, time used: $time1 seconds"

