#!/bin/bash

mkdir -p data
wget https://snap.stanford.edu/data/movies.txt.gz -O data/movies.txt.gz
hdfs dfs -mkdir movies
pip install -r requirements.txt -c constraints.txt
python movies.py | hdfs dfs -put - movies/movies.json
