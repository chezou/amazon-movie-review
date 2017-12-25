#!/bin/bash

set -ex

mkdir -p data
wget https://snap.stanford.edu/data/movies.txt.gz -O data/movies.txt.gz
hdfs dfs -mkdir movies
pip install -r requirements.txt -c constraints.txt
python movies.py | hdfs dfs -put - movies/movies.json
rm data/movies.txt.gz

wget https://snap.stanford.edu/data/bigdata/amazon/amazon-meta.txt.gz -O data/amazon-meta.txt
python parse-amazon-meta.py | hdfs dfs -put - movies/meta.json
rm data/amazon-meta.txt.gz
