# Amazon Movie Review

This is an example code to build recommenders with/without Spark.

Original data is [Amazon Movie Reviews](https://snap.stanford.edu/data/web-Movies.html) provided by Stanford Network Analysis Project
.

This repo includes:

- Parse amazon movie reviews data and amazon meta data.
- Create tables with Spark.
- Visualize basic stats of review data.
- Build recommender with ALS from spark.ml.
- Build recommender with Factorization Machine using [fastFM](https://github.com/ibayer/fastFM/).

## How to use it

1. Open workbench and run `setup.sh` for data preparation on the terminal. It takes about 20 minutes to parse data and put JSON into HDFS
2. Run `data-preparation.py` to create tables
3. Run `data-visualization.py` to show summary of data
4. Run `build-recommender.py` for building a Spark ALS model
5. Run `fastfm-recommender.py` for building a Factorization Machines model
