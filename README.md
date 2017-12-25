# Amazon Movie Review

This repo is for ML Spec teams's certification project.

I use [Amazon Movie Reviews](https://snap.stanford.edu/data/web-Movies.html) data.
https://wiki.cloudera.com/display/FieldTechServices/Dataset+Repository#DatasetRepository-Retail

## How to use it

1. Run `setup.sh` for data preparation. It takes about 20 minutes to parse data into HDFS
2. Open workbench and run `data-preparation.py` to create tables
3. Run `data-visualization.py` to show summary of data
4. Run `build-recommender.py` for building a Spark ALS model
5. Run `fastfm-recommender.py` for building a Factorization Machines model
