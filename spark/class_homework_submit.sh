#! /bin/bash

PYSPARK_PYTHON=python3.7 spark-submit \
  --master local \
  $1