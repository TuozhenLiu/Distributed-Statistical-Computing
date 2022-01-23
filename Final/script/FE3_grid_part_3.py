#! /usr/bin/env python3.7

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
import gc
import numpy as np


# Global Vars
TARGET = 'sales'         # Our main target
END_TRAIN = 1913         # Last day in train set
MAIN_INDEX = ['id','d']  # We can identify item by these columns
raw_data_dir = "dbfs:/FileStore/M5/data/"
target_Store = "CA_1"

# Start SparkSession
spark = SparkSession \
    .builder \
    .appName("Final") \
    .master("yarn") \
    .getOrCreate()

# Load Data
calendar_df = spark.read.csv(raw_data_dir+'calendar.csv', header=True, inferSchema=True)
grid_sdf = spark.read.parquet(raw_data_dir+'/processing/grid_part_1')

# Merge calendar
grid_sdf = grid_sdf.select(MAIN_INDEX)
icols = ['date',
         'd',
         'event_name_1',
         'event_type_1',
         'event_name_2',
         'event_type_2',
         'snap_CA',
         'snap_TX',
         'snap_WI']
grid_sdf = grid_sdf.join(calendar_df.select(*icols), on='d', how='left')

# Make Dates Features
grid_sdf = grid_sdf.withColumn("tm_d", F.dayofmonth(grid_sdf.date)) \
    .withColumn("tm_w", F.weekofyear(grid_sdf.date)) \
    .withColumn("tm_m", F.month(grid_sdf.date)) \
    .withColumn("tm_y", F.year(grid_sdf.date) - 2010) \
    .withColumn("tm_dw", F.dayofweek(grid_sdf.date)) \
    .withColumn("tm_w_end", F.when(F.col("tm_dw")>=5, 1).otherwise(0))

# Save
grid_sdf.write.format("parquet").options(header='true', inferschema='true').save(raw_data_dir+'/processing/grid_part_3')