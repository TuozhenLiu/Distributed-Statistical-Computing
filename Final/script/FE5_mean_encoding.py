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
grid_sdf = spark.read.parquet(raw_data_dir+'/processing/grid_part_1')

base_cols = grid_sdf.columns
icols =  [
            ['state_id'],
            ['store_id'],
            ['cat_id'],
            ['dept_id'],
            ['state_id', 'cat_id'],
            ['state_id', 'dept_id'],
            ['store_id', 'cat_id'],
            ['store_id', 'dept_id'],
            ['item_id'],
            ['item_id', 'state_id'],
            ['item_id', 'store_id']
            ]
for col in icols:
    print('Encoding', col)
    col_name = '_'+'_'.join(col)+'_'
    temp_df = grid_sdf.groupby(col) \
        .agg(F.mean('sales').alias('enc'+col_name+'mean'),
             F.stddev('sales').alias('enc'+col_name+'std'))
    grid_sdf = grid_sdf.join(temp_df, on=col, how="left")

keep_cols = [col for col in grid_sdf.columns if col not in base_cols]
grid_sdf = grid_sdf.select(*(['id', 'd']+keep_cols))

# Save
grid_sdf.write.format("parquet").options(header='true', inferschema='true').save(raw_data_dir+'/processing/mean_encoding_df')