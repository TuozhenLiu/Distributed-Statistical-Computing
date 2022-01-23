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
grid_sdf = spark.read.parquet(raw_data_dir+'/processing/grid_df')
prices_df = spark.read.csv(raw_data_dir+'sell_prices.csv', header=True, inferSchema=True)
prices_df = prices_df.filter(F.col("store_id") == target_Store)
calendar_df = spark.read.csv(raw_data_dir+'calendar.csv', header=True, inferSchema=True)

release_df = prices_df \
    .groupby(['store_id','item_id']) \
    .agg(F.min('wm_yr_wk').alias('release'))

# Now we can merge release_df
grid_sdf = grid_sdf.join(release_df, on = ['store_id','item_id'], how='left')

# We want to remove some "zeros" rows from grid_df to do it we need wm_yr_wk column let's merge partly calendar_df to have it
grid_sdf = grid_sdf.join(calendar_df.select('wm_yr_wk', 'd'), on = 'd', how='left')

# Now we can cutoff some rows
grid_sdf = grid_sdf.filter('wm_yr_wk >= release')
#     .withColumn('release', F.col('release')-F.min('release'))
# grid_sdf['release'] = grid_sdf['release'] - grid_sdf['release'].min()

# Save part 1
grid_sdf.write.format("parquet").options(header='true', inferschema='true').save(raw_data_dir+'/processing/grid_part_1')