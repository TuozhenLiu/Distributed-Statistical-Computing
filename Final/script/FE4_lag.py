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
grid_df = grid_sdf.select('id', 'd', 'sales').toPandas()

# Lags with 28 day shift
SHIFT_DAY = 28
LAG_DAYS = [col for col in range(SHIFT_DAY,SHIFT_DAY+15)]
grid_df = grid_df.assign(**{
        '{}_lag_{}'.format(col, l): grid_df.groupby(['id'])[col].transform(lambda x: x.shift(l))
        for l in LAG_DAYS
        for col in [TARGET]
    })
# Minify lag columns
for col in list(grid_df):
    if 'lag' in col:
        grid_df[col] = grid_df[col].astype(np.float16)

# Rollings with 28 day shift
print('Create rolling aggs')
for i in [7,14,30,60,180]:
    print('Rolling period:', i)
    grid_df['rolling_mean_'+str(i)] = grid_df \
        .groupby(['id'])[TARGET] \
        .transform(lambda x: x.shift(SHIFT_DAY).rolling(i).mean()) \
        .astype(np.float16)
    grid_df['rolling_std_'+str(i)]  = grid_df \
        .groupby(['id'])[TARGET] \
        .transform(lambda x: x.shift(SHIFT_DAY).rolling(i).std()) \
        .astype(np.float16)

# Rollings with sliding shift
for d_shift in [1,7,14]: 
    print('Shifting period:', d_shift)
    for d_window in [7,14,30,60]:
        col_name = 'rolling_mean_tmp_'+str(d_shift)+'_'+str(d_window)
        grid_df[col_name] = grid_df \
            .groupby(['id'])[TARGET] \
            .transform(lambda x: x.shift(d_shift).rolling(d_window).mean()) \
            .astype(np.float16)

grid_sdf = spark.createDataFrame(grid_df)

# Save
grid_sdf.write.format("parquet").options(header='true', inferschema='true').save(raw_data_dir+'/processing/lags_df_28')