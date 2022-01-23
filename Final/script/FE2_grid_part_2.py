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
prices_df = spark.read.csv(raw_data_dir+'sell_prices.csv', header=True, inferSchema=True)
prices_df = prices_df.filter(F.col("store_id") == target_Store)
calendar_df = spark.read.csv(raw_data_dir+'calendar.csv', header=True, inferSchema=True)
grid_sdf = spark.read.parquet(raw_data_dir+'/processing/grid_part_1')

# basic aggregations
@F.pandas_udf("store_id string, item_id string, wm_yr_wk int, sell_price double, price_max double, price_min double, price_std double, price_mean double, price_nunique int", F.PandasUDFType.GROUPED_MAP)  
def basic_agg(pdf):
    sell_price = pdf.sell_price
    pdf = pdf.assign(price_max=sell_price.max()) \
        .assign(price_min=sell_price.min()) \
        .assign(price_std=sell_price.std()) \
        .assign(price_mean=sell_price.mean()) \
        .assign(price_nunique=sell_price.nunique())
    return pdf
                     
prices_df = prices_df.groupby(['store_id','item_id']).apply(basic_agg)

# price_norm
prices_df = prices_df.withColumn("price_norm", F.col('sell_price') / F.col('price_max'))

# item_nunique
@F.pandas_udf("store_id string, item_id string, wm_yr_wk int, sell_price double, price_max double, price_min double, price_std double, price_mean double, price_nunique int, price_norm double, item_nunique int", F.PandasUDFType.GROUPED_MAP)  
def item_nunique(pdf):
    item_id = pdf.item_id
    pdf = pdf.assign(item_nunique=item_id.nunique())
    return pdf

prices_df = prices_df.groupby(['store_id','item_id']).apply(item_nunique)

calendar_prices = calendar_df.select('wm_yr_wk','month','year')
calendar_prices = calendar_prices.drop_duplicates(subset=['wm_yr_wk'])
prices_df = prices_df.join(calendar_prices, on='wm_yr_wk', how='left')

# Now we can add price "momentum" (some sort of)
# Shifted by week 
# by month mean
# by year mean

@F.pandas_udf("store_id string, item_id string, wm_yr_wk int, sell_price double, price_max double, price_min double, price_std double, price_mean double, price_nunique int, price_norm double, item_nunique int, price_momentum double", F.PandasUDFType.GROUPED_MAP)  
def calc_momentum(pdf):
    sell_price = pdf.sell_price
    pdf = pdf.assign(price_momentum = sell_price / sell_price.transform(lambda x: x.shift(1)))
    return pdf

prices_df = prices_df.toPandas()
prices_df['price_momentum'] = prices_df['sell_price']/prices_df.groupby(['store_id','item_id'])['sell_price'].transform(lambda x: x.shift(1))
prices_df['price_momentum_m'] = prices_df['sell_price']/prices_df.groupby(['store_id','item_id','month'])['sell_price'].transform('mean')
prices_df['price_momentum_y'] = prices_df['sell_price']/prices_df.groupby(['store_id','item_id','year'])['sell_price'].transform('mean')
del prices_df['month'], prices_df['year']
prices_df = spark.createDataFrame(prices_df)

grid_sdf = grid_sdf.select('id', 'd', 'store_id','item_id','wm_yr_wk') \
    .join(prices_df, on=['store_id','item_id','wm_yr_wk'], how='left')

# save
grid_sdf.write.format("parquet").options(header='true', inferschema='true').save(raw_data_dir+'/processing/grid_part_2')