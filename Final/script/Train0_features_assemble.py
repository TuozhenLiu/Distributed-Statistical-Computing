#! /usr/bin/env python3.7

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
import gc
import numpy as np
from pyspark.ml.feature import VectorAssembler, OneHotEncoder, StringIndexer
from pyspark.ml import Pipeline


# Global Vars
TARGET = 'sales'         # Our main target
END_TRAIN = 1913         # Last day in train set
MAIN_INDEX = ['id','d']  # We can identify item by these columns
raw_data_dir = "dbfs:/FileStore/M5/data/"
target_Store = "CA_1"

grid1_colunm = ["id", "d", "item_id", "dept_id", "cat_id", "sales", "release"]

grid2_colunm = ["id", "d", 'sell_price', 'price_max', 'price_min', 'price_std',
               'price_mean', 'price_norm', 'price_nunique', 'item_nunique',
               'price_momentum', 'price_momentum_m', 'price_momentum_y']

grid3_colunm = ["id", "d", 'event_name_1', 'event_type_1', 'event_name_2',
               'event_type_2', 'snap_CA', 'snap_TX', 'snap_WI', 'tm_d', 'tm_w', 'tm_m',
               'tm_y', 'tm_dw', 'tm_w_end']

lag_colunm = ["id", "d", 'sales_lag_28', 'sales_lag_29', 'sales_lag_30',
             'sales_lag_31', 'sales_lag_32', 'sales_lag_33', 'sales_lag_34',
             'sales_lag_35', 'sales_lag_36', 'sales_lag_37', 'sales_lag_38',
             'sales_lag_39', 'sales_lag_40', 'sales_lag_41', 'sales_lag_42',
             
             'rolling_mean_7', 'rolling_std_7', 'rolling_mean_14', 'rolling_std_14',
             'rolling_mean_30', 'rolling_std_30', 'rolling_mean_60',
             'rolling_std_60', 'rolling_mean_180', 'rolling_std_180']

mean_enc_colunm = ["id", "d", 
    'enc_store_id_dept_id_mean', 'enc_store_id_dept_id_std', 
    'enc_item_id_state_id_mean', 'enc_item_id_state_id_std',
]

FIRST_DAY = 710

# Start SparkSession
spark = SparkSession \
    .builder \
    .appName("Final") \
    .master("yarn") \
    .getOrCreate()

# Load Data
grid_df = spark.read.parquet(raw_data_dir+'/processing/grid_part_1').select(*grid1_colunm)

@F.pandas_udf("int")
def extract_d(s: pd.Series) -> pd.Series:
    return s.map(lambda x: int(x.replace("d_", "")))

grid_df = grid_df.withColumn("d_int", extract_d("d")).filter(F.col("d_int") > FIRST_DAY).drop("d_int")

temp_df = spark.read.parquet(raw_data_dir+'/processing/grid_part_2', header=True, inferSchema=True).select(*grid2_colunm)
grid_df = grid_df.join(temp_df, on = MAIN_INDEX, how = "left")
temp_df = spark.read.parquet(raw_data_dir+'/processing/grid_part_3', header=True, inferSchema=True).select(*grid3_colunm)
grid_df = grid_df.join(temp_df, on = MAIN_INDEX, how = "left")
temp_df = spark.read.parquet(raw_data_dir+'/processing/lags_df_28', header=True, inferSchema=True).select(*lag_colunm)
grid_df = grid_df.join(temp_df, on = MAIN_INDEX, how = "left")
temp_df = spark.read.parquet(raw_data_dir+'/processing/mean_encoding_df', header=True, inferSchema=True).select(*mean_enc_colunm)
grid_df = grid_df.join(temp_df, on = MAIN_INDEX, how = "left")

grid_df = grid_df.drop("id")

# Fill / Drop NA
grid_df = grid_df.na.fill(subset=["event_name_1", "event_type_1", "event_name_2", "event_type_2"], value='NA')
grid_df = grid_df.dropna()

# Features Assemble
features_col = [col for col in grid_df.columns if col not in ['sales', 'd']]

stages = list()
categoricalColumns = ["item_id", "dept_id", "cat_id", "event_name_1", "event_type_1", "event_name_2", "event_type_2"]
for cate in categoricalColumns:
    indexer = StringIndexer().setInputCol(cate).setOutputCol(f"{cate}_Index")
    encoder = OneHotEncoder().setInputCol(indexer.getOutputCol()).setOutputCol(f"{cate}_classVec")
    stages.append(indexer)
    stages.append(encoder)

numericCols = [col for col in features_col if col not in categoricalColumns]
assemblerInputs = [col + "_classVec" for col in categoricalColumns] + numericCols
assembler = VectorAssembler().setInputCols(assemblerInputs).setOutputCol("features").setHandleInvalid("skip")
stages.append(assembler)

pipeline = Pipeline(stages=stages)

pipelineModel = pipeline.fit(grid_df)
pipelineModel.save(raw_data_dir + '/processing/FE_pipeline')

# Transform grid_df
train_df = pipelineModel.transform(grid_df).select(["d", F.col("sales").alias("label"), "features"])
train_df.withColumn("d_int", extract_d("d")).filter(F.col("d_int") > FIRST_DAY).drop("d_int")
test_df = train_df.withColumn("d_int", extract_d("d")).filter(F.col("d_int")>1885).select("label", "features")
train_df = train_df.withColumn("d_int", extract_d("d")).filter(F.col("d_int")<=1885).select("label", "features")

# Save
train_df.write.format("parquet").options(header='true', inferschema='true').save(raw_data_dir+'/processing/train_df')
test_df.write.format("parquet").options(header='true', inferschema='true').save(raw_data_dir+'/processing/test_df')