#! /usr/bin/env python3.7


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
import gc
import numpy as np
from synapse.ml.lightgbm import LightGBMRegressor, LightGBMRegressionModel
from synapse.ml.train import ComputeModelStatistics
import matplotlib.pyplot as plt


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

# Load data
train_df = spark.read.parquet(raw_data_dir+'/processing/train_df')
test_df = spark.read.parquet(raw_data_dir+'/processing/test_df')
model = LightGBMRegressionModel.loadNativeModelFromFile(raw_data_dir+"/processing/lgbm")

# Feature Importances Visualization
print(model.getFeatureImportances())

assemblerInputs = [
    "item_idclassVec", "dept_idclassVec", "cat_idclassVec", 
    "event_name_1classVec", "event_type_1classVec", "event_name_2classVec", "event_type_2classVec", 
    "release", "sell_price", "price_max", "price_min", "price_std", "price_mean", "price_norm", "price_nunique", 
    "item_nunique", "price_momentum", "price_momentum_m", "price_momentum_y", 
    "snap_CA", "snap_TX", "snap_WI", "tm_d", "tm_w", "tm_m", "tm_y", "tm_dw", "tm_w_end", 
    "sales_lag_28", "sales_lag_29", "sales_lag_30",
    "sales_lag_31", "sales_lag_32", "sales_lag_33", "sales_lag_34",
    "sales_lag_35", "sales_lag_36", "sales_lag_37", "sales_lag_38",
    "sales_lag_39", "sales_lag_40", "sales_lag_41", "sales_lag_42",        
    "rolling_mean_7", "rolling_std_7", "rolling_mean_14", "rolling_std_14",
    "rolling_mean_30", "rolling_std_30", "rolling_mean_60",
    "rolling_std_60", "rolling_mean_180", "rolling_std_180",
    "enc_store_id_dept_id_mean", "enc_store_id_dept_id_std", 
    "enc_item_id_state_id_mean", "enc_item_id_state_id_std"
]

feature_importances = model.getFeatureImportances()
fi = pd.Series(feature_importances[len(feature_importances)-50:], index=assemblerInputs[7:])
fi = fi.sort_values(ascending = True)
f_index = fi.index
f_values = fi.values
 
# print feature importances 
print ('f_index:',f_index)
print ('f_values:',f_values)

# plot
x_index = list(range(len(fi)))
x_index = [x/len(fi) for x in x_index]
plt.rcParams['figure.figsize'] = (20,20)
plt.barh(x_index,f_values,height = 0.028 ,align="center",color = 'tan',tick_label=f_index)
plt.xlabel('importances')
plt.ylabel('features')
plt.show()

# Prediction
scoredData = model.transform(test_df)
scoredData.show(10)

metrics = ComputeModelStatistics(evaluationMetric='regression',
                                 labelCol='label',
                                 scoresCol='prediction') \
            .transform(scoredData)
metrics.show()


