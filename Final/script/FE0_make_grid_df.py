#! /usr/bin/env python3.7


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
import gc
import numpy as np


def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def sparkMelt(frame, id_vars=None, value_vars=None, var_name=None, value_name=None):
    """
    Pandas melting functions implemented in Spark

    Args:
        frame (Spark DataFrame): Spark dataframe to work on.
        id_vars (list, optional): Column(s) to use as identifier variables. Defaults to None.
        value_vars (list, optional): Column(s) to unpivot. If not specified, uses all columns that are not set as id_vars. Defaults to None.
        var_name (list, optional): Name to use for the 'variable' column. Defaults to None. If None, use 'variable'.
        value_name (list, optional): Name to use for the 'value' column. Defaults to None. If None, use 'value'.

    Returns:
        [Spark DataFrame]: Unpivoted Spark DataFrame.
    """
    
    id_vars = id_vars if id_vars else frame.columns

    value_vars = [col_name for col_name in frame.columns if col_name not in id_vars] \
        if not value_vars else value_vars
    
    # if value_vars is None, no columns need to be melted
    if not value_vars:
        return frame
    
    var_name = 'variable' if not var_name else var_name
    value_name = 'value' if not value_name else value_name
    
    for col_name in value_vars:
        print(col_name)
        frame = frame.withColumn(col_name,
                                 F.struct(F.lit(col_name).alias('var_name'), F.col(col_name).alias('var_value')))
    
    frame = frame.withColumn('_zip', F.array(*value_vars)) \
        .select(*(id_vars + ['_zip'])) \
        .withColumn('_key_value', F.explode('_zip')) \
        .withColumn(var_name,  F.col('_key_value')['var_name']) \
        .withColumn(value_name,  F.col('_key_value')['var_value']) \
        .select(*(id_vars + [var_name, value_name]))

    return frame


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
train_df = spark.read.csv(raw_data_dir+'sales_train_validation.csv', header=True, inferSchema=True)

index_columns = ['id','item_id','dept_id','cat_id','store_id','state_id']
grid_df = pd.melt(train_df.filter(F.col("store_id") == target_Store).toPandas(), 
                  id_vars = index_columns, 
                  var_name = 'd', 
                  value_name = TARGET)

# To be able to make predictions, we need to add "test set" to our grid
add_grid = pd.DataFrame()
for i in range(1,29):
    temp_df = train_df \
        .select(*index_columns) \
        .filter(F.col("store_id") == target_Store) \
        .toPandas()
    # temp_df = temp_df.drop_duplicates()
    temp_df['d'] = 'd_'+ str(END_TRAIN+i)
    temp_df[TARGET] = np.nan
    add_grid = pd.concat([add_grid,temp_df])

grid_df = pd.concat([grid_df,add_grid])
grid_df = grid_df.reset_index(drop=True)

del temp_df, add_grid, train_df
gc.collect()

# Let's check our memory usage
print("{:>20}: {:>8}".format('Original grid_df',sizeof_fmt(grid_df.memory_usage(index=True).sum())))

for col in index_columns:
    grid_df[col] = grid_df[col].astype('category')

print("{:>20}: {:>8}".format('Reduced grid_df',sizeof_fmt(grid_df.memory_usage(index=True).sum())))

# Save
# grid_df.to_pickle('processing/grid_df.pkl')
grid_sdf = spark.createDataFrame(grid_df)
grid_sdf.write.format("parquet").options(header='true', inferschema='true').save(raw_data_dir+'/processing/grid_df')