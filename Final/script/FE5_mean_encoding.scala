import spark.implicits._
import org.apache.spark.sql.{ColumnName, DataFrame, Encoders, Row, SparkSession, functions}

// Global Vars
val TARGET = "sales"         // Our main target
val END_TRAIN = 1913         // Last day in train set
val raw_data_dir = "dbfs:/FileStore/M5/data/"
val target_Store = "CA_1"

// Start SparkSession
val spark = SparkSession
  .builder()
  .appName("Final")
  .master("local")
  .getOrCreate()

// Load Data
// var grid_sdf = spark.read.options(Map("header"->"true", "inferSchema"->"true")).csv(raw_data_dir+"/processing/grid_part_1")
var grid_sdf = spark.read.parquet(raw_data_dir+"/processing/grid_part_1")

val base_cols = grid_sdf.columns

var col_name = "_" + "state_id" + "_"
var icol = Array($"state_id")
var joincol = Seq("state_id")
var temp_df = grid_sdf.groupBy(icol: _*)
  .agg(functions.mean("sales").alias("enc"+col_name+"mean"),
       functions.stddev("sales").alias("enc"+col_name+"std"))
grid_sdf = grid_sdf.join(temp_df, joincol, "left")

col_name = "_" + "store_id" + "_"
icol = Array($"store_id")
joincol = Seq("store_id")
temp_df = grid_sdf.groupBy(icol: _*)
  .agg(functions.mean("sales").alias("enc"+col_name+"mean"),
       functions.stddev("sales").alias("enc"+col_name+"std"))
grid_sdf = grid_sdf.join(temp_df, joincol, "left")

col_name = "_" + "cat_id" + "_"
icol = Array($"cat_id")
joincol = Seq("cat_id")
temp_df = grid_sdf.groupBy(icol: _*)
  .agg(functions.mean("sales").alias("enc"+col_name+"mean"),
       functions.stddev("sales").alias("enc"+col_name+"std"))
grid_sdf = grid_sdf.join(temp_df, joincol, "left")

col_name = "_" + "dept_id" + "_"
icol = Array($"dept_id")
joincol = Seq("dept_id")
temp_df = grid_sdf.groupBy(icol: _*)
  .agg(functions.mean("sales").alias("enc"+col_name+"mean"),
       functions.stddev("sales").alias("enc"+col_name+"std"))
grid_sdf = grid_sdf.join(temp_df, joincol, "left")

col_name = "_" + "state_id" + "_" + "cat_id" + "_"
icol = Array($"state_id", $"cat_id")
joincol = Seq("state_id", "cat_id")
temp_df = grid_sdf.groupBy(icol: _*)
  .agg(functions.mean("sales").alias("enc"+col_name+"mean"),
       functions.stddev("sales").alias("enc"+col_name+"std"))
grid_sdf = grid_sdf.join(temp_df, joincol, "left")

col_name = "_" + "state_id" + "_" + "dept_id" + "_"
icol = Array($"state_id", $"dept_id")
joincol = Seq("state_id", "dept_id")
temp_df = grid_sdf.groupBy(icol: _*)
  .agg(functions.mean("sales").alias("enc"+col_name+"mean"),
       functions.stddev("sales").alias("enc"+col_name+"std"))
grid_sdf = grid_sdf.join(temp_df, joincol, "left")

col_name = "_" + "store_id" + "_" + "cat_id" + "_"
icol = Array($"store_id", $"cat_id")
joincol = Seq("store_id", "cat_id")
temp_df = grid_sdf.groupBy(icol: _*)
  .agg(functions.mean("sales").alias("enc"+col_name+"mean"),
       functions.stddev("sales").alias("enc"+col_name+"std"))
grid_sdf = grid_sdf.join(temp_df, joincol, "left")

col_name = "_" + "store_id" + "_" + "dept_id" + "_"
icol = Array($"store_id", $"dept_id")
joincol = Seq("store_id", "dept_id")
temp_df = grid_sdf.groupBy(icol: _*)
  .agg(functions.mean("sales").alias("enc"+col_name+"mean"),
       functions.stddev("sales").alias("enc"+col_name+"std"))
grid_sdf = grid_sdf.join(temp_df, joincol, "left")

col_name = "_" + "item_id" + "_"
icol = Array($"item_id")
joincol = Seq("item_id")
temp_df = grid_sdf.groupBy(icol: _*)
  .agg(functions.mean("sales").alias("enc"+col_name+"mean"),
       functions.stddev("sales").alias("enc"+col_name+"std"))
grid_sdf = grid_sdf.join(temp_df, joincol, "left")

col_name = "_" + "item_id" + "_" + "state_id" + "_"
icol = Array($"item_id", $"state_id")
joincol = Seq("item_id", "state_id")
temp_df = grid_sdf.groupBy(icol: _*)
  .agg(functions.mean("sales").alias("enc"+col_name+"mean"),
       functions.stddev("sales").alias("enc"+col_name+"std"))
grid_sdf = grid_sdf.join(temp_df, joincol, "left")

col_name = "_" + "item_id" + "_" + "store_id" + "_"
icol = Array($"item_id", $"store_id")
joincol = Seq("item_id", "store_id")
temp_df = grid_sdf.groupBy(icol: _*)
  .agg(functions.mean("sales").alias("enc"+col_name+"mean"),
       functions.stddev("sales").alias("enc"+col_name+"std"))
grid_sdf = grid_sdf.join(temp_df, joincol, "left")

for(col <- base_cols){
  if(col != "d" & col != "id")
    grid_sdf = grid_sdf.drop(col)
}

// Save
grid_sdf.write.format("parquet").options(header="true", inferschema="true").save(raw_data_dir+"/processing/mean_encoding_df")