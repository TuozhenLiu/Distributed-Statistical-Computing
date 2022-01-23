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
var grid_sdf = spark.read.parquet(raw_data_dir+"/processing/grid_df")

var prices_df = spark.read.options(Map("header"->"true", "inferSchema"->"true")).csv(raw_data_dir+"sell_prices.csv")

prices_df = prices_df.filter($"store_id" === target_Store)

val calendar_df = spark.read.options(Map("header"->"true", "inferSchema"->"true")).csv(raw_data_dir+"calendar.csv")

val release_df = prices_df.groupBy("store_id", "item_id")
  .agg(functions.min("wm_yr_wk")
  .alias("release"))

// Now we can merge release_df
grid_sdf = grid_sdf.join(release_df, Seq("store_id", "item_id"), "left")

// We want to remove some "zeros" rows from grid_df to do it we need wm_yr_wk column let's merge partly calendar_df to have it
grid_sdf = grid_sdf.join(calendar_df.select("wm_yr_wk", "d"), Seq("d"), "left")

// Now we can cutoff some rows
grid_sdf = grid_sdf.filter($"wm_yr_wk" >= $"release")

// Save part 1
grid_sdf.write.format("parquet").options(header="true", inferschema="true").save(raw_data_dir+"/processing/grid_part_1")