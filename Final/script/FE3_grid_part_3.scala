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
val calendar_df = spark.read.options(Map("header"->"true", "inferSchema"->"true")).csv(raw_data_dir+"calendar.csv")
// var grid_sdf = spark.read.options(Map("header"->"true", "inferSchema"->"true")).csv(raw_data_dir+"/processing/grid_part_1")
var grid_sdf = spark.read.parquet(raw_data_dir+"/processing/grid_part_1")

// Merge calendar
grid_sdf = grid_sdf.select("id", "d")
val icols = Array($"date",
         $"d",
         $"event_name_1",
         $"event_type_1",
         $"event_name_2",
         $"event_type_2",
         $"snap_CA",
         $"snap_TX",
         $"snap_WI")
grid_sdf = grid_sdf.join(calendar_df.select(icols: _*), Seq("d"), "left")

// Make Dates Features
grid_sdf = grid_sdf.withColumn("tm_d", functions.dayofmonth(grid_sdf("date")))
  .withColumn("tm_w", functions.weekofyear(grid_sdf("date")))
  .withColumn("tm_m", functions.month(grid_sdf("date")))
  .withColumn("tm_y", functions.year(grid_sdf("date")) - 2010)
  .withColumn("tm_dw", functions.dayofweek(grid_sdf("date")))
  .withColumn("tm_w_end", functions.when($"tm_dw" >= 5, 1).otherwise(0))

// Save
grid_sdf.write.format("parquet").options(header="true", inferschema="true").save(raw_data_dir+"/processing/grid_part_3")