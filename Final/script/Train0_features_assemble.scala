import spark.implicits._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{ColumnName, DataFrame, Encoders, Row, SparkSession, functions}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.ml.{PipelineStage, Pipeline}
import org.apache.spark.ml.feature.{VectorAssembler, OneHotEncoder, StringIndexer}

// Global Vars
val TARGET = "sales"         // Our main target
val END_TRAIN = 1913         // Last day in train set
val raw_data_dir = "dbfs:/FileStore/M5/data/"
val target_Store = "CA_1"
val FIRST_DAY = 710

// Start SparkSession
val spark = SparkSession
  .builder()
  .appName("Final")
  .master("local")
  .getOrCreate()

// Load Data
var grid_df = spark.read.parquet(raw_data_dir+"/processing/grid_part_1")
  .select("id", "d", "item_id", "dept_id", "cat_id", "sales", "release")

grid_sdf = grid_sdf.withColumn("d_int", functions.regexp_replace($"d", "d_", ""))
  .withColumn("d_int", $"d_int".cast(DataTypes.IntegerType))
  .filter($"d_int" > FIRST_DAY)
  .drop("d_int")

var temp_df = spark.read.parquet(raw_data_dir+"/processing/grid_part_2")
  .select("id", "d", "sell_price", "price_max", "price_min", "price_std",
          "price_mean", "price_norm", "price_nunique", "item_nunique",
          "price_momentum", "price_momentum_m", "price_momentum_y")
grid_sdf = grid_sdf.join(temp_df, Seq("id", "d"), "left")


temp_df = spark.read.parquet(raw_data_dir+"/processing/grid_part_3")
  .select("id", "d", "event_name_1", "event_type_1", "event_name_2",
          "event_type_2", "snap_CA", "snap_TX", "snap_WI", "tm_d", "tm_w", "tm_m",
          "tm_y", "tm_dw", "tm_w_end")
grid_sdf = grid_sdf.join(temp_df, Seq("id", "d"), "left")

temp_df = spark.read.parquet(raw_data_dir+"/processing/lags_df_28")
  .select("id", "d", "sales_lag_28", "sales_lag_29", "sales_lag_30",
          "sales_lag_31", "sales_lag_32", "sales_lag_33", "sales_lag_34",
          "sales_lag_35", "sales_lag_36", "sales_lag_37", "sales_lag_38",
          "sales_lag_39", "sales_lag_40", "sales_lag_41", "sales_lag_42",         
          "rolling_mean_7", "rolling_std_7", "rolling_mean_14", "rolling_std_14",
          "rolling_mean_30", "rolling_std_30", "rolling_mean_60",
          "rolling_std_60", "rolling_mean_180", "rolling_std_180")
grid_sdf = grid_sdf.join(temp_df, Seq("id", "d"), "left")

temp_df = spark.read.parquet(raw_data_dir+"/processing/mean_encoding_df")
  .select("id", "d", 
          "enc_store_id_dept_id_mean", "enc_store_id_dept_id_std", 
          "enc_item_id_state_id_mean", "enc_item_id_state_id_std")
grid_sdf = grid_sdf.join(temp_df, Seq("id", "d"), "left")

grid_sdf = grid_sdf.drop("id")

// Fill / Drop NA
grid_sdf = grid_sdf.na.fill(cols=Seq("event_name_1", "event_type_1", "event_name_2", "event_type_2"), value="NA")
grid_sdf = grid_sdf.na.drop()

// Features Assemble
val categoricalColumns = Array("item_id", "dept_id", "cat_id", "event_name_1", "event_type_1", "event_name_2", "event_type_2")
val stagesArray = new ListBuffer[PipelineStage]()

for (cate <- categoricalColumns) {
  val indexer = new StringIndexer().setInputCol(cate).setOutputCol(s"${cate}Index")
  val encoder = new OneHotEncoder().setInputCol(indexer.getOutputCol).setOutputCol(s"${cate}classVec")
  stagesArray.append(indexer,encoder)
}

val numericCols = grid_sdf.columns.filter(_ != "event_name_1").filter(_ != "event_type_1").filter(_ != "event_name_2").filter(_ != "event_type_2")
  .filter(_ != "item_id").filter(_ != "dept_id").filter(_ != "cat_id").filter(_ != "d").filter(_ != "sales")

val assemblerInputs = categoricalColumns.map(_ + "classVec") ++ numericCols
val assembler = new VectorAssembler().setInputCols(assemblerInputs).setOutputCol("features")
stagesArray.append(assembler)

val pipeline = new Pipeline()
pipeline.setStages(stagesArray.toArray)
val pipelineModel = pipeline.fit(grid_sdf)

// Transform grid_df
var train_df = pipelineModel.transform(grid_sdf).select($"d", $"sales".alias("label"), $"features")
// train_df.withColumn("d_int", functions.regexp_replace($"d", "d_", ""))
//   .withColumn("d_int", $"d_int".cast(DataTypes.IntegerType))
//   .filter($"d_int" > FIRST_DAY)
//   .drop("d_int")
var test_df = train_df.withColumn("d_int", functions.regexp_replace($"d", "d_", ""))
  .filter($"d_int" > 1885)
  .select("label", "features")
train_df = train_df.withColumn("d_int", functions.regexp_replace($"d", "d_", ""))
  .filter($"d_int" <= 1885)
  .select("label", "features")

// Save
train_df.write.format("parquet").options(header="true", inferschema="true").save(raw_data_dir+"/processing/train_df")
test_df.write.format("parquet").options(header="true", inferschema="true").save(raw_data_dir+"/processing/test_df")