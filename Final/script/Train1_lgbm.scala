import spark.implicits._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{ColumnName, DataFrame, Encoders, Row, SparkSession, functions}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.ml.{PipelineStage, Pipeline}
import org.apache.spark.ml.feature.{VectorAssembler, OneHotEncoder, StringIndexer}
import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMRegressionModel, LightGBMRegressor}

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

// Load data
val train_df = spark.read.parquet(raw_data_dir+"/processing/train_df")

// Train
val model: LightGBMRegressor = new LightGBMRegressor()
  .setNumIterations(1500)
  .setNumLeaves(2^8-1)
  .setBoostFromAverage(false)
  .setFeatureFraction(0.5)
  .setMaxBin(100)
  .setObjective("tweedie")
  .setLearningRate(0.03)
  .setBaggingFraction(0.5)
  .setBaggingFreq(1)
  .setBaggingSeed(1234)
  .setBoostingType("gbdt")
  .setMetric("rmse")
  .setVerbosity(1)

val fitted_model = model.fit(train_df)

// Save
fitted_model.saveNativeModel(filename=raw_data_dir+"/processing/lgbm", overwrite=true)