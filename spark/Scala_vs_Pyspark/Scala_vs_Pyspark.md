# Scala vs Pyspark (Distributed Statistical Computing Homework)

 ***刘拓臻  应用统计专硕 21  2021210789***

## Contents (目录)

[TOC]

请用不少于20个示例对比PySpark代码与Scala代码处理Spark的异同。

## 一、Spark程序启动及初始化

### 1. Starting Interactive Spark Shell

Scala通过在命令行使用`spark-shell`命令，启用与当前Spark版本相对应的Scala交互命令行程序。

Pyspark在pip安装后，可直接在命令行使用`pyspark`命令，启用pyspark交互命令行程序。

启动交互命令行程序后，SparkSession被自动创建：`SparkSession available as 'spark'`。

- Scala

  ```shell
  spark-shell
  ```

- Pyspark

  ```shell
  pyspark
  ```

### 2. Initializing Spark

执行Spark程序必须做的第一件事是创建SparkContext、SparkSession对象，它告诉Spark如何访问集群。

SparkSession是Spark2.x后引入的概念。在2.x之前，对于不同的功能，需要使用不同的Context：

- 创建和操作RDD时，使用SparkContext
- 使用Streaming时，使用StreamingContext
- 使用SQL时，使用sqlContext
- 使用Hive时，使用HiveContext

在2.x中，为了统一上述的Context，引入SparkSession，实质上是SQLContext、HiveContext、SparkContext的组合。

- [ ] 若是通过 Interactive Spark Shell 启动spark程序，SparkSession被自动创建。

- [ ] 若是通过Scala脚本、Python脚本、ipython kernel jupyter启动spark程序，要手动创建SparkSession。

对于直接创建SparkContext的任务，首先需要构建一个包含应用程序信息的SparkConf对象。若是在jupyter上使用ipython kernel运行pyspark，还需要先借助findspark模块进行初始化，连接到spark程序。

- Scala

  ```scala
  import org.apache.spark.{SparkContext, SparkConf}
  val conf = new SparkConf().setAppName("appName").setMaster("local")  // or "yarn"
  new SparkContext(conf)
  ```

- Pyspark

  ```python
  # for jupyter only
  import findspark
  findspark.init()
  ```

  ```python
  from pyspark import SparkContext, SparkConf
  conf = SparkConf().setAppName("appName").setMaster("local")  # or "yarn"
  sc = SparkContext(conf=conf)
  ```

对于需要创建SparkSession的任务，需要先手动创建SparkSession（Interactive Spark Shell已自动创建），然后再创建SparkContext。

- Scala

  ```scala
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession
    .builder()
    .appName("appName")
    .master("local")  // or "yarn"
    .getOrCreate()
  val sc = spark.sparkContext
  ```

- Pyspark

  ```python
  from pyspark.sql import SparkSession
  spark = SparkSession \
      .builder \
      .appName("appName") \
      .master("local") \  # or "yarn"
      .getOrCreate()
  sc = spark.sparkContext
  ```

## 二、基础RDD操作

弹性分布式数据集（RDD）是Spark中可并行操作的容错元素集合，是Spark的核心数据抽象方式。

### 3. Creating RDDs

Scala和Pyspark中RDD的创建方式大致相同，有两种方法：

1. 通过应用程序中的集合来创建。
2. 引用外部存储系统中的数据集，如共享文件系统、HDFS、HBase或提供Hadoop InputFormat的任何数据源。

- Scala

  ```scala
  // Parallelized Collections
  val data = Array(1, 2, 3, 4, 5)
  val distData = sc.parallelize(data)
  // ParallelCollectionRDD[33] at parallelize at <console>:35
  ```

  ```scala
  // External Datasets
  val distFile = sc.textFile("data.txt")
  // org.apache.spark.rdd.RDD[String] = data.txt MapPartitionsRDD[10] at textFile at <console>:26
  ```

- Pyspark

  ```python
  # Parallelized Collections
  data = [1, 2, 3, 4, 5]
  distData = sc.parallelize(data)
  # ParallelCollectionRDD[1] at parallelize at PythonRDD.scala:195
  ```

  ```python
  # External Datasets 
  distFile = sc.textFile("data.txt")
  # data.txt MapPartitionsRDD[7] at textFile at NativeMethodAccessorImpl.java:0
  ```

### 4. Basic RDD Operations

RDD支持两种类型的操作：`transformation`（从现有数据集创建新数据集）和 ` action`（在数据集上运行计算后向驱动程序返回值）。
Spark中的所有transformation都是lazy（惰性）的，因为它们不会立即计算结果。相反，他们只记得应用于某些基本数据集（例如文件）的转换。仅当action需要将结果返回到驱动程序时，才会计算转换。这种设计使Spark能够更高效地运行。

Scala和Python中的基础RDD操作语法大致相同，以函数型编程的形式进行操作，对于简单的transformation / action函数，可以使用匿名函数编程，Scala中的语法为`x => f(x)`，Python中的语法为`lambda x: f(x)`。

- Scala

  ```scala
  val lines = sc.textFile("data.txt")
  val lineLengths = lines.map(s => s.length)
  val totalLength = lineLengths.reduce((a, b) => a + b)
  ```

- Pyspark

  ```python
  lines = sc.textFile("data.txt")
  lineLengths = lines.map(lambda s: len(s))
  totalLength = lineLengths.reduce(lambda a, b: a + b)
  ```

### 5. Persisting an RDD in memory

由于Spark中的所有transformation都是lazy（惰性）的，默认情况下，每次对每个转换的RDD运行操作时，都会重新计算它。但是，也可以使用persist（或cache）方法在内存中持久化RDD，在这种情况下，Spark将保留集群中的元素，以便下次查询时更快地访问它。还支持在磁盘上持久化RDD，或跨多个节点复制RDD。

- Scala

  ```scala
  // If we also wanted to use lineLengths again later, we could add:
  lineLengths.persist()
  ```

- Pyspark

  ```python
  # If we also wanted to use lineLengths again later, we could add:
  lineLengths.persist()
  ```

### 6. Passing Functions to Spark

由于匿名函数只能用于编写简单函数（不支持多语句或不返回值的函数），为了向Spark传递更复杂的函数，可以通过以下自定义函数的方式，实现统计每行不为“北京”的词语个数。Scala和Pyspark的实现方法大致相同。

- Scala

  ```scala
  def myFunc(s: String): Int = {
      val s_array = s.split(",")
      var len = 0
      for(x <- s_array){
          if( x != "北京" ) len += 1
      }
      return len
  }
  // myFunc: (s: String)Int
  textFile.map(myFunc).collect()
  ```

- Pyspark

  ```python
  def myFunc(s):
      words = s.split(",")
      return len([w for w in words if w != "北京"])
  
  textFile.map(myFunc).collect()
  ```

### 7. Special Example: Filter lines

Filter 是个transformation的操作，在执行当前行的时候并不会直接运行（lazy），在遇到.count()等action才会执行。

在Scala的filter函数中，通过匿名函数`line => line.contains("Spark")`筛选包含Spark的行。

在Pyspark的filter函数中，通过对textFile的value列进行操作`textFile.value.contains("Spark")`，

返回`Column<b'contains(value, Spark)'>`对象，来筛选包含Spark的行。

- Scala

  ```scala
  val linesWithSpark = textFile.filter(line => line.contains("Spark"))
  // linesWithSpark: org.apache.spark.sql.Dataset[String] = [value: string]
  ```

- Pyspark

  ```python
  linesWithSpark = textFile.filter(textFile.value.contains("Spark"))
  # textFile.value.contains("Spark"): Column<b'contains(value, Spark)'>
  # linesWithSpark: DataFrame[value: string]
  ```

### 8. Broadcast Variables

通常，当传递给Spark操作（如map或reduce）的函数在远程集群节点上执行时，它在函数中使用的所有变量的单独副本上工作。这些变量被复制到每台机器上，远程机器上的变量更新不会传播回驱动程序。支持跨任务的通用、读写共享变量将是低效的，Spark为此提供了两种类型的共享变量：`Broadcast Variables`和`Accumulators`。

`Broadcast Variables`允许在每台机器上缓存一个只读变量，而不是将其副本与任务一起发送，可以使用它们以高效的方式为每个节点提供一个大型输入数据集的副本。Spark通过使用高效的广播算法来分配，以降低通信成本。

- Scala

  ```scala
  val broadcastVar = sc.broadcast(Array(1, 2, 3))
  // broadcastVar: org.apache.spark.broadcast.Broadcast[Array[Int]] = Broadcast(0)
  broadcastVar.value
  // res0: Array[Int] = Array(1, 2, 3)
  ```

- Pyspark

  ```python
  broadcastVar = sc.broadcast([1, 2, 3])
  # <pyspark.broadcast.Broadcast object at 0x102789f10>
  broadcastVar.value
  # [1, 2, 3]
  ```

### 9. Accumulators

`Accumulators`是Spark提供的累加器，可以有效地并行支持，可用于实现计数器（如MapReduce）或求和。Spark本机支持数字类型的累加器，可以自定义对新类型的支持。

- Scala

  ```scala
  val accum = sc.longAccumulator("My Accumulator")
  // accum: org.apache.spark.util.LongAccumulator = LongAccumulator(id: 0, name: Some(My Accumulator), value: 0)
  sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x))
  accum.value
  // res2: Long = 10
  ```

- Pyspark

  ```python
  accum = sc.accumulator(0)
  accum
  # Accumulator<id=0, value=0>
  sc.parallelize([1, 2, 3, 4]).foreach(lambda x: accum.add(x))
  accum.value
  # 10
  ```

## 三、DataFrame（SQL）操作

Spark SQL是用于结构化数据处理的Spark模块。与基本的Spark RDD API不同，Spark SQL提供的接口为Spark提供了有关数据结构和正在执行的计算的更多信息。在内部，Spark SQL使用这些额外的信息来执行额外的优化。有几种与Spark SQL交互的方法，包括SQL和Dataset API。计算结果时，将使用相同的执行引擎，这与用于表示计算的API/语言无关。这种统一意味着开发人员可以轻松地在不同的API之间来回切换，基于API的切换提供了表示给定转换的最自然的方式。

### 10. Dataset vs DataFrame

Spark的主要抽象是一个数据的分布式集合，称为Dataset。Dataset是Spark 1.6中添加的一个新接口，它提供了RDD（强类型，能够使用强大的lambda函数）的优点和Spark SQL优化的执行引擎的优点，可以从Hadoop输入格式（如HDFS文件）或通过转换其他数据集来创建Dataset。Dataset API有Scala和Java两种版本。Python不支持Dataset API。但由于Python的动态特性，Dataset API的许多优点已经可用，我们不需要在Python中使用强类型的Dataset。

DataFrame是一个由命名列组成的Dataset。从概念上讲，它相当于关系数据库中的表或R/Python中的DataFrame，但具有更丰富的优化功能。DataFrame可以从广泛的源构建，例如：结构化数据文件、配置单元中的表、外部数据库或现有RDD。DataFrameAPI在Scala、Java、Python和R中可用。在Scala API中，DataFrame只是Dataset[Row]的类型别名。我们可以通过以下方式读取本地文件创建一个新的DataFrame：

- Scala

  ```scala
  val textFile = spark.read.textFile("README.md")
  // textFile: org.apache.spark.sql.Dataset[String] = [value: string]
  ```

- Pyspark

  ```python
  textFile = spark.read.text("README.md")
  # textFile: DataFrame[value: string]
  ```

### 11. DataFrame Operations

DataFrame为Scala、Java、Python和R中的结构化数据操作提供了一种特定于领域的语言。如上所述，在Spark 2.0中，DataFrame只是Scala和Java API中的Dataset[Row]，这些操作也称为Untyped Dataset Operations。

Scala和Pyspark的用法大致相同，Scala中通过`$"col_name"`来选择目标列，而Pyspark中采用类似pandas的语法形式，通过`df["col_name"]`来选择目标列。

- Scala

  ```scala
  // This import is needed to use the $-notation
  import spark.implicits._
  df.select($"name", $"age" + 1).show()
  df.filter($"age" > 21).show()
  df.groupBy("age").count().show()
  ```

- Pyspark

  ```python
  df.select(df['name'], df['age'] + 1).show()
  df.filter(df['age'] > 21).show()
  df.groupBy("age").count().show()
  ```

### 12. SQL Queries

SparkSession上的sql函数使应用程序能够以编程方式运行sql查询，并将查询结果DataFrame作为返回。在查询前，首先要将DataFrame通过`createOrReplaceTempView`当作一个SQL临时视图。Scala和Pyspark的用法相同。

- Scala

  ```scala
  // Register the DataFrame as a SQL temporary view
  df.createOrReplaceTempView("people")
  
  val sqlDF = spark.sql("SELECT * FROM people")
  sqlDF.show()
  ```

- Pyspark

  ```python
  # Register the DataFrame as a SQL temporary view
  df.createOrReplaceTempView("people")
  
  sqlDF = spark.sql("SELECT * FROM people")
  sqlDF.show()
  ```

Spark SQL中的临时视图的作用域仅是当前会话，如果创建它的会话终止，它将消失。如果希望在所有会话之间共享临时视图，并在Spark应用程序终止之前保持活动状态，则可以创建全局临时视图。

- Scala

  ```scala
  // Register the DataFrame as a global temporary view
  df.createGlobalTempView("people")
  
  // Global temporary view is tied to a system preserved database `global_temp`
  spark.sql("SELECT * FROM global_temp.people").show()
  
  // Global temporary view is cross-session
  spark.newSession().sql("SELECT * FROM global_temp.people").show()
  ```

- Pyspark

  ```python
  # Register the DataFrame as a global temporary view
  df.createGlobalTempView("people")
  
  # Global temporary view is tied to a system preserved database `global_temp`
  spark.sql("SELECT * FROM global_temp.people").show()
  
  # Global temporary view is cross-session
  spark.newSession().sql("SELECT * FROM global_temp.people").show()
  ```

### 13. Creating Datasets

Spark中Datasets的创建只能通过Scala或Java来进行。

- Scala

  ```scala
  case class Person(name: String, age: Long)
  
  // Encoders are created for case classes
  val caseClassDS = Seq(Person("Andy", 32)).toDS()
  caseClassDS.show()
  // +----+---+
  // |name|age|
  // +----+---+
  // |Andy| 32|
  // +----+---+
  
  // Encoders for most common types are automatically provided by importing spark.implicits._
  val primitiveDS = Seq(1, 2, 3).toDS()
  primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)
  
  // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
  val path = "examples/src/main/resources/people.json"
  val peopleDS = spark.read.json(path).as[Person]
  peopleDS.show()
  // +----+-------+
  // | age|   name|
  // +----+-------+
  // |null|Michael|
  // |  30|   Andy|
  // |  19| Justin|
  // +----+-------+
  ```

### 14. Interoperating with RDDs by Inferring the Schema

Spark SQL支持两种不同的方法将现有RDD转换为DataFrame。第一种方法使用reflection来推断包含特定类型对象的RDDschema。当编写Spark应用程序时已经知道schema时，这种基于reflection的方法会产生更简洁的代码，并且效果很好。

- Scala

  ```scala
  // For implicit conversions from RDDs to DataFrames
  import spark.implicits._
  
  // Create an RDD from a text file, convert it to a Dataframe
  val spark_df = sc
    .textFile("/lifeng/student/liutuozhen/streaming/aggregate/table.csv")
    .map(_.split(","))
    .map(attributes => (attributes(0), attributes(2).trim.toInt))
    .toDF()
  ```

- Pyspark

  ```python
  from pyspark.sql import Row
  
  # Load a text file and convert each line to a Row.
  lines = sc.textFile("/lifeng/student/liutuozhen/streaming/aggregate/table.csv")
  parts = lines.map(lambda l: l.split(","))
  cols = parts.map(lambda p: Row(area=p[0], num=int(p[2])))
  
  # Infer the schema, and register the DataFrame as a table.
  spark_df = spark.createDataFrame(cols)
  spark_df.createOrReplaceTempView("tempDF")
  
  # SQL can be run over DataFrames that have been registered as a table.
  spark.sql("SELECT * FROM tempDF WHERE num>70")
  ```

### 15. Programmatically Specifying the Schema

将RDD转换为DataFrame的第二种方法是通过编程接口，该接口允许您构造schema，然后将其应用于现有RDD。虽然此方法更为详细，但它可以在运行时才知道列及其类型时构造DataFrame。

- Scala

  ```scala
  import org.apache.spark.sql.types._
  // The schema is encoded in a string
  val schemaString = "name age"
  
  // Generate the schema based on the string of schema
  val fields = schemaString.split(" ")
    .map(fieldName => StructField(fieldName, StringType, nullable = true))
  val schema = StructType(fields)
  
  // Convert records of the RDD (people) to Rows
  val rowRDD = peopleRDD
    .map(_.split(","))
    .map(attributes => Row(attributes(0), attributes(1).trim))
  
  // Apply the schema to the RDD
  val peopleDF = spark.createDataFrame(rowRDD, schema)
  ```

- Pyspark

  ```python
  # Import data types
  from pyspark.sql.types import *
  # The schema is encoded in a string.
  
  # Create a schema
  schemaString = ["area", "gender", "num"]
  StringType = [StringType(), StringType(), IntegerType()]
  fields = [StructField(field_name, string_type, True) \
            for field_name, string_type in zip(schemaString, StringType)] # True: 是否允许空值
  schema = StructType(fields)
  
  spark_df = spark.createDataFrame(cols, schema)
  ```

### 16. DataFrame to Json / Pandas

对于Spark DataFrame格式的数据，我们还可以将其转换为Json格式，并通过collect()提取到应用程序。对于Pyspark，可以进一步转换成Pandas DataFrame。

- Scala

  ```scala
  val df_json = spark_df.toJSON()
  df_json.collect()
  // Array({"_1":"北京","_2":7}, {"_1":"江苏","_2":8}, ...
  ```

- Pyspark

  ```python
  df_json = spark_df.toJSON()
  df_json.collect()
  # ['{"area":"江苏","num":77}',
  # ...
  # '{"area":"浙江","num":74}']
  ```

  ```python
  spark_df.toPandas()
  # pandas.core.frame.DataFrame
  ```

### 17. Pandas User-defined functions

对于Pyspark，在我们将Spark DataFrame转换成Pandas DataFrame后，可以很方便的利用Pandas来自定义函数进行分组、聚合等操作。

- Pyspark

  ```python
  air2_pdf = air.select(["DayOfWeek", "ArrDelay","AirTime","Distance"]).toPandas()
  
  import pandas as pd
  
  def myfun(pdf):
      out = dict()
      out["ArrDelay"] = pdf.ArrDelay.mean()
      out["AirTime"]  = pdf.AirTime.mean()
      out["Distance"] = pdf.Distance.mean()
  
      return pd.DataFrame(out, index=[0])
  
  myfun(air2_pdf)
  ```

  ```python
  from pyspark.sql.functions import pandas_udf, PandasUDFType
  
  @pandas_udf("DayOfWeek long, ArrDelay long, AirTime long, Distance long", PandasUDFType.GROUPED_MAP)  
  def myfun(pdf):
      out = dict() 
      out["ArrDelay"] = pdf.ArrDelay.mean()
      out["AirTime"]  = pdf.AirTime.mean()
      out["Distance"] = pdf.Distance.mean()
      
      return pd.DataFrame(out, index=[0])
  
  air3 = air2.na.drop()
  air3.groupby("DayOfWeek").apply(myfun).show()
  ```

## 四、MLlib的使用

MLlib是Spark的机器学习（ML）库。它的目标是使实用的机器学习具有可扩展性和易用性。目前，基于DataFrame的API是主要的API，从Spark 2.0开始，Spark.mllib包中基于RDD的API已进入维护模式。Spark的主要机器学习API现在是Spark.ml包中基于DataFrame的API。

### 18. Basic statistics

使用`ml.stat`库可利用Spark快速进行基础的统计分析，例如相关性分析（Correlation）、假设检验（Hypothesis testing）、描述性统计（Summarizer），Scala和Pyspark的用法基本相同，只需要将DataFrame以相应的格式传入统计函数，以卡方检验（ChiSquareTest）为例：

- Scala

  ```scala
  import org.apache.spark.ml.linalg.{Vector, Vectors}
  import org.apache.spark.ml.stat.ChiSquareTest
  
  val data = Seq(
    (0.0, Vectors.dense(0.5, 10.0)),
    (0.0, Vectors.dense(1.5, 20.0)),
    (1.0, Vectors.dense(1.5, 30.0)),
    (0.0, Vectors.dense(3.5, 30.0)),
    (0.0, Vectors.dense(3.5, 40.0)),
    (1.0, Vectors.dense(3.5, 40.0))
  )
  
  val df = data.toDF("label", "features")
  val chi = ChiSquareTest.test(df, "features", "label").head
  println(s"pValues = ${chi.getAs[Vector](0)}")
  println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
  println(s"statistics ${chi.getAs[Vector](2)}")
  ```

- Pyspark

  ```python
  from pyspark.ml.linalg import Vectors
  from pyspark.ml.stat import ChiSquareTest
  
  data = [(0.0, Vectors.dense(0.5, 10.0)),
          (0.0, Vectors.dense(1.5, 20.0)),
          (1.0, Vectors.dense(1.5, 30.0)),
          (0.0, Vectors.dense(3.5, 30.0)),
          (0.0, Vectors.dense(3.5, 40.0)),
          (1.0, Vectors.dense(3.5, 40.0))]
  df = spark.createDataFrame(data, ["label", "features"])
  
  r = ChiSquareTest.test(df, "features", "label").head()
  print("pValues: " + str(r.pValues))
  print("degreesOfFreedom: " + str(r.degreesOfFreedom))
  print("statistics: " + str(r.statistics))
  ```

### 19. Machine learning algorithms

在Spark中进行机器学习算法只需要导入并实例化相应的算法类，并通过`fit`函数拟合，类似于Python中的Sklearn库。在Scala和Pyspark中的用法类似，以线性回归模型（LinearRegression）为例：

- Scala

  ```scala
  import org.apache.spark.ml.regression.LinearRegression
  
  // Load training data
  val training = spark.read.format("libsvm")
    .load("data/mllib/sample_linear_regression_data.txt")
  
  val lr = new LinearRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)
  
  // Fit the model
  val lrModel = lr.fit(training)
  
  // Print the coefficients and intercept for linear regression
  println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
  
  // Summarize the model over the training set and print out some metrics
  val trainingSummary = lrModel.summary
  println(s"numIterations: ${trainingSummary.totalIterations}")
  println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
  trainingSummary.residuals.show()
  println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
  println(s"r2: ${trainingSummary.r2}")
  ```

- Pyspark

  ```python
  from pyspark.ml.regression import LinearRegression
  
  # Load training data
  training = spark.read.format("libsvm") \
      .load("data/mllib/sample_linear_regression_data.txt")
  
  lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
  
  # Fit the model
  lrModel = lr.fit(training)
  
  # Print the coefficients and intercept for linear regression
  print("Coefficients: %s" % str(lrModel.coefficients))
  print("Intercept: %s" % str(lrModel.intercept))
  
  # Summarize the model over the training set and print out some metrics
  trainingSummary = lrModel.summary
  print("numIterations: %d" % trainingSummary.totalIterations)
  print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
  trainingSummary.residuals.show()
  print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
  print("r2: %f" % trainingSummary.r2)
  ```

## 五、提交Spark应用程序

### 20. Self-Contained Applications

用Scala提交应用程序时，我们除了需要编写一个`SimpleApp.scala`Spark程序，还需要包括一个sbt配置文件`build.sbt`，此文件还添加Spark依赖的存储库。在创建一个包含应用程序代码的`jar`包后，使用spark提交脚本来运行我们的程序。

用Pyspark提交应用程序时，我们只需要编写一个`SimpleApp.py`Spark程序，然后通过`python SimpleApp.py`在终端运行。

- Scala

  ```scala
  /* SimpleApp.scala */
  import org.apache.spark.sql.SparkSession
  
  object SimpleApp {
    def main(args: Array[String]) {
      val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
      val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
      val logData = spark.read.textFile(logFile).cache()
      val numAs = logData.filter(line => line.contains("a")).count()
      val numBs = logData.filter(line => line.contains("b")).count()
      println(s"Lines with a: $numAs, Lines with b: $numBs")
      spark.stop()
    }
  }
  ```

  ```scala
  /* build.sbt */
  name := "Simple Project"
  version := "1.0"
  scalaVersion := "2.12.15"
  libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"
  ```

  ```shell
  # Package a jar containing your application
  sbt package
  # [info] Packaging {..}/{..}/target/scala-2.12/simple-project_2.12-1.0.jar
  ```

  ```shell
  # Use spark-submit to run your application
  YOUR_SPARK_HOME/bin/spark-submit \
    --class "SimpleApp" \
    --master local[4] \
    target/scala-2.12/simple-project_2.12-1.0.jar
  ```

- Pyspark

  ```python
  """SimpleApp.py"""
  from pyspark.sql import SparkSession
  
  logFile = "YOUR_SPARK_HOME/README.md"  # Should be some file on your system
  spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
  logData = spark.read.text(logFile).cache()
  
  numAs = logData.filter(logData.value.contains('a')).count()
  numBs = logData.filter(logData.value.contains('b')).count()
  
  print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
  
  spark.stop()
  ```

  ```shell
  # Use the Python interpreter to run your application
  python SimpleApp.py
  ```

  

