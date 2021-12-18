#! /usr/bin/env python3.7

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier, GBTClassifier, MultilayerPerceptronClassifier, LinearSVC
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Initialize
spark = SparkSession.builder.appName("Ltz's Homework").master('local').getOrCreate()

# Load data with schema
schema_sdf = StructType([
        StructField('Usage', DoubleType(), True),
        StructField('Limit', DoubleType(), True),
        StructField('MortgageStatus', IntegerType(), True),
        StructField('HistoryStatus', IntegerType(), True),
        StructField('CountStatus', IntegerType(), True),
        StructField('OverdueStatus', IntegerType(), True),
        StructField('Gender_male', IntegerType(), True)
    ])
credit = spark.read.options(header='true').schema(schema_sdf).csv("file:///home/student/student/liutuozhen/spark/credit_data.csv")

# Glance
credit.show(3)

credit.describe(['OverdueStatus']).show()

assembler = VectorAssembler(
        inputCols=["Usage", "Limit", "MortgageStatus", "HistoryStatus", "CountStatus", "Gender_male"],
        outputCol="features"
)

# Transform data structure
data = assembler.transform(credit).select("OverdueStatus", "features").withColumnRenamed("OverdueStatus","label")
print(data)

# Split the data into training and test sets (20% held out for testing)
(trainingData, testData) = data.randomSplit([0.8, 0.2])

# Define MulticlassClassificationEvaluator & predict function
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
def predict(name, model, testData):
    predictions = model.transform(testData)
    accuracy = evaluator.evaluate(predictions)
    print(f"{name} | Test Accuracy = {round(accuracy, 6)}")

# Train & Test
lr = LogisticRegression(maxIter=100, regParam=0.01)
model = lr.fit(trainingData)
predict("LogisticRegression", model, testData)

dt = DecisionTreeClassifier(maxDepth=3)
model = dt.fit(trainingData)
predict("DecisionTreeClassifier", model, testData)

rf = RandomForestClassifier(numTrees=10, maxDepth=5)
model = rf.fit(trainingData)
predict("RandomForestClassifier", model, testData)

gbt = GBTClassifier(maxIter=10)
model = gbt.fit(trainingData)
predict("GBTClassifier", model, testData)

lsvc = LinearSVC(maxIter=10, regParam=0.01)
model = lsvc.fit(trainingData)
predict("LinearSVC", model, testData)

