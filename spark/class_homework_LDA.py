#! /usr/bin/env python3.7

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.clustering import LDA
import pyspark.sql.functions as fn
from pyspark.ml.stat import Correlation

# Initialize
spark = SparkSession.builder.appName("Ltz's Homework").master('local').getOrCreate()

# Load data with schema
from pyspark.sql.types import *
schema_sdf = StructType([
        StructField('date', StringType(), True),
        StructField('rating', IntegerType(), True),
        StructField('review', StringType(), True)
    ])
review = spark.read.options(header='true').schema(schema_sdf).csv("file:///home/student/student/liutuozhen/spark/avengers_review.csv")
review = review.dropna()

# Glance
review.show(3)

# Transform data structure
tokenizer = Tokenizer(inputCol="review", outputCol="words")
review_words = tokenizer.transform(review)
review_words.show(3)

# Calculate TF-IDF
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=100)
featurizedData = hashingTF.transform(review_words)

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

data = rescaledData.select("rating", "features")
data.show()

# Model LDA
lda = LDA(k=2, maxIter=10)
model = lda.fit(data)

# Describe topics.
topics = model.describeTopics(3)
print("The topics described by their top-weighted terms:")
topics.show(truncate=False)

# Test Correlationship between topic and label
transformed = model.transform(data)
transformed.select("rating", "topicDistribution").show(truncate=False)
vectorToColumn = fn.udf(lambda vec: vec[0].item(), DoubleType())
transformed_probs = transformed.withColumn("topic_prob", vectorToColumn(transformed["topicDistribution"])).select("rating", "topic_prob")
transformed_probs.show(3)

assembler = VectorAssembler(
    inputCols=["rating", "topic_prob"],
    outputCol="features")

df_for_cor = assembler.transform(transformed_probs)
r1 = Correlation.corr(df_for_cor, "features").head()
print("Pearson correlation matrix:\n" + str(r1[0]))