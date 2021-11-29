#! /bin/bash

# Current code directory
PWD=$(cd $(dirname $0); pwd)
cd $PWD 1> /dev/null 2>&1
echo "LOCAL PWD: $PWD"

# Task config
TASKNAME=lr
ROOTPATH=/lifeng/student/liutuozhen/streaming
HADOOP_HOME=/usr/lib/hadoop-current/
HADOOP_VERSION=2.7.2

# Hadoop input and output
HADOOP_WORKSPACE=$ROOTPATH/$TASKNAME
INPUT_FILE=data_small.txt
HADOOP_INPUT_DIR=$HADOOP_WORKSPACE/$INPUT_FILE
HADOOP_OUTPUT_DIR=$HADOOP_WORKSPACE/beta_hat
echo "HADOOP WORKSPACE: $HADOOP_WORKSPACE"
echo "HADOOP INPUT: $HADOOP_INPUT_DIR"
echo "HADOOP OUTPUT: $HADOOP_OUTPUT_DIR"

# Check file
hadoop fs -test -e $HADOOP_WORKSPACE
if [ $? -eq 1 ]; then
	hadoop fs -mkdir $HADOOP_WORKSPACE
fi

hadoop fs -test -e $HADOOP_OUTPUT_DIR
if [ $? -eq 0 ]; then
	hadoop fs -rm -r $HADOOP_OUTPUT_DIR
fi

# Start job
	#-D num.key.fields.for.partition=2 \
	#-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
hadoop jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-${HADOOP_VERSION}.jar \
	-files mapper_beta_hat.py,reducer_beta_hat.py \
    -D mapreduce.job.name=$TASKNAME \
    -D mapreduce.job.priority=HIGH \
	-D mapreduce.job.maps=20 \
	-D mapreduce.job.reduces=1 \
	-D stream.memory.limit=10000 \
    -D stream.num.map.output.key.fields=1 \
	-output ${HADOOP_OUTPUT_DIR} \
    -input ${HADOOP_INPUT_DIR} \
    -mapper mapper_beta_hat.py \
    -reducer reducer_beta_hat.py

# print output
# echo "output ls:"
# hadoop fs -ls ${HADOOP_OUTPUT_DIR}/*
# echo "output cat:"
# hadoop fs -cat ${HADOOP_OUTPUT_DIR}/*

# Get output
hadoop fs -get $HADOOP_OUTPUT_DIR/* ./
echo "output ls:"
ls ./part-*
echo "output cat:"
cat ./part-*

exit 0
