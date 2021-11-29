#! /bin/bash

# Current code directory
PWD=$(cd $(dirname $0); pwd)
cd $PWD 1> /dev/null 2>&1
echo "LOCAL PWD: $PWD"

# Task config
TASKNAME=printcolumn
ROOTPATH=/lifeng/student/liutuozhen/streaming
HADOOP_HOME=/usr/lib/hadoop-current/
HADOOP_VERSION=2.7.2

# Hadoop input and output
HADOOP_INPUT_DIR=$ROOTPATH/$TASKNAME/table.txt
HADOOP_OUTPUT_DIR=$ROOTPATH/$TASKNAME/output
echo "HADOOP INPUT: $HADOOP_INPUT_DIR"
echo "HADOOP OUTPUT: $HADOOP_OUTPUT_DIR"

# Check output file
hadoop fs -test -e ${HADOOP_OUTPUT_DIR}
if [ $? -eq 0 ]; then
	hadoop fs -rm -r ${HADOOP_OUTPUT_DIR}
fi

# Start job
#    -D num.key.fields.for.partition=100 \
hadoop jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-${HADOOP_VERSION}.jar \
    -D mapreduce.job.name=$TASKNAME \
    -D mapreduce.job.priority=NORMAL \
	-D mapreduce.job.maps=5 \
	-D mapreduce.job.reduces=0 \
	-D stream.memory.limit=1000 \
    -files mapper.sh \
    -output ${HADOOP_OUTPUT_DIR} \
    -input ${HADOOP_INPUT_DIR} \
    -mapper mapper.sh \
    #-reducer None

# print output
echo "output ls:"
hadoop fs -ls ${HADOOP_OUTPUT_DIR}/*
echo "output cat:"
hadoop fs -cat ${HADOOP_OUTPUT_DIR}/*

exit 0
