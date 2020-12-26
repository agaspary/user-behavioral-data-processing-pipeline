#!/usr/bin/env bash

bootstrapServers="*****"
sourceTopic="*****"
outputPath="*****"
groupId="*****"

spark-submit \
  --class com.agaspary.driver.Driver \
  --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:2.4.5 \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.hadoop.fs.defaultFS=*****  \
  --conf spark.hadoop.yarn.resourcemanager.address=*****  \
  --conf spark.yarn.jars=***** \
  --conf spark.sql.shuffle.partitions=20  \
  --name user-behavioral-data-processing-pipeline ../target/kafka-spark-hdfs-pipeline-jar-with-dependencies.jar  \
  "$bootstrapServers" "$sourceTopic" "$outputPath" "$groupId"

