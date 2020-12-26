# user-behavioral-data-processing-pipeline

- The main purpose of this pipeline is to collect user behavior data for the BI team for further analytics.

- The data will be consumed from Kafka by Spark Streaming and will be stored in the HDFS as Parquet files.

- The data will be stored in HDFS on a daily basis.

#### Kafka message format!

```json
{
   "os":"mac",
   "src":"amazon.com",
   "browser":"chrome",
   "clicks":{
      "clickCount":2,
      "clickDetails":[
         {
            "x":"345",
            "y":"783",
            "timestamp":1608964744561
         },
         {
            "x":"215",
            "y":"976",
            "timestamp":1608964744563
         }
      ]
   },
   "id":"05h0300",
   "userId":90700,
   "timestamp":1609064744560
}
```

#### Configuration!

Before submitting the job you have to configure *start.sh* script.
Please define following configurations.

> "bootstrapServers"
> "sourceTopic"
> "outputPath"
> "groupId"
> "spark.hadoop.fs.defaultFS"
> "spark.hadoop.yarn.resourcemanager.address"
> "spark.yarn.jars"

##### Java Version!

- 1.8.*

#### To compile use:
```sh
$ mvn package
```

#### For subbmiting the job:
```sh
$ ./start.sh
```
