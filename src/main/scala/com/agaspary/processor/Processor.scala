package com.agaspary.processor

import java.util.Properties

import com.agaspary.helper.Helper
import com.agaspary.sink.HdfsSink.writeToParquet
import com.agaspary.source.KafkaSource
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

object Processor {
  def start(spark: SparkSession, streamingContext: StreamingContext, properties: Properties): Unit = {
    val kafkaSourceStream = KafkaSource.getKafkaSourceStream( streamingContext, properties )
    kafkaSourceStream.map( _._2 ).foreachRDD( rdd => {
      if (!rdd.isEmpty) {
        import spark.implicits._
        val sourceDF = rdd.toDF( "value" )
        val df = KafkaSource.castToJsonDF( spark, sourceDF )
        val sinkDF = Helper.addNewDateColBaseOnTimestampCol( df, "date", "timestamp" )
        sinkDF.show()
        writeToParquet( sinkDF, properties.getProperty( "outputPath" ), "date", "parquet" )
      }
    } )
  }
}
