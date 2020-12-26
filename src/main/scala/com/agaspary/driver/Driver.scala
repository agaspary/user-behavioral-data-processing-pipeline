package com.agaspary.driver

import com.agaspary.helper.Helper
import com.agaspary.processor.Processor
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Driver {
  def main(args: Array[String]): Unit = {

    val properties = Helper.propertyFromArgs( args )
    val spark: SparkSession = SparkSession.builder.getOrCreate
    val streamingContext = new StreamingContext( spark.sparkContext, Seconds( 2 ) )

    Processor.start( spark, streamingContext, properties )

    streamingContext.checkpoint( properties.getProperty( "outputPath" ) + "__checkpoint" )
    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
