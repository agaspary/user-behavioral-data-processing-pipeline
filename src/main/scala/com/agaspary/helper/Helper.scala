package com.agaspary.helper

import java.util.Properties

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_unixtime}

object Helper {
  def propertyFromArgs(args: Array[String]): Properties = {
    if (args.length != 4) {
      System.err.println( "Error: required parameter is missing." )
      System.exit( 1 )
    }
    val properties = new Properties()
    properties.put( "bootstrapServers", args( 0 ) )
    properties.put( "sourceTopics", args( 1 ) )
    properties.put( "outputPath", args( 2 ) )
    properties.put( "groupId", args( 3 ) )
    properties
  }

  def addNewDateColBaseOnTimestampCol(df: DataFrame, newDateColName: String, timestampColName: String): DataFrame = {
    df.withColumn( newDateColName, from_unixtime( col( timestampColName ) / 1000, "YYYY-MM-dd" ) )
  }
}
