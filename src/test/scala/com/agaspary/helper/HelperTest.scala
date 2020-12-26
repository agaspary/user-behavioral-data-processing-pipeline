package com.agaspary.helper

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Assert.{assertEquals, assertThat}
import org.junit.Test

class HelperTest {

  lazy val spark: SparkSession = SparkSession.builder().appName( "Test" ).master( "local" ).getOrCreate()
  val testDf: DataFrame = spark.read.json( "src/main/resources/data/test.json" )

  @Test
  def TestAddNewDateColBaseOnTimestampCol(): Unit = {
    val dFWithNewCol = Helper.addNewDateColBaseOnTimestampCol( testDf, "date", "timestamp" )
    assertEquals( dFWithNewCol.select( "date" ).collectAsList().get( 0 ).getString( 0 ), "2021-01-06" )
  }

  @Test
  def TestPropertyFromArgs(): Unit = {
    val properties = Helper.propertyFromArgs( Array( "localhost:9092", "test", "outputPath", "groupId" ) )
    assertEquals( properties.getProperty( "sourceTopics" ), "test" )
  }

}
