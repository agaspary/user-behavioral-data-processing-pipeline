package com.agaspary.source

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, from_unixtime}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object KafkaSource {

  val struct: StructType = new StructType()
    .add( "id", DataTypes.StringType )
    .add( "userId", DataTypes.IntegerType )
    .add( "browser", DataTypes.StringType )
    .add( "os", DataTypes.StringType )
    .add( "timestamp", DataTypes.LongType )
    .add( "src", DataTypes.StringType )
    .add( "clicks", DataTypes.StringType )

  def getKafkaSourceStream(streamingContext: StreamingContext, properties: Properties): DStream[(String, String)] = {

    val sourceTopics = Array( properties.getProperty( "sourceTopics" ) )

    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String]( sourceTopics, getKafkaSourceParams( properties.getProperty( "bootstrapServers" ), properties.getProperty( "groupId" ) ) )
    )
    kafkaStream.map( record => (record.key, record.value) )
  }

  private def getKafkaSourceParams(bootstrapServers: String, groupId: String): Map[String, Object] = {
    val kafkaSourceParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: java.lang.Boolean)
    )
    kafkaSourceParams
  }

  def castToJsonDF(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    df.select( from_json( $"value", KafkaSource.struct ).as( "data" ) )
      .select( $"data.id".as( "id" ), $"data.userId".as( "userId" ), $"data.browser".as( "browser" ), $"data.os".as( "os" ), $"data.timestamp".as( "timestamp" ), $"data.src".as( "src" ), $"data.clicks".as( "clicks" ) )
  }

}
