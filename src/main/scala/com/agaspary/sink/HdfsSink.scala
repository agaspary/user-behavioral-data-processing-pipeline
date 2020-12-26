package com.agaspary.sink

import org.apache.spark.sql.{DataFrame, SaveMode}

object HdfsSink {
  def writeToParquet(df: DataFrame, parquetSource: String, partitionColName: String, namePrefix: String): Unit = {
    df.write.format("parquet").partitionBy( partitionColName ).mode( SaveMode.Append ).parquet( parquetSource + namePrefix )
  }
}
