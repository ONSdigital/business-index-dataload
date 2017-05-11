package uk.gov.ons.bi.dataload.reader

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by websc on 10/02/2017.
  */

abstract class BIDataReader(sc: SparkContext) {

  val sqlContext =  SQLContext.getOrCreate(sc)

  def readFromSourceFile(srcFilePath: String): DataFrame

  def writeParquet(df: DataFrame, targetFilePath: String):Unit = {
    df.write.mode("overwrite").parquet(targetFilePath)
  }
}

