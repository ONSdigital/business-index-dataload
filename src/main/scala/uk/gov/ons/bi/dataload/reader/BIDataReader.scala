package uk.gov.ons.bi.dataload.reader

import org.apache.spark.sql.DataFrame

/**
  * Created by websc on 10/02/2017.
  */
trait BIDataReader {

  def readFromSourceFile(srcFilePath: String): DataFrame

  def writeParquet(df: DataFrame, targetFilePath: String):Unit = {
    df.write.mode("overwrite").parquet(targetFilePath)
  }
}
