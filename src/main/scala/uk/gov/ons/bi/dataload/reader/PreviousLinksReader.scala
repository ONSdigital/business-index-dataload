package uk.gov.ons.bi.dataload.reader

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

/**
  * Created by websc on 08/03/2017.
  */
class PreviousLinksReader (sc: SparkContext)
  extends BIDataReader (sc: SparkContext){

  def readFromSourceFile(srcFilePath: String): DataFrame = {
    sqlContext.read.parquet(srcFilePath)
  }
}
