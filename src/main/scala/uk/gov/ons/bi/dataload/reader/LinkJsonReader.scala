package uk.gov.ons.bi.dataload.reader

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

/**
  * Created by websc on 08/02/2017.
  */
class LinkJsonReader (implicit sc: SparkContext)
  extends BIDataReader {

  def readFromSourceFile(srcFilePath: String): DataFrame = {
    sqlContext.read.json(srcFilePath)
  }

}
