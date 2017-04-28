package uk.gov.ons.bi.dataload.reader

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import uk.gov.ons.bi.dataload.utils.ContextMgr

/**
  * Created by websc on 08/02/2017.1
  */
class LinkJsonReader (ctxMgr: ContextMgr)
  extends BIDataReader {

  val sqlContext = ctxMgr.sqlContext

  def readFromSourceFile(srcFilePath: String): DataFrame = {

    println(s"Reading Links from JSON file $srcFilePath")
    sqlContext.read.json(srcFilePath)
  }

}
