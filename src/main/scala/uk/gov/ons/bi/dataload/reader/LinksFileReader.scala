package uk.gov.ons.bi.dataload.reader

import org.apache.spark.sql.DataFrame
import uk.gov.ons.bi.dataload.utils.ContextMgr

class LinksFileReader (ctxMgr: ContextMgr) extends BIDataReader {

  val spark = ctxMgr.spark

  def readFromSourceFile(srcFilePath: String): DataFrame = {
    spark.read.parquet(srcFilePath)
  }

  def readFromLocal(): String = {
    val external = getClass.getClassLoader.getResource("external").toString
    val split = external.split("external")
    split(0)
  }
}
