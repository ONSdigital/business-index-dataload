package uk.gov.ons.bi.dataload.reader

import org.apache.spark.sql.DataFrame
import uk.gov.ons.bi.dataload.utils.ContextMgr

/**
  * Created by websc on 08/02/2017.1
  */
class LinkJsonReader (ctxMgr: ContextMgr)
  extends BIDataReader {

  val sqlContext = ctxMgr.sqlContext

  def readFromSourceFile(srcFilePath: String): DataFrame = {
    sqlContext.read.json(srcFilePath)
  }

}
