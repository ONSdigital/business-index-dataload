package uk.gov.ons.bi.dataload.reader

import org.apache.spark.sql.DataFrame
import uk.gov.ons.bi.dataload.utils.ContextMgr

/**
  * Created by websc on 08/02/2017.1
  */
class LinksParquetReader (ctxMgr: ContextMgr)
  extends BIDataReader {

  val spark = ctxMgr.spark

  def readFromSourceFile(srcFilePath: String): DataFrame = {
    spark.read.parquet(srcFilePath)
  }

}
