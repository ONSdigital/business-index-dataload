package uk.gov.ons.bi.dataload

/**
  * Created by websc on 29/06/2017.
  */

import org.apache.spark.SparkConf
import uk.gov.ons.bi.dataload.exports.HmrcBiCsvExtractor
import uk.gov.ons.bi.dataload.utils.ContextMgr

/**
  * Created by websc on 29/06/2017.
  */
object HmrcBiExportApp extends Serializable with DataloadApp {

  val sparkConf = new SparkConf().setAppName("Export Business Index to CSV for HMRC")
  // ContextMgr provides our app-specific Spark context stuff
  val ctxMgr = new ContextMgr(sparkConf)

  // Run the HMRC extraction
  HmrcBiCsvExtractor.extractBiToCsv(ctxMgr, appConfig)

}
