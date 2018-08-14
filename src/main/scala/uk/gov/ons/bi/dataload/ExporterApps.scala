package uk.gov.ons.bi.dataload

/**
  * Created by websc on 29/06/2017.
  */

import org.apache.spark.sql.SparkSession
import uk.gov.ons.bi.dataload.exports.HmrcBiCsvExtractor
import uk.gov.ons.bi.dataload.utils.ContextMgr

/**
  * Created by websc on 29/06/2017.
  */
//object HmrcBiExportApp extends Serializable with DataloadApp {
//
//  // val sparkConf = new SparkConf().setAppName("Export Business Index to CSV for HMRC")
//  val sparkSess = SparkSession.builder.appName("Export Business Index to CSV for HMRC").enableHiveSupport.getOrCreate
//  // ContextMgr provides our app-specific Spark context stuff
//  val ctxMgr = new ContextMgr(sparkSess)
//
//  // Run the HMRC extraction
//  HmrcBiCsvExtractor.extractBiToCsv(ctxMgr, appConfig)
//
//}
