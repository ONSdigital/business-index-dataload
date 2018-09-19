package uk.gov.ons.bi.dataload

import org.apache.spark.sql.SparkSession

import uk.gov.ons.bi.dataload.exports.HmrcBiCsvExtractor
import uk.gov.ons.bi.dataload.utils.ContextMgr

object HmrcBiExportApp extends Serializable with DataloadApp {

  val sparkSess = cluster match {
    case "local" => SparkSession.builder.master("local").appName("Business Index").getOrCreate()
    case "cluster" => SparkSession.builder.appName("ONS BI Dataload: Apply UBRN rules to Link data").enableHiveSupport.getOrCreate
  }
  val ctxMgr = new ContextMgr(sparkSess)

  // Run the HMRC extraction
  HmrcBiCsvExtractor.extractBiToCsv(ctxMgr, appConfig)

}
