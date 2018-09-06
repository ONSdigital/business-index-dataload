package uk.gov.ons.bi.dataload

import uk.gov.ons.bi.dataload.exports.HmrcBiCsvExtractor

object HmrcBiExportApp extends Serializable with DataloadApp {

  // Run the HMRC extraction
  HmrcBiCsvExtractor.extractBiToCsv(ctxMgr, appConfig)

}
