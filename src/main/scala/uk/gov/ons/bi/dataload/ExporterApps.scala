package uk.gov.ons.bi.dataload

/**
  * Created by websc on 29/06/2017.
  */

import uk.gov.ons.bi.dataload.exports.HmrcBiCsvExtractor

/**
  * Created by websc on 29/06/2017.
  */
object HmrcBiExportApp extends Serializable with DataloadApp {

  // Run the HMRC extraction
  HmrcBiCsvExtractor.extractBiToCsv(ctxMgr, appConfig)

}
