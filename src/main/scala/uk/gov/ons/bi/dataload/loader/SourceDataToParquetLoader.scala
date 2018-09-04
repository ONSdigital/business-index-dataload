package uk.gov.ons.bi.dataload.loader

import com.google.inject.Singleton

import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.reader._
import uk.gov.ons.bi.dataload.writer.BiParquetWriter
import uk.gov.ons.bi.dataload.utils.{AppConfig, ContextMgr}

@Singleton
class SourceDataToParquetLoader(ctxMgr: ContextMgr) extends BIDataReader {

  val log = ctxMgr.log

  def getAdminDataPaths(biSource: BusinessDataSource, appConfig: AppConfig): (String, String) = {

    // Get source/target directories

    // External data (HMRC - CH, VAT, PAYE)
    val extDataConfig = appConfig.ExtDataConfig
    val extDir = getExtDir(appConfig)

    // Lookups source directory (TCN)
    val lookupsConfig = appConfig.OnsDataConfig.lookupsConfig

    // output directory
    val workingDir = getAppDataConfig(appConfig, "working")

     //Get directories and file names etc for specified data source
    val (baseDir, extSrcFile, extDataDir, parquetFile) = biSource match {
      case VAT  => (extDir, extDataConfig.vat, extDataConfig.vatDir, getAppDataConfig(appConfig, "vat"))
      case CH   => (extDir, extDataConfig.ch, extDataConfig.chDir, getAppDataConfig(appConfig, "ch"))
      case PAYE => (extDir, extDataConfig.paye, extDataConfig.payeDir, getAppDataConfig(appConfig, "paye"))
      case TCN  => (getAppDataConfig(appConfig, "env"), lookupsConfig.tcnToSic,lookupsConfig.dir, getAppDataConfig(appConfig, "tcn"))
    }

    val inputPath = s"$baseDir/$extDataDir/$extSrcFile"
    val outputPath = s"$workingDir/$parquetFile"

    (inputPath, outputPath)
  }

  def writeAdminToParquet(inputPath: String, outputPath: String, tempTable: String, biSource: BusinessDataSource) = {
    log.info(s"Reading $biSource data from: $inputPath")

    // Get corresponding reader based on BIDataSource
    val reader = new CsvReader(ctxMgr, tempTable)

    // Process the data
    val data = reader.readFromAdminSourceFile(inputPath, biSource)
    log.info(s"Writing $biSource data to: $outputPath")

    BiParquetWriter.writeParquet(data, outputPath)
  }
}
