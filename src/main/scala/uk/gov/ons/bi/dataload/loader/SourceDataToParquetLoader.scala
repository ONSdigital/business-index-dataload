package uk.gov.ons.bi.dataload.loader

import com.google.inject.Singleton
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.reader._
import uk.gov.ons.bi.dataload.utils.{AppConfig, ContextMgr}

/**
  * Created by websc on 14/02/2017.
  */

@Singleton
class SourceDataToParquetLoader(ctxMgr: ContextMgr) {

  val log = ctxMgr.log

  def loadBusinessDataToParquet(biSource: BusinessDataSource, appConfig: AppConfig) = {

    // Get source/target directories

    // External data (HMRC)
    val extDataConfig = appConfig.ExtDataConfig
    val extBaseDir = extDataConfig.dir
    val extEnv = extDataConfig.env

    // Application working directory
    val appDataConfig = appConfig.AppDataConfig
    val workingDir = appDataConfig.workingDir

    // Get directories and file names etc for specified data source
    val (extSrcFile, extDataDir, parquetFile, tempTable) = biSource match {
      case VAT => (extDataConfig.vat, extDataConfig.vatDir, appDataConfig.vat, "temp_vat")
      case CH => (extDataConfig.ch, extDataConfig.chDir, appDataConfig.ch, "temp_ch")
      case PAYE => (extDataConfig.paye, extDataConfig.payeDir, appDataConfig.paye, "temp_paye")
    }

    val extSrcFilePath = s"/$extEnv/$extBaseDir/$extDataDir/$extSrcFile"
    log.info(s"Reading $biSource data from: $extSrcFilePath")

    // Get corresponding reader based on BIDataSource
    val reader = new CsvReader(ctxMgr, tempTable)

    // Process the data
    val data = reader.readFromAdminSourceFile(extSrcFilePath, biSource)
    val targetFilePath = s"$workingDir/$parquetFile"
    log.info(s"Writing $biSource data to: $targetFilePath")

    reader.writeParquet(data, targetFilePath)
  }


  def loadTcnToSicCsvLookupToParquet(appConfig: AppConfig) = {

    // Get source/target directories

    // Lookups source directory
    val lookupsEnv = appConfig.AppDataConfig.env
    val lookupsConfig = appConfig.OnsDataConfig.lookupsConfig
    val lookupsDir = lookupsConfig.dir
    val tcnToSicFile = lookupsConfig.tcnToSic

    // Application working directory
    val appDataConfig = appConfig.AppDataConfig
    val workingDir = appDataConfig.workingDir

    val parquetFile = appDataConfig.tcn

    val extSrcFilePath = s"/$lookupsEnv/$lookupsDir/$tcnToSicFile"

    log.info(s"Reading TCN-SIC lookup from: $extSrcFilePath")

    // Get CSV reader for this data source
    val reader: BIDataReader = new CsvReader(ctxMgr, "temp_tcn")

    // Process the data
    val data = reader.readFromSourceFile(extSrcFilePath)

    val targetFilePath = s"$workingDir/$parquetFile"

    log.info(s"Writing TCN-SIC lookup to: $targetFilePath")

    reader.writeParquet(data, targetFilePath)
  }

  def loadSourceBusinessDataToParquet(appConfig: AppConfig) = {

    loadBusinessDataToParquet(CH, appConfig)

    loadBusinessDataToParquet(VAT, appConfig)

    loadBusinessDataToParquet(PAYE, appConfig)

    loadTcnToSicCsvLookupToParquet(appConfig)
  }

}
