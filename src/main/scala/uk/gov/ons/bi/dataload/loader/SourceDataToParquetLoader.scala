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

     //Get directories and file names etc for specified data source
        val (extSrcFile, extDataDir, parquetFile, tempTable) = biSource match {
          case VAT  => (extDataConfig.vat, extDataConfig.vatDir, appDataConfig.vat, "temp_vat")
          case CH   => (extDataConfig.ch, extDataConfig.chDir, appDataConfig.ch, "temp_ch")
          case PAYE => (extDataConfig.paye, extDataConfig.payeDir, appDataConfig.paye, "temp_paye")
        }

    val inputPath = s"$extEnv/$extBaseDir/$extDataDir/$extSrcFile"
    val outputPath = s"$workingDir/$parquetFile"
    writeAdminParquet(inputPath, outputPath, tempTable, biSource)
  }

  def writeAdminParquet(inputPath: String, outputPath: String, tempTable: String, biSource: BusinessDataSource) = {
    log.info(s"Reading $biSource data from: $inputPath")

    // Get corresponding reader based on BIDataSource
    val reader = new CsvReader(ctxMgr, tempTable)

    // Process the data
    val data = reader.readFromAdminSourceFile(inputPath, biSource)
    log.info(s"Writing $biSource data to: $outputPath")

    reader.writeParquet(data, outputPath)
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
    val targetFilePath = s"$workingDir/$parquetFile"

    writeAdminParquet(extSrcFilePath, targetFilePath, "temp_tcn", TCN)
  }

  def loadSourceBusinessDataToParquet(appConfig: AppConfig) = {

    loadBusinessDataToParquet(CH, appConfig)

    loadBusinessDataToParquet(VAT, appConfig)

    loadBusinessDataToParquet(PAYE, appConfig)

    loadTcnToSicCsvLookupToParquet(appConfig)
  }

}
