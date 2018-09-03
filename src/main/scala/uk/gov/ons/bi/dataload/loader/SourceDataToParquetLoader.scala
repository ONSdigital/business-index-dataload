package uk.gov.ons.bi.dataload.loader

import com.google.inject.Singleton

import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.reader._
import uk.gov.ons.bi.dataload.utils.{AppConfig, ContextMgr}

/**
  * Created by websc on 14/02/2017.
  */

@Singleton
class SourceDataToParquetLoader(ctxMgr: ContextMgr) extends BIDataReader {

  val log = ctxMgr.log

  def getAdminDataPaths(biSource: BusinessDataSource, appConfig: AppConfig): (String, String) = {

    // Get source/target directories

    // External data (HMRC - CH, VAT, PAYE)
    val extDataConfig = appConfig.ExtDataConfig
    val extEnv = extDataConfig.env
    val extBaseDir = extDataConfig.dir

    // Application working directory
    val appDataConfig = appConfig.AppDataConfig

    // Lookups source directory (TCN)
    val lookupsConfig = appConfig.OnsDataConfig.lookupsConfig

    // output directory
    val workingDir = getWorkingDir(appConfig)

     //Get directories and file names etc for specified data source
    val (baseDir, extSrcFile, extDataDir, parquetFile) = biSource match {
      case VAT  => (s"$extEnv/$extBaseDir", extDataConfig.vat, extDataConfig.vatDir, appDataConfig.vat)
      case CH   => (s"$extEnv/$extBaseDir", extDataConfig.ch, extDataConfig.chDir, appDataConfig.ch)
      case PAYE => (s"$extEnv/$extBaseDir", extDataConfig.paye, extDataConfig.payeDir, appDataConfig.paye)
      case TCN  => (appDataConfig.env, lookupsConfig.tcnToSic,lookupsConfig.dir, appDataConfig.tcn)
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

    reader.writeParquet(data, outputPath)
  }
}
