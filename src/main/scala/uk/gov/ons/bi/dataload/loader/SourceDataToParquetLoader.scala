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

    // External data (HMRC)
    val extDataConfig = appConfig.ExtDataConfig
    val extBaseDir = extDataConfig.dir
    val extEnv = extDataConfig.env

    // Application working directory
    val appDataConfig = appConfig.AppDataConfig
    //val workingDir = appDataConfig.workingDir

    val workingDir = getWorkingDir(appConfig)

     //Get directories and file names etc for specified data source
        val (extSrcFile, extDataDir, parquetFile) = biSource match {
          case VAT  => (extDataConfig.vat, extDataConfig.vatDir, appDataConfig.vat)
          case CH   => (extDataConfig.ch, extDataConfig.chDir, appDataConfig.ch)
          case PAYE => (extDataConfig.paye, extDataConfig.payeDir, appDataConfig.paye)
        }

    val inputPath = s"$extEnv/$extBaseDir/$extDataDir/$extSrcFile"
    val outputPath = s"$workingDir/$parquetFile"

    (inputPath, outputPath)
  }

  def getTcnDataPath(appConfig: AppConfig): (String, String) = {

    // Lookups source directory
    val lookupsEnv = appConfig.AppDataConfig.env
    val lookupsConfig = appConfig.OnsDataConfig.lookupsConfig
    val lookupsDir = lookupsConfig.dir
    val tcnToSicFile = lookupsConfig.tcnToSic

    // Application working directory
    val appDataConfig = appConfig.AppDataConfig
    val workingDir = getWorkingDir(appConfig)

    val parquetFile = appDataConfig.tcn

    val inputPath = s"/$lookupsEnv/$lookupsDir/$tcnToSicFile"
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
