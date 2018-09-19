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

    // External data (HMRC - CH, VAT, PAYE, TCN)

    val extDir = appConfig.External.externalPath
    val workingDir = appConfig.BusinessIndex.workPath

    //Get directories and file names etc for specified data source

    val (extDataDir, extSrcFile, parquetFile) = biSource match {
      case CH  => (appConfig.External.chDir, appConfig.External.ch, appConfig.BusinessIndex.ch)
      case VAT   => (appConfig.External.vatDir, appConfig.External.vat, appConfig.BusinessIndex.vat)
      case PAYE => (appConfig.External.payeDir, appConfig.External.paye, appConfig.BusinessIndex.paye)
      case TCN  => (appConfig.External.lookupsDir, appConfig.External.tcnToSic, appConfig.BusinessIndex.tcn)
    }

    val inputPath = s"$extDir/$extDataDir/$extSrcFile"
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
