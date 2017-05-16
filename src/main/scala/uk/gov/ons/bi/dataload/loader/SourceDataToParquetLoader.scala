package uk.gov.ons.bi.dataload.loader

import com.google.inject.Singleton
import org.apache.spark.{SparkConf, SparkContext}
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.reader._
import uk.gov.ons.bi.dataload.utils.AppConfig

/**
  * Created by websc on 14/02/2017.
  */

@Singleton
class SourceDataToParquetLoader(val sc: SparkContext) {

  def loadBusinessDataToParquet(biSource: BusinessDataSource, appConfig: AppConfig) = {

    // Get source/target directories

    // External data (HMRC)
    val extDataConfig = appConfig.ExtDataConfig
    val extBaseDir = extDataConfig.dir

    // Application working directory
    val appDataConfig = appConfig.AppDataConfig
    val workingDir = appDataConfig.workingDir

    // Get directories and file names etc for specified data source
    val (extSrcFile, extDataDir, parquetFile, tempTable) = biSource match {
      case VAT => (extDataConfig.vat, extDataConfig.vatDir, appDataConfig.vat, "temp_vat")
      case CH => (extDataConfig.ch, extDataConfig.chDir, appDataConfig.ch, "temp_ch")
      case PAYE => (extDataConfig.paye, extDataConfig.payeDir, appDataConfig.paye, "temp_paye")
    }

    val extSrcFilePath = s"$extBaseDir/$extDataDir/$extSrcFile"

    // Get corresponding reader based on BIDataSource
    val reader: BIDataReader = new CsvReader(sc, tempTable)

    // Process the data
    println(s"Reading from: $extSrcFilePath")
    val data = reader.readFromSourceFile(extSrcFilePath)
    val targetFilePath = s"$workingDir/$parquetFile"

    println(s"Writing to: $targetFilePath")
    reader.writeParquet(data, targetFilePath)
  }


  def loadTcnToCsvLookupToParquet(appConfig: AppConfig) = {

    // Get source/target directories

    // Lookups source directory
    val lookupsConfig = appConfig.OnsDataConfig.lookupsConfig
    val lookupsDir = lookupsConfig.dir
    val tcnToSicFile = lookupsConfig.tcnToSic

    // Application working directory
    val appDataConfig = appConfig.AppDataConfig
    val workingDir = appDataConfig.workingDir
    val parquetFile = appDataConfig.tcn

    val extSrcFilePath = s"$lookupsDir/$tcnToSicFile"

    // Get CSV reader for this data source
    val reader: BIDataReader = new CsvReader(sc, "temp_tcn")

    // Process the data
    println(s"Reading from: $extSrcFilePath")
    val data = reader.readFromSourceFile(extSrcFilePath)

    data.printSchema()

    val targetFilePath = s"$workingDir/$parquetFile"

    println(s"Writing to: $targetFilePath")
    reader.writeParquet(data, targetFilePath)
  }

  def loadSourceBusinessDataToParquet(appConfig: AppConfig) = {

    loadBusinessDataToParquet(CH, appConfig)

    loadBusinessDataToParquet(VAT, appConfig)

    loadBusinessDataToParquet(PAYE, appConfig)

    loadTcnToCsvLookupToParquet(appConfig)
  }

}
