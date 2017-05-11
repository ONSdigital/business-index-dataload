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
class SourceDataToParquetLoader (val sc: SparkContext){

    def loadBusinessDataToParquet(biSource: BusinessDataSource, appConfig: AppConfig) = {

      // Get source/target directories
      val extDataConfig = appConfig.ExtDataConfig
      val extBaseDir = extDataConfig.dir

      val appDataConfig = appConfig.AppDataConfig
      val workingDir = appDataConfig.workingDir

      // Get directories and file names for specified data source
      val (extSrcFile, extDataDir, parquetFile) = biSource match {
        case VAT => (extDataConfig.vat, extDataConfig.vatDir, appDataConfig.vat)
        case CH => (extDataConfig.ch, extDataConfig.chDir, appDataConfig.ch)
        case PAYE => (extDataConfig.paye, extDataConfig.payeDir, appDataConfig.paye)
      }

      val extSrcFilePath = s"$extBaseDir/$extDataDir/$extSrcFile"

      // Get corresponding reader based on BIDataSource
      val reader: BIDataReader = biSource match {
        case VAT => new VatCsvReader(sc)
        case CH => new CompaniesHouseCsvReader(sc)
        case PAYE => new PayeCsvReader(sc)
      }

      // Process the data
      println(s"Reading from: $extSrcFilePath")
      val data = reader.readFromSourceFile(extSrcFilePath)
      val targetFilePath = s"$workingDir/$parquetFile"

      println(s"Writing to: $targetFilePath")
      reader.writeParquet(data, targetFilePath)
    }

    def loadSourceBusinessDataToParquet(appConfig: AppConfig) = {

      loadBusinessDataToParquet(CH, appConfig)

      loadBusinessDataToParquet(VAT, appConfig)

      loadBusinessDataToParquet(PAYE, appConfig)
    }

  }
