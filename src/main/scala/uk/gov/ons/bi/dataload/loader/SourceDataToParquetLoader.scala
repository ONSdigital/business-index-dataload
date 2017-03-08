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
      val sourceDataConfig = appConfig.SourceDataConfig
      val srcPath = sourceDataConfig.dir

      val parquetDataConfig = appConfig.ParquetDataConfig
      val parquetPath = parquetDataConfig.dir

      // Get directories and file names for specified data source
      val (srcFile, dataDir, parquetFile) = biSource match {
        case VAT => (sourceDataConfig.vat, sourceDataConfig.vatDir, parquetDataConfig.vat)
        case CH => (sourceDataConfig.ch, sourceDataConfig.chDir, parquetDataConfig.ch)
        case PAYE => (sourceDataConfig.paye, sourceDataConfig.payeDir, parquetDataConfig.paye)
      }

      val srcFilePath = s"$srcPath/$dataDir/$srcFile"

      // Get corresponding reader based on BIDataSource
      val reader: BIDataReader = biSource match {
        case VAT => new VatCsvReader(sc)
        case CH => new CompaniesHouseCsvReader(sc)
        case PAYE => new PayeCsvReader(sc)
      }

      // Process the data
      println(s"Reading from: $srcFilePath")
      val data = reader.readFromSourceFile(srcFilePath)
      val targetFilePath = s"$parquetPath/$parquetFile"

      println(s"Writing to: $targetFilePath")
      reader.writeParquet(data, targetFilePath)
    }

    def loadSourceBusinessDataToParquet(appConfig: AppConfig) = {

      loadBusinessDataToParquet(CH, appConfig)

      loadBusinessDataToParquet(VAT, appConfig)

      loadBusinessDataToParquet(PAYE, appConfig)
    }

  }
