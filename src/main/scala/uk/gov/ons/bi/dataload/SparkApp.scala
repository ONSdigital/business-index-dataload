package uk.gov.ons.bi.dataload

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.reader._
import uk.gov.ons.bi.dataload.utils.AppConfig


/**
  * Created by websc on 02/02/2017.

  Dev local run command - need to figure out how to run on cluster:

  spark-submit --class uk.gov.ons.bi.dataload.SparkApp
   --packages com.databricks:spark-csv_2.10:1.5.0
   --master local[*]
   --driver-memory 2G --executor-memory 2G
   --driver-java-options "-Xms1g -Xmx4g"
   target/scala-2.10/business-index-dataload_2.10-1.0.jar
  */
  
object SparkApp {

  // Trying to use implicit voodoo to make SC available
  implicit val sc = new SparkContext(new SparkConf().setAppName("ONS BI Dataload"))

  def loadDataToParquet (biSource: BIDataSource, appConfig: AppConfig) = {

    // Get soruce/target directories
    val sourceDataConfig = appConfig.SourceDataConfig
    val srcPath = sourceDataConfig.dir

    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir

    // Get file names for specified data source
    val (srcFile, parquetFile) = biSource match {
      case VAT => (sourceDataConfig.vat, parquetDataConfig.vat)
      case CH => (sourceDataConfig.ch, parquetDataConfig.ch)
      case PAYE => (sourceDataConfig.paye, parquetDataConfig.paye)
      case LINKS => (sourceDataConfig.links, parquetDataConfig.links)
    }

    val srcFilePath = s"$srcPath/$srcFile"

    // Get corresponding reader based on BIDataSource
    val reader:BIDataReader = biSource match {
      case VAT => new VatCsvReader
      case CH => new CompaniesHouseCsvReader
      case PAYE => new PayeCsvReader
      case LINKS => new LinkJsonReader
    }

    // Process the data
    val chData = reader.readFromSourceFile(srcFilePath)

    reader.writeParquet(chData, parquetPath, parquetFile )
  }

  def loadSourceDataToParquet(appConfig: AppConfig) = {

    loadDataToParquet(CH, appConfig)

    loadDataToParquet(VAT, appConfig)

    loadDataToParquet(PAYE, appConfig)

    loadDataToParquet(LINKS, appConfig)
  }

  def buildLinkedData(appConfig: AppConfig) = {

    val builder = new LinkBuilder

    val data = builder.testQuery

    data.printSchema

    data.show(10)
  }

  def main(args: Array[String]) {

    val appConfig = new AppConfig

    loadSourceDataToParquet(appConfig)

    //buildLinkedData(appConfig)

  }
}
