package uk.gov.ons.bi.dataload

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.reader._
import uk.gov.ons.bi.dataload.utils.AppConfig


/**
  * Created by websc on 02/02/2017.

  Dev local run command - need to figure out how to run on cluster:

  Memory options may need to be tweaked but not sure how.
  Jars are for spark-csv package, which cannot be loaded via SBT for some reason.
  This will be fixed in Spark 2.0, which includes spark-csv by default.

  spark-submit --class uk.gov.ons.bi.dataload.SparkApp
  --master local[*]
  --driver-memory 2G --executor-memory 2G
  --driver-java-options "-Xms1g -Xmx4g"
  --jars ./lib/spark-csv_2.10-1.5.0.jar,./lib/univocity-parsers-1.5.1.jar,./lib/commons-csv-1.1.jar
  target/scala-2.10/business-index-dataload_2.10-1.0.jar

  */
  
object SparkApp {

  // Trying to use implicit voodoo to make SC available
  implicit val sc = new SparkContext(new SparkConf().setAppName("ONS BI Dataload"))

  def loadDataToParquet (biSource: BIDataSource, appConfig: AppConfig) = {

    // Get source/target directories
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
    val data = reader.readFromSourceFile(srcFilePath)
    val targetFilePath = s"$parquetPath/$parquetFile"

    println(s"Writing to: $targetFilePath")
    reader.writeParquet(data, targetFilePath)
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

  def preProcessCH(appConfig: AppConfig) = {

    // Get source/target directories
    val sourceDataConfig = appConfig.SourceDataConfig
    val srcPath = sourceDataConfig.dir

    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir

    // Get corresponding reader based on BIDataSource
    val reader:BIDataReader = new CompaniesHouseCsvReader

    // Pre-process the data
    // There are 5 files to read and merge

    val nFiles = 5
    val chFiles = (1 to nFiles).map{n => s"BasicCompanyData-2017-02-03-part${n}_${nFiles}.csv"}

    val chRdds: Seq[DataFrame] = chFiles.map { srcFile =>
      val srcFilePath = s"$srcPath/$srcFile"
      reader.readFromSourceFile(srcFilePath)
    }

    val combined = (chRdds.reduceLeft(_ unionAll _)).repartition(1)

    // Output to parquet
    val parquetFile = "BasicCompanyData-2017-02-03.parquet"
    reader.writeParquet(combined, parquetPath)
  }

  def main(args: Array[String]) {

    val appConfig = new AppConfig

    // preProcessCH(appConfig)

    // loadSourceDataToParquet(appConfig)

    // buildLinkedData(appConfig)

  }
}
