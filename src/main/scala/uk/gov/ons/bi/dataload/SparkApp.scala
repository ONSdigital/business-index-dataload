package uk.gov.ons.bi.dataload

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import uk.gov.ons.bi.dataload.reader._
import uk.gov.ons.bi.dataload.utils.AppConfig


/**
  * Created by websc on 02/02/2017.

  Dev local run command - need to figure out how to run on cluster:

  spark-submit --class uk.gov.ons.bi.dataload.SparkApp --packages com.databricks:spark-csv_2.10:1.5.0 
  --master local[*] --driver-memory 2G --executor-memory 2G 
  --driver-java-options "-Xms1g -Xmx4g" target/scala-2.10/business-index-dataload_2.10-1.0.jar 
  */
  
object SparkApp {

  implicit val sc = new SparkContext(new SparkConf().setAppName("ONS BI Dataload"))
//  implicit val sqlContext = new SQLContext(sc)


  def loadVatToParquet (appConfig: AppConfig) = {

    val sourceDataConfig = appConfig.SourceDataConfig
    val srcPath = sourceDataConfig.dir
    val srcFile = sourceDataConfig.vat

    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetFile = parquetDataConfig.vat

    val reader = new VatCsvReader(srcPath, srcFile)(sc)

    val chData = reader.extractFromCsv

    reader.writeParquet(chData, parquetPath, parquetFile )

  }

  def loadPayeToParquet (appConfig: AppConfig) = {

    val sourceDataConfig = appConfig.SourceDataConfig
    val srcPath = sourceDataConfig.dir
    val srcFile = sourceDataConfig.paye

    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetFile = parquetDataConfig.paye
    val reader = new PayeCsvReader(srcPath, srcFile)

    val chData = reader.extractFromCsv

    reader.writeParquet(chData, parquetPath, parquetFile )

  }


  def loadCompaniesHouseToParquet (appConfig: AppConfig) = {

    val sourceDataConfig = appConfig.SourceDataConfig
    val srcPath = sourceDataConfig.dir
    val srcFile = sourceDataConfig.ch

    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetFile = parquetDataConfig.ch

    val chReader = new CompaniesHouseCsvReader(srcPath, srcFile)

    val chData = chReader.extractFromCsv

    chReader.writeParquet(chData, parquetPath, parquetFile )

  }

  def loadLinksToParquet(appConfig: AppConfig) = {

    val sourceDataConfig = appConfig.SourceDataConfig
    val srcPath = sourceDataConfig.dir
    val srcFile = sourceDataConfig.links

    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetFile = parquetDataConfig.links

    val linkReader = new LinkJsonReader(srcPath, srcFile)

    val linkData: DataFrame = linkReader.readFromJson

    println(s"Links file record count: ${linkData.count}")

    val path = s"${parquetPath}/${parquetFile}"
    linkData.write.mode("overwrite").parquet(path)
  }



  def buildLinkedData = {

    val builder = new LinkBuilder

    val data = builder.testQuery

    data.printSchema

    data.show(10)
  }

  def main(args: Array[String]) {

    val appConfig = new AppConfig


    // create Spark context with Spark configuration
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //loadCompaniesHouseToParquet(appConfig)

    //loadVatToParquet(appConfig)

    //loadPayeToParquet(appConfig)

    loadLinksToParquet(appConfig)

    //buildLinkedData

    // this is used to implicitly convert an RDD to a DataFrame.
    //import sqlContext.implicits._
  }
}
