package uk.gov.ons.bi.dataload.linker

import com.google.inject.Singleton
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import uk.gov.ons.bi.dataload.model.{CH, LINKS, PAYE, VAT}
import uk.gov.ons.bi.dataload.utils.AppConfig

/**
  * Created by websc on 14/02/2017.
  */
@Singleton
class LinkJoiner (implicit val sc: SparkContext){

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  import sqlContext.implicits._

  def doStuff = println("I DON'T DO ANYTHING YET!")

  def loadCompaniesFromParquet(appConfig: AppConfig) = {

    // Get data directories
    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetData = parquetDataConfig.ch

    val dataFile = s"$parquetPath/$parquetData"

    val df = sqlContext.read.parquet(dataFile)

    df.select($"CompanyNumber",$"CompanyName")

  }

  def loadLinksFromParquet(appConfig: AppConfig) = {

    // Get data directories
    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetData = parquetDataConfig.links

    val linksFile = s"$parquetPath/$parquetData"

    val links = sqlContext.read.parquet(linksFile)

    // Need to modify it so Company Number is NOT an array - just take first one (assume at least one)
    links.select($"CH".getItem(0).as("CompanyNumber"), $"UBRN", $"VAT", $"PAYE")
  }

  def buildJoinedData(appConfig: AppConfig) = {

    val links = loadLinksFromParquet(appConfig)
    links.cache()
    links.printSchema
    println(s"Links data contains ${links.count} records.")

    val ch = loadCompaniesFromParquet(appConfig)

    val joined = links.join(ch,Seq("CompanyNumber"),"outer").select($"CompanyNumber", $"CompanyName",$"PAYE", $"VAT",$"UBRN")
    joined.show(10)

    println(s"Joined data (left outer join) contains ${joined.count} records.")


  }

}
