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

  def loadCompaniesFromParquet(appConfig: AppConfig) = {

    // Get data directories
    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetData = parquetDataConfig.ch

    val dataFile = s"$parquetPath/$parquetData"

    val df = sqlContext.read.parquet(dataFile)

    df.select(
      $"CompanyNumber",
      $"CompanyName",
      $"CompanyStatus",
      $"SICCodeSicText_1")

  }

  def loadLinksFromParquet(appConfig: AppConfig) = {

    // Get data directories
    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetData = parquetDataConfig.links

    val linksFile = s"$parquetPath/$parquetData"

    val links = sqlContext.read.parquet(linksFile)

    // For now, we will just take FIRST item (if any) from each of the arrays in Link record
    links.select(
      $"CH".getItem(0).as("CompanyNumber"),
      $"UBRN",
      $"VAT".getItem(0).as("VAT"),
      $"PAYE".getItem(0).as("PAYE")

    )
  }

  def buildJoinedData(appConfig: AppConfig) = {

    val links = loadLinksFromParquet(appConfig)
    links.printSchema

    val ch = loadCompaniesFromParquet(appConfig)

    val joined = links.join(ch,Seq("CompanyNumber"),"outer")
      .select(
        $"CompanyNumber",
        $"CompanyName",
        $"CompanyStatus",
        $"SICCodeSicText_1".as("IndustryCode"),
        $"PAYE", $"VAT",$"UBRN")

    joined.show(20)

    println(s"Joined data (left outer join) contains ${joined.count} records.")

  }

}
