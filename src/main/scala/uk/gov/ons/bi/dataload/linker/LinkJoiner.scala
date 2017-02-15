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
      $"SICCodeSicText_1",
      $"RegAddressPostCode")

  }

  def loadVatFromParquet(appConfig: AppConfig) = {

    // Get data directories
    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetData = parquetDataConfig.vat

    val dataFile = s"$parquetPath/$parquetData"

    val df = sqlContext.read.parquet(dataFile)

    df.printSchema

    // rename VAT ref to match Links record
    df.select(
      $"entref".as("vat_entref"),
      $"vatref".as("VAT"),
      //$"name".as("vat_name"),
      $"inqcode".as("vat_inqcode"),
      $"sic92",
      $"legalstatus".as("vat_legalstatus"),
      $"turnover")

  }

  def loadPayeFromParquet(appConfig: AppConfig) = {

    // Get data directories
    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetData = parquetDataConfig.paye

    val dataFile = s"$parquetPath/$parquetData"

    val df = sqlContext.read.parquet(dataFile)

    df.printSchema

    // rename ref to match Links record
    df.select(
      $"entref".as("paye_entref"),
      $"payeref".as("PAYE"),
      $"nameline1".as("paye_name"),
      $"inqcode".as("paye_inqcode"),
      $"legalstatus".as("paye_legalstatus"),
      $"employer_cat",
      $"postcode".as("paye_postcode")
    )

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

    val vat = loadVatFromParquet(appConfig)

    val paye = loadPayeFromParquet(appConfig)

    val linksCh = links.join(ch,Seq("CompanyNumber"),"outer")
      .select(
        $"CompanyNumber",
        $"CompanyName",
        $"CompanyStatus",
        $"SICCodeSicText_1".as("industryCode"),
        $"RegAddressPostCode".as("ch_postcode"),
        $"PAYE", $"VAT",$"UBRN")

    val linksChVat = linksCh.join(vat,Seq("VAT"),"outer")

    val linksChVatPaye = linksChVat.join(paye,Seq("PAYE"),"outer")

    linksChVatPaye.printSchema

    // Get data directories
    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val biOutput = "INTERIM_BI_DATA.parquet"

    val biFile = s"$parquetPath/$biOutput"

    linksChVatPaye.write.mode("overwrite").parquet(biFile)


  }

}
