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

    // Only interested in a subset of columns
    df.select(
      $"CompanyNumber".as("co_number"),
      $"CompanyName".as("co_name"),
      $"CompanyStatus".as("co_status"),
      $"SICCodeSicText_1".as("co_sic_code1"),
      $"RegAddressPostCode".as("co_postcode")
    )

  }

  def loadVatFromParquet(appConfig: AppConfig) = {

    // Get data directories
    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetData = parquetDataConfig.vat

    val dataFile = s"$parquetPath/$parquetData"

    val df = sqlContext.read.parquet(dataFile)
    // Only interested in a subset of columns
    df.select(
      $"entref".as("vat_entref"),
      $"vatref",
      //$"name".as("vat_name"),
      $"inqcode".as("vat_inqcode"),
      $"sic92".as("vat_sic92"),
      $"legalstatus".as("vat_legalstatus"),
      $"turnover".as("vat_turnover")
    )

  }

  def loadPayeFromParquet(appConfig: AppConfig) = {

    // Get data directories
    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetData = parquetDataConfig.paye

    val dataFile = s"$parquetPath/$parquetData"

    val df = sqlContext.read.parquet(dataFile)

    df.printSchema

    // Only interested in a subset of columns
    df.select(
      $"entref".as("paye_entref"),
      $"payeref",
      $"nameline1".as("paye_name"),
      $"inqcode".as("paye_inqcode"),
      $"legalstatus".as("paye_legalstatus"),
      $"employer_cat".as("paye_employer_cat"),
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
    // Nested data structure - not clear what rules are for handling this yet.
    // For now, we will just take FIRST item (if any) from each of the arrays in Link record
    links.select(
      $"CH".getItem(0).as("CH"),
      $"UBRN",
      $"VAT".getItem(0).as("VAT"),
      $"PAYE".getItem(0).as("PAYE")

    )
  }

  def joinLinksToCompanies(links:DataFrame, ch: DataFrame): DataFrame = {
    links.registerTempTable("ln")
    ch.registerTempTable("ch")
    val sql = "SELECT ln.*, ch.* FROM ln LEFT OUTER JOIN ch ON (ln.CH = ch.co_number)"
    sqlContext.sql(sql)
  }

  def joinLinksChToVat(links:DataFrame, vat: DataFrame): DataFrame = {
    links.registerTempTable("ln")
    vat.registerTempTable("vat")
    val sql = "SELECT ln.*, vat.* FROM ln LEFT OUTER JOIN vat ON (ln.VAT = vat.vatref)"
    sqlContext.sql(sql)
  }


  def joinLinksChVatToPaye(links:DataFrame, paye: DataFrame): DataFrame = {
    links.registerTempTable("ln")
    paye.registerTempTable("paye")
    val sql = "SELECT ln.*, paye.* FROM ln LEFT OUTER JOIN paye ON (ln.VAT = paye.payeref)"
    sqlContext.sql(sql)
  }

  def buildJoinedData(appConfig: AppConfig) = {

    val links = loadLinksFromParquet(appConfig)
    links.printSchema

    val ch = loadCompaniesFromParquet(appConfig)

    val vat = loadVatFromParquet(appConfig)

    val paye = loadPayeFromParquet(appConfig)

    val linksCh = joinLinksToCompanies(links, ch)

    val linksChVat = joinLinksChToVat(linksCh, vat)

    val linksChVatPaye = joinLinksChVatToPaye(linksChVat, paye)

    linksChVatPaye.printSchema

    // Get data directories
    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val biOutput = "INTERIM_BI_DATA.parquet"

    val biFile = s"$parquetPath/$biOutput"

    linksChVatPaye.write.mode("overwrite").parquet(biFile)

  }

}
