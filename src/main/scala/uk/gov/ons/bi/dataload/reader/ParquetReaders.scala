package uk.gov.ons.bi.dataload.reader

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.utils.AppConfig

/**
  * Created by websc on 16/02/2017.
  */

class ParquetReader (implicit sc: SparkContext) {

  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  def loadCompanyRecsFromParquet(appConfig: AppConfig): RDD[CompanyRec] = {
    // Read Parquet data via SparkSQL but return as RDD so we can use RDD joins etc
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
    ).map { row =>  // NEED TO DEAL WITH null IN DATA!
      CompanyRec(row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4))
    }

  }

  def loadLinkRecsFromParquet(appConfig: AppConfig): RDD[LinkRec] = {
    // Read Parquet data via SparkSQL but return as RDD so we can use RDD joins etc
    // Get data directories
    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetData = parquetDataConfig.links

    val linksFile = s"$parquetPath/$parquetData"

    val links = sqlContext.read.parquet(linksFile)
    // Nested data structure - not clear what rules are for handling this yet.
    links.select(
      $"UBRN",
      $"CH",
      $"VAT",
      $"PAYE"
    ).map{row =>
      val ubrn = row.getString(0)
      // CH is currently provided as an array but we only want teh first entry (if any)
      val ch: Option[String] = if (row.isNullAt(0)) None else row.getSeq[String](0).headOption
      val vat: Option[Seq[String]] = if (row.isNullAt(2)) None else Some(row.getSeq[String](2))
      val paye: Option[Seq[String]] = if (row.isNullAt(3)) None else Some(row.getSeq[String](3))

      LinkRec(ubrn, ch, vat, paye )
    }

  }

  def loadPayeRecsFromParquet(appConfig: AppConfig) = {

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

}
