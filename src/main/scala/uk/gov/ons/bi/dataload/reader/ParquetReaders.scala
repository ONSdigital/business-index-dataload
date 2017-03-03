package uk.gov.ons.bi.dataload.reader

import com.google.inject.Singleton
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.joda.time.DateTime
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.utils.AppConfig

import scala.util.{Success, Try}

/**
  * Created by websc on 16/02/2017.
  */
@Singleton
class ParquetReader(sc: SparkContext) {

  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  def getDataFrameFromParquet(appConfig: AppConfig, src: BIDataSource): DataFrame = {
    // Read Parquet data via SparkSQL

    // Get data directories
    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetData = src match {
      case LINKS => parquetDataConfig.links
      case CH => parquetDataConfig.ch
      case VAT => parquetDataConfig.vat
      case PAYE => parquetDataConfig.paye
    }
    val dataFile = s"$parquetPath/$parquetData"

    sqlContext.read.parquet(dataFile)
  }

  def loadCompanyRecsFromParquet(appConfig: AppConfig): RDD[(String, CompanyRec)] = {
    // Yields RDD of (Company No, company record)

    // Read Parquet data via SparkSQL but return as RDD so we can use RDD joins etc.
    val df = getDataFrameFromParquet(appConfig, CH)
    // Using SQL for more flexibility with conflicting datatypes in sample/real data
    df.registerTempTable("temp_comp")

    // Only interested in a subset of columns. SQL is easier to maintain here.

    val extracted = sqlContext.sql(
      """
        |SELECT
        | CompanyNumber,
        | CompanyName,
        | CompanyStatus,
        | SICCodeSicText_1,
        | RegAddressPostCode
        |FROM temp_comp
        |WHERE CompanyNumber IS NOT NULL""".stripMargin)

    // Need to be careful of NULLs vs blanks in data, so using explicit null-check here.
    extracted.map { row =>
      val companyNoStr = if (row.isNullAt(0)) "" else row.getString(0)
      val companyNo = if (row.isNullAt(0)) None else Option(row.getString(0))
      val companyName = if (row.isNullAt(1)) None else Option(row.getString(1))
      val companyStatus = if (row.isNullAt(2)) None else Option(row.getString(2))
      val sicCode1 = if (row.isNullAt(3)) None else Option(row.getString(3))
      val postcode = if (row.isNullAt(4)) None else Option(row.getString(4))

      (companyNoStr, CompanyRec(companyNo, companyName, companyStatus, sicCode1, postcode))
    }

  }


  def loadLinkRecsFromParquet(appConfig: AppConfig): RDD[LinkRec] = {
    // Read Parquet data via SparkSQL but return as RDD so we can use RDD joins etc
    val df = getDataFrameFromParquet(appConfig, LINKS)

    // NB: This is a nested data structure where CH/PAYE/VAT are lists, and only UBRN is mandatory
    df.select(
      $"UBRN",
      $"CH",
      $"VAT",
      $"PAYE"
    ).map { row =>
      // UBRN is String in original JSON but we need to convert it to long (or use -1 for bad strings)
      val ubrnStr = row.getString(0)
      val ubrn = Try {ubrnStr.toLong}
      match {
        case Success(n: Long) => n
        case _ => -1L
      }
      // CH is currently provided as an array but we only want the first entry (if any)
      val ch: Option[String] = if (row.isNullAt(1)) None else row.getSeq[String](1).headOption
      val vat: Option[Seq[String]] = if (row.isNullAt(2)) None else Option(row.getSeq[String](2))
      val paye: Option[Seq[String]] = if (row.isNullAt(3)) None else Option(row.getSeq[String](3))

      LinkRec(ubrn, ch, vat, paye)
    }.filter(lr => lr.ubrn > 0)  // Throw away Links with bad UBRNs

  }

  def loadPayeRecsFromParquet(appConfig: AppConfig): RDD[(String, PayeRec)] = {

    // Yields RDD of (PAYE Ref, PAYE record)

    val df = getDataFrameFromParquet(appConfig, PAYE)

    // Only interested in a subset of columns
    // Using SQL for more flexibility with conflicting datatypes in sample/real data
    df.registerTempTable("temp_paye")
    val extracted = sqlContext.sql(
      """
        |SELECT
        |CAST(payeref AS STRING) AS payeref,
        | nameline1,
        | postcode,
        | legalstatus,
        | CAST(dec_jobs AS DOUBLE) AS dec_jobs,
        | CAST(mar_jobs AS DOUBLE) AS mar_jobs,
        | CAST(june_jobs AS DOUBLE) AS june_jobs,
        | CAST(sept_jobs AS DOUBLE) AS sept_jobs,
        | CAST(jobs_lastupd AS STRING) AS jobs_lastupd
        |FROM temp_paye
        |WHERE payeref IS NOT NULL""".stripMargin)

    // Need to be careful of NULLs vs blanks in data, so using explicit null-check here.

    extracted.map { row =>
      val payeRefStr = if (row.isNullAt(0)) "" else row.getString(0)
      // Can't re-factor this to a separate function as you get Task not serializable errors
      val rec = {
        val payeRef = if (row.isNullAt(0)) None else Option(row.getString(0))
        val nameLine1 = if (row.isNullAt(1)) None else Option(row.getString(1))
        val postcode = if (row.isNullAt(2)) None else Option(row.getString(2))
        val legalStatus = if (row.isNullAt(3)) None else Option(row.getInt(3))

        val decJobs = if (row.isNullAt(4)) None else Option(row.getDouble(4))
        val marJobs = if (row.isNullAt(5)) None else Option(row.getDouble(5))
        val junJobs = if (row.isNullAt(6)) None else Option(row.getDouble(6))
        val sepJobs = if (row.isNullAt(7)) None else Option(row.getDouble(7))

        val jobsLastUpd = if (row.isNullAt(8)) None else Option(row.getString(8))

        PayeRec(payeRef, nameLine1, postcode, legalStatus, decJobs, marJobs, junJobs, sepJobs, jobsLastUpd)
      }
      (payeRefStr, rec)
    }

  }


  def loadVatRecsFromParquet(appConfig: AppConfig): RDD[(String, VatRec)] = {

    // Yields RDD of (VAT Ref, VAT record)

    val df = getDataFrameFromParquet(appConfig, VAT)

    // Only interested in a subset of columns. SQL is easier to maintain here.
    df.registerTempTable("temp_vat")
    val extracted = sqlContext.sql(
      """
        |SELECT CAST(vatref AS LONG) AS vatref,
        |nameline1,
        |postcode,
        |sic92,
        |legalstatus,
        |CAST(turnover AS LONG) AS turnover
        |FROM temp_vat
        |WHERE vatref IS NOT NULL""".stripMargin)

    // Need to be careful of NULLs vs blanks in data, so using explicit null-check here.
    extracted.map { row =>
      val vatRefStr = if (row.isNullAt(0)) "" else row.getLong(0).toString
      // Can't re-factor this to a separate function as you get Task not serializable errors
      val rec = {
        val vatRef = if (row.isNullAt(0)) None else Option(row.getLong(0))
        val nameLine1 = if (row.isNullAt(1)) None else Option(row.getString(1))
        val postcode = if (row.isNullAt(2)) None else Option(row.getString(2))
        val sic92 = if (row.isNullAt(3)) None else Option(row.getInt(3))
        val legalStatus = if (row.isNullAt(4)) None else Option(row.getInt(4))
        val turnover = if (row.isNullAt(5)) None else Option(row.getLong(5))

        VatRec(vatRef, nameLine1, postcode, sic92, legalStatus, turnover)
      }
      (vatRefStr, rec)
    }
  }

  def getBIEntriesFromParquet(appConfig: AppConfig): DataFrame = {
    // Read Parquet data for Business Indexes as DataFrame via SparkSQL

    // Get data directories
    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetData = parquetDataConfig.bi

    val dataFile = s"$parquetPath/$parquetData"

    sqlContext.read.parquet(dataFile)
  }
}
