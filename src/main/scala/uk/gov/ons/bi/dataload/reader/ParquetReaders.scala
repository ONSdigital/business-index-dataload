package uk.gov.ons.bi.dataload.reader

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.utils.AppConfig

/**
  * Created by websc on 16/02/2017.
  */

class ParquetReader(implicit sc: SparkContext) {

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
    // Yields (Company No, company record)

    // Read Parquet data via SparkSQL but return as RDD so we can use RDD joins etc

    val df = getDataFrameFromParquet(appConfig, CH)

    // Only interested in a subset of columns
    df.select(
      $"CompanyNumber".as("co_number"),
      $"CompanyName".as("co_name"),
      $"CompanyStatus".as("co_status"),
      $"SICCodeSicText_1".as("co_sic_code1"),
      $"RegAddressPostCode".as("co_postcode")
    ).map { row => // NEED TO DEAL WITH null IN DATA!
      val companyNo = row.getString(0)
      (companyNo, CompanyRec(companyNo, row.getString(1), row.getString(2), row.getString(3), row.getString(4)))
    }

  }

  def loadLinkRecsFromParquet(appConfig: AppConfig): RDD[LinkRec] = {
    // Read Parquet data via SparkSQL but return as RDD so we can use RDD joins etc

    val df = getDataFrameFromParquet(appConfig, LINKS)

    // Nested data structure where only UBRN is mandatory
    df.select(
      $"UBRN",
      $"CH",
      $"VAT",
      $"PAYE"
    ).map { row =>
      val ubrn = row.getString(0)
      // CH is currently provided as an array but we only want the first entry (if any)
      val ch: Option[String] = if (row.isNullAt(1)) None else row.getSeq[String](1).headOption
      val vat: Option[Seq[String]] = if (row.isNullAt(2)) None else Some(row.getSeq[String](2))
      val paye: Option[Seq[String]] = if (row.isNullAt(3)) None else Some(row.getSeq[String](3))

      LinkRec(ubrn, ch, vat, paye)
    }

  }

  def loadPayeRecsFromParquet(appConfig: AppConfig): RDD[(String, PayeRec)] = {

    // Yields (PAYE Ref, PAYE record)

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
        |FROM temp_paye""".stripMargin)

    extracted.map { row =>
      val payeRef = row.getString(0)
      (payeRef,
        PayeRec(payeRef, row.getString(1), row.getString(2), row.getInt(3), row.getDouble(4),
          row.getDouble(5), row.getDouble(6), row.getDouble(7), row.getString(8)))
    }
  }

  def loadVatRecsFromParquet(appConfig: AppConfig): RDD[(String, VatRec)] = {

    // Yields (VAT Ref, VAT record)

    val df = getDataFrameFromParquet(appConfig, VAT)

    // Only interested in a subset of columns
    // Using SQL for more flexibility with conflicting datatypes in sample/real data
    df.registerTempTable("temp_vat")
    val extracted = sqlContext.sql(
      """
        |SELECT CAST(vatref AS LONG) AS vatref,
        |nameline1,
        |postcode,
        |sic92,
        |legalstatus,
        |CAST(turnover AS LONG) AS turnover
        |FROM temp_vat""".stripMargin)

    extracted.map { row =>
      val vatRef = row.getLong(0)
      (vatRef.toString,
        VatRec(vatRef, nameLine1 = row.getString(1), postcode = row.getString(2),
          sic92 = row.getInt(3), legalStatus = row.getInt(4), turnover = row.getLong(5))
      )
    }
  }
}
