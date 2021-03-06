package uk.gov.ons.bi.dataload.reader

import org.apache.spark.rdd._
import org.apache.spark.sql.DataFrame

import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.utils.{AppConfig, ContextMgr}

class ParquetReaders(appConfig: AppConfig, ctxMgr: ContextMgr) extends BIDataReader {

  val spark = ctxMgr.spark
  import spark.implicits._

  def readFromSourceFile(srcFilePath: String): DataFrame = {
    spark.read.parquet(srcFilePath)
  }

  def getDataFrameFromParquet(src: BIDataSource): DataFrame = {
    // Get data directories:
    // our business data Parquet files are stored under a working directory.
    val workingDir = appConfig.BusinessIndex.workPath
    val parquetData = src match {
      case LINKS => appConfig.BusinessIndex.links
      case CH => appConfig.BusinessIndex.ch
      case VAT => appConfig.BusinessIndex.vat
      case PAYE => appConfig.BusinessIndex.paye
      case TCN_SIC_LOOKUP => appConfig.BusinessIndex.tcn
    }
    val dataFile = s"$workingDir/$parquetData"

    readFromSourceFile(dataFile)
  }

  def chParquetReader(): RDD[(String, CompanyRec)] = {
    // Yields RDD of (Company No, company record)

    // Read Parquet data via SparkSQL but return as RDD so we can use RDD joins etc.
    val df = getDataFrameFromParquet(CH)
    // Using SQL for more flexibility with conflicting datatypes in sample/real data
    //df.registerTempTable("temp_comp")
    df.createOrReplaceTempView("temp_comp")

    // Only interested in a subset of columns. SQL is easier to maintain here.

    val extracted = spark.sql(
      """
        |SELECT
        | CompanyNumber,
        | CompanyName,
        | CompanyStatus,
        | SICCodeSicText_1,
        | RegAddressPostCode,
        | RegAddressAddressLine1,
        | RegAddressAddressLine2,
        | RegAddressPostTown,
        | RegAddressCounty,
        | RegAddressCountry
        |FROM temp_comp
        |WHERE CompanyNumber IS NOT NULL""".stripMargin).rdd

    // Need to be careful of NULLs vs blanks in data, so using explicit null-check here.
    extracted.map { row =>
      val companyNoStr = if (row.isNullAt(0)) "" else row.getString(0)
      val companyNo = if (row.isNullAt(0)) None else Option(row.getString(0))
      val companyName = if (row.isNullAt(1)) None else Option(row.getString(1))
      val companyStatus = if (row.isNullAt(2)) None else Option(row.getString(2))
      val sicCode1 = if (row.isNullAt(3)) None else Option(row.getString(3))
      val postcode = if (row.isNullAt(4)) None else Option(row.getString(4))
      val address1 = if (row.isNullAt(5)) None else Option(row.getString(5))
      val address2 = if (row.isNullAt(6)) None else Option(row.getString(6))
      val address3 = if (row.isNullAt(7)) None else Option(row.getString(7))
      val address4 = if (row.isNullAt(8)) None else Option(row.getString(8))
      val address5 = if (row.isNullAt(9)) None else Option(row.getString(9))

      (companyNoStr, CompanyRec(companyNo, companyName, companyStatus, sicCode1, postcode, address1, address2, address3, address4, address5))
    }
  }

  def linksParquetReader(): RDD[LinkRec] = {
    // Read Parquet data via SparkSQL but return as RDD so we can use RDD joins etc
    val df = getDataFrameFromParquet(LINKS)
    // NB: This is a nested data structure where CH/PAYE/VAT are lists, and only UBRN is mandatory
    df.select(
      $"UBRN",
      $"CH",
      $"VAT",
      $"PAYE"
    ).rdd.map { row =>
      val ubrn = if (row.isNullAt(0)) 1L else row.getLong(0)
      // CH is currently provided as an array but we only want the first entry (if any)
      val ch: Option[String] = if (row.isNullAt(1)) None else row.getSeq[String](1).headOption
      val vat: Option[Seq[String]] = if (row.isNullAt(2)) None else Option(row.getSeq[String](2))
      val paye: Option[Seq[String]] = if (row.isNullAt(3)) None else Option(row.getSeq[String](3))

      LinkRec(ubrn, ch, vat, paye)
    }.filter(lr => lr.ubrn >= 0) // Throw away Links with bad UBRNs
  }

  def payeParquetReader(): RDD[(String, PayeRec)] = {

    // Yields RDD of (PAYE Ref, PAYE record)

    val payeDf = getDataFrameFromParquet(PAYE)

    // Need to join to lookup table TCN-->SIC
    val lookupDf = getDataFrameFromParquet(TCN_SIC_LOOKUP)

    // Only interested in a subset of columns
    // Using SQL for more flexibility with conflicting datatypes in sample/real data
    payeDf.createOrReplaceTempView("paye")

    // lookup columns are currently uppercase i.e. TCN and SIC07
    // lookup columns are integers, but PAYE columns are strings.
    lookupDf.createOrReplaceTempView("sic_lookup")

    val extracted = spark.sql(
      """
        |SELECT
        |CAST(paye.payeref AS STRING) AS payeref,
        | paye.name1,
        | paye.name2,
        | paye.name3,
        | paye.postcode,
        | paye.status,
        | CAST(paye.dec_jobs AS DOUBLE) AS dec_jobs,
        | CAST(paye.mar_jobs AS DOUBLE) AS mar_jobs,
        | CAST(paye.june_jobs AS DOUBLE) AS june_jobs,
        | CAST(paye.sept_jobs AS DOUBLE) AS sept_jobs,
        | CAST(paye.jobs_lastupd AS STRING) AS jobs_lastupd,
        | CAST(paye.stc AS INT) AS stc,
        | CAST(sic_lookup.SIC07 AS STRING) AS SIC07,
        | paye.deathcode,
        | address1,
        | address2,
        | address3,
        | address4,
        | address5,
        | tradstyle1
        |FROM paye LEFT OUTER JOIN sic_lookup ON (sic_lookup.TCN = paye.stc)
        |WHERE paye.payeref IS NOT NULL""".stripMargin).rdd

    // Need to be careful of NULLs vs blanks in data, so using explicit null-check here.

    extracted.map { row =>
      val payeRefStr = if (row.isNullAt(0)) "" else row.getString(0)
      // Can't re-factor this to a separate function as you get Task not serializable errors
      val rec = {
        val payeRef = if (row.isNullAt(0)) None else Option(row.getString(0))
        val nameLine1 = if (row.isNullAt(1)) None else Option(row.getString(1))
        val nameLine2 = if (row.isNullAt(2)) None else Option(row.getString(2))
        val nameLine3 = if (row.isNullAt(3)) None else Option(row.getString(3))

        val name: Option[String] = Some(s"${nameLine1.getOrElse("")} ${nameLine2.getOrElse("")} ${nameLine3.getOrElse("")}")

        val postcode = if (row.isNullAt(4)) None else Option(row.getString(4))
        val legalStatus = if (row.isNullAt(5)) None else Option(row.getInt(5))

        val decJobs = if (row.isNullAt(6)) None else Option(row.getDouble(6))
        val marJobs = if (row.isNullAt(7)) None else Option(row.getDouble(7))
        val junJobs = if (row.isNullAt(8)) None else Option(row.getDouble(8))
        val sepJobs = if (row.isNullAt(9)) None else Option(row.getDouble(9))

        val jobsLastUpd = if (row.isNullAt(10)) None else Option(row.getString(10))

        val stc = if (row.isNullAt(11)) None else Option(row.getInt(11))
        val sic = if (row.isNullAt(12)) None else Option(row.getString(12))

        val deathcode = if (row.isNullAt(13)) None else Option(row.getString(13))

        val address1 = if (row.isNullAt(14)) None else Option(row.getString(14))
        val address2 = if (row.isNullAt(15)) None else Option(row.getString(15))
        val address3 = if (row.isNullAt(16)) None else Option(row.getString(16))
        val address4 = if (row.isNullAt(17)) None else Option(row.getString(17))
        val address5 = if (row.isNullAt(18)) None else Option(row.getString(18))
        val tradingStyle = if (row.isNullAt(19)) None else Option(row.getString(19))

        PayeRec(payeRef, name, postcode, legalStatus, decJobs, marJobs, junJobs, sepJobs,
          jobsLastUpd, stc, sic, deathcode, address1, address2, address3, address4, address5, tradingStyle)
      }
      (payeRefStr, rec)
    }
  }

  def vatParquetReader(): RDD[(String, VatRec)] = {

    // Yields RDD of (VAT Ref, VAT record)

    val df = getDataFrameFromParquet(VAT)

    // Only interested in a subset of columns. SQL is easier to maintain here.
    df.createOrReplaceTempView("temp_vat")
    val extracted = spark.sql(
      """
        | SELECT CAST(vatref AS LONG) AS vatref,
        | name1,
        | name2,
        | name3,
        | postcode,
        | CAST(sic92 AS STRING) AS sic92,
        | status,
        | CAST(turnover AS LONG) AS turnover,
        | CAST (deathcode AS STRING) AS deathcode,
        | address1,
        | address2,
        | address3,
        | address4,
        | address5,
        | tradstyle1
        | FROM temp_vat
        | WHERE vatref IS NOT NULL""".stripMargin).rdd

    // Need to be careful of NULLs vs blanks in data, so using explicit null-check here.
    extracted.map { row =>
      val vatRefStr = if (row.isNullAt(0)) "" else row.getLong(0).toString
      // Can't re-factor this to a separate function as you get Task not serializable errors
      val rec = {
        val vatRef = if (row.isNullAt(0)) None else Option(row.getLong(0))
        val nameLine1 = if (row.isNullAt(1)) None else Option(row.getString(1))
        val nameLine2 = if (row.isNullAt(2)) None else Option(row.getString(2))
        val nameLine3 = if (row.isNullAt(3)) None else Option(row.getString(3))

        val name: Option[String] = Some(s"${nameLine1.getOrElse("")} ${nameLine2.getOrElse("")} ${nameLine3.getOrElse("")}")

        val postcode = if (row.isNullAt(4)) None else Option(row.getString(4))
        val sic92 = if (row.isNullAt(5)) None else Option(row.getString(5))
        val legalStatus = if (row.isNullAt(6)) None else Option(row.getInt(6))
        val turnover = if (row.isNullAt(7)) None else Option(row.getLong(7))
        val deathcode = if (row.isNullAt(8)) None else Option(row.getString(8))
        val address1 = if (row.isNullAt(9)) None else Option(row.getString(9))
        val address2 = if (row.isNullAt(10)) None else Option(row.getString(10))
        val address3 = if (row.isNullAt(11)) None else Option(row.getString(11))
        val address4 = if (row.isNullAt(12)) None else Option(row.getString(12))
        val address5 = if (row.isNullAt(13)) None else Option(row.getString(13))
        val tradingStyle = if (row.isNullAt(14)) None else Option(row.getString(14))

        VatRec(vatRef, name, postcode, sic92, legalStatus, turnover, deathcode,
          address1, address2, address3, address4, address5, tradingStyle)
      }
      (vatRefStr, rec)
    }
  }

  def biParquetReader(): DataFrame = {
    // Read Parquet data for Business Indexes as DataFrame via SparkSQL

    // Get data directories
    val workingDir = appConfig.BusinessIndex.workPath
    val biData = appConfig.BusinessIndex.bi

    val dataFile = s"$workingDir/$biData"

    spark.read.parquet(dataFile)
  }
}


