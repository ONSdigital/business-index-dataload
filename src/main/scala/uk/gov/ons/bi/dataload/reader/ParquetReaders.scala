package uk.gov.ons.bi.dataload.reader

import com.google.inject.Singleton
import org.apache.spark.rdd._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.utils.{AppConfig, ContextMgr}

/**
  * Created by websc on 16/02/2017.
  */
@Singleton
class ParquetReader(ctxMgr: ContextMgr) extends BIDataReader {

  val sqlContext =  ctxMgr.spark

  override def readFromSourceFile(srcFilePath: String): DataFrame = {
    sqlContext.read.parquet(srcFilePath)
  }

  def getDataFrameFromParquet(appConfig: AppConfig, src: BIDataSource): DataFrame = {
    // Get data directories:
    // our business data Parquet files are stored under a working directory.
    val appDataConfig = appConfig.AppDataConfig
    val workingDir = appDataConfig.workingDir
    val parquetData = src match {
      case LINKS => appDataConfig.links
      case CH => appDataConfig.ch
      case VAT => appDataConfig.vat
      case PAYE => appDataConfig.paye
      case TCN_SIC_LOOKUP => appDataConfig.tcn
    }
    val dataFile = s"$workingDir/$parquetData"

    readFromSourceFile(dataFile)
  }

}

@Singleton
class CompanyRecsParquetReader(ctxMgr: ContextMgr) extends ParquetReader(ctxMgr: ContextMgr) {

  def loadFromParquet(appConfig: AppConfig): RDD[(String, CompanyRec)] = {
    // Yields RDD of (Company No, company record)

    // Read Parquet data via SparkSQL but return as RDD so we can use RDD joins etc.
    val df = getDataFrameFromParquet(appConfig, CH)
    // Using SQL for more flexibility with conflicting datatypes in sample/real data
    //df.registerTempTable("temp_comp")
    df.createOrReplaceTempView("temp_comp")

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
        |WHERE CompanyNumber IS NOT NULL""".stripMargin).rdd

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
}

@Singleton
class ProcessedLinksParquetReader(ctxMgr: ContextMgr) extends ParquetReader(ctxMgr: ContextMgr) {

  // Need these for DF/SQL ops
  import sqlContext.implicits._

  def loadFromParquet(appConfig: AppConfig): RDD[LinkRec] = {
    // Read Parquet data via SparkSQL but return as RDD so we can use RDD joins etc
    val df = getDataFrameFromParquet(appConfig, LINKS)

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
}

@Singleton
class PayeRecsParquetReader(ctxMgr: ContextMgr) extends ParquetReader(ctxMgr: ContextMgr) {

  def loadFromParquet(appConfig: AppConfig): RDD[(String, PayeRec)] = {

    // Yields RDD of (PAYE Ref, PAYE record)

    val payeDf = getDataFrameFromParquet(appConfig, PAYE)

    // Need to join to lookup table TCN-->SIC
    val lookupDf = getDataFrameFromParquet(appConfig, TCN_SIC_LOOKUP)

    // Only interested in a subset of columns
    // Using SQL for more flexibility with conflicting datatypes in sample/real data
    payeDf.registerTempTable("paye")

    // lookup columns are currently uppercase i.e. TCN and SIC07
    // lookup columns are integers, but PAYE columns are strings.
    lookupDf.registerTempTable("sic_lookup")

    val extracted = sqlContext.sql(
      """
        |SELECT
        |CAST(paye.payeref AS STRING) AS payeref,
        | paye.name1,
        | paye.postcode,
        | paye.status,
        | CAST(paye.dec_jobs AS DOUBLE) AS dec_jobs,
        | CAST(paye.mar_jobs AS DOUBLE) AS mar_jobs,
        | CAST(paye.june_jobs AS DOUBLE) AS june_jobs,
        | CAST(paye.sept_jobs AS DOUBLE) AS sept_jobs,
        | CAST(paye.jobs_lastupd AS STRING) AS jobs_lastupd,
        | CAST(paye.stc AS INT) AS stc,
        | sic_lookup.SIC07,
        | paye.deathcode
        |FROM paye LEFT OUTER JOIN sic_lookup ON (sic_lookup.TCN = paye.stc)
        |WHERE paye.payeref IS NOT NULL""".stripMargin).rdd

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

        val stc = if (row.isNullAt(9)) None else Option(row.getInt(9))
        val sic = if (row.isNullAt(10)) None else Option(row.getInt(10))

        val deathcode = if (row.isNullAt(11)) None else Option(row.getString(11))

        PayeRec(payeRef, nameLine1, postcode, legalStatus, decJobs, marJobs, junJobs, sepJobs,
          jobsLastUpd, stc, sic, deathcode)
      }
      (payeRefStr, rec)
    }
  }
}

@Singleton
class VatRecsParquetReader(ctxMgr: ContextMgr) extends ParquetReader(ctxMgr: ContextMgr) {

  def loadFromParquet(appConfig: AppConfig): RDD[(String, VatRec)] = {

    // Yields RDD of (VAT Ref, VAT record)

    val df = getDataFrameFromParquet(appConfig, VAT)

    // Only interested in a subset of columns. SQL is easier to maintain here.
    df.registerTempTable("temp_vat")
    val extracted = sqlContext.sql(
      """
        |SELECT CAST(vatref AS LONG) AS vatref,
        |name1,
        |postcode,
        |sic92,
        |status,
        |CAST(turnover AS LONG) AS turnover,
        |CAST (deathcode AS STRING) AS deathcode
        |FROM temp_vat
        |WHERE vatref IS NOT NULL""".stripMargin).rdd

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
        val deathcode = if (row.isNullAt(6)) None else Option(row.getString(6))

        VatRec(vatRef, nameLine1, postcode, sic92, legalStatus, turnover, deathcode)
      }
      (vatRefStr, rec)
    }
  }

}

@Singleton
class BIEntriesParquetReader(ctxMgr: ContextMgr) extends ParquetReader(ctxMgr: ContextMgr) {

  def loadFromParquet(appConfig: AppConfig): DataFrame = {
    // Read Parquet data for Business Indexes as DataFrame via SparkSQL

    // Get data directories
    val appDataConfig = appConfig.AppDataConfig
    val workingDir = appDataConfig.workingDir
    val biData = appDataConfig.bi

    val dataFile = s"$workingDir/$biData"

    sqlContext.read.parquet(dataFile)
  }
}

