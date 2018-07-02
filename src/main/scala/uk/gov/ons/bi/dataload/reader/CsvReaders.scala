package uk.gov.ons.bi.dataload.reader

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.model.{BiSparkDataFrames, BusinessDataSource}
import uk.gov.ons.bi.dataload.utils.ContextMgr

/**
  * Created by websc on 21/02/2017.
  */

class CsvReader(ctxMgr: ContextMgr, tempTableName: String)
  extends BIDataReader {

  val spark = ctxMgr.spark
  val log =  ctxMgr.log

  val companySchema = StructType(Array(
    StructField("CompanyName", StringType, true),
    StructField("CompanyNumber", StringType, true),
    StructField("RegAddressCareOf", StringType, true),
    StructField("RegAddressPOBox", StringType, true),
    StructField("RegAddressAddressLine1", StringType, true),
    StructField("RegAddressAddressLine2", StringType, true),
    StructField("RegAddressPostTown", StringType, true),
    StructField("RegAddressCounty", StringType, true),
    StructField("RegAddressCountry", StringType, true),
    StructField("RegAddressPostCode", StringType, true),
    StructField("CompanyCategory", StringType, true),
    StructField("CompanyStatus", StringType, true),
    StructField("CountryOfOrigin", StringType, true),
    StructField("DissolutionDate", StringType, true),
    StructField("IncorporationDate", StringType, true),
    StructField("AccountsAccountRefDay", StringType, true),
    StructField("AccountsAccountRefMonth", StringType, true),
    StructField("AccountsNextDueDate", StringType, true),
    StructField("AccountsLastMadeUpdate", StringType, true),
    StructField("AccountsAccountCategory", StringType, true),
    StructField("ReturnsNextDueDate", StringType, true),
    StructField("ReturnsLastMadeDate", StringType, true),
    StructField("MortgagesNumMortCharges", StringType, true),
    StructField("MortgagesNumMortOutstanding", StringType, true),
    StructField("MortgagesNumMortPartSatisfied", StringType, true),
    StructField("MortgagesNumMortSatisfied", StringType, true),
    StructField("SICCodeSicText_1", StringType, true),
    StructField("SICCodeSicText_2", StringType, true),
    StructField("SICCodeSicText_3", StringType, true),
    StructField("SICCodeSicText_4", StringType, true),
    StructField("LimitedPartnershipsNumGenPartners", StringType, true),
    StructField("LimitedPartnershipsNumLimPartners", StringType, true),
    StructField("URI", StringType, true),
    StructField("PreviousName_1CONDATE", StringType, true),
    StructField("PreviousNAme_1CompanyName", StringType, true),
    StructField("PreviousName_2CONDATE", StringType, true),
    StructField("PreviousNAme_2CompanyName", StringType, true),
    StructField("PreviousName_3CONDATE", StringType, true),
    StructField("PreviousNAme_3CompanyName", StringType, true),
    StructField("PreviousName_4CONDATE", StringType, true),
    StructField("PreviousNAme_4CompanyName", StringType, true),
    StructField("PreviousName_5CONDATE", StringType, true),
    StructField("PreviousNAme_5CompanyName", StringType, true),
    StructField("PreviousName_6CONDATE", StringType, true),
    StructField("PreviousNAme_6CompanyName", StringType, true),
    StructField("PreviousName_7CONDATE", StringType, true),
    StructField("PreviousNAme_7CompanyName", StringType, true),
    StructField("PreviousName_8CONDATE", StringType, true),
    StructField("PreviousNAme_8CompanyName", StringType, true),
    StructField("PreviousName_9CONDATE", StringType, true),
    StructField("PreviousNAme_9CompanyName", StringType, true),
    StructField("PreviousName_10CONDATE", StringType, true),
    StructField("PreviousNAme_10CompanyName", StringType, true),
    StructField("ConfStmtNextDueDate", StringType, true),
    StructField("ConfStmtLastMadeUpDate", StringType, true)
  ))

  val payeSchema = StructType(Array(
    StructField("entref", StringType, true),
    StructField("payeref", StringType, true),
    StructField("deathcode", StringType, true),
    StructField("birthdate", StringType, true),
    StructField("deathdate", StringType, true),
    StructField("mfullemp", StringType, true),
    StructField("msubemp", StringType, true),
    StructField("ffullemp", StringType, true),
    StructField("fsubemp", StringType, true),
    StructField("unclemp", StringType, true),
    StructField("unclsubemp", StringType, true),
    StructField("dec_jobs", DoubleType, true),
    StructField("mar_jobs", DoubleType, true),
    StructField("june_jobs", DoubleType, true),
    StructField("sept_jobs", DoubleType, true),
    StructField("jobs_lastupd", StringType, true),
    StructField("status", IntegerType, true),
    StructField("prevpaye", StringType, true),
    StructField("employer_cat", StringType, true),
    StructField("stc", IntegerType, true),
    StructField("crn", StringType, true),
    StructField("actiondate", StringType, true),
    StructField("addressref", StringType, true),
    StructField("marker", StringType, true),
    StructField("inqcode", StringType, true),
    StructField("name1", StringType, true),
    StructField("name2", StringType, true),
    StructField("name3", StringType, true),
    StructField("tradstyle1", StringType, true),
    StructField("tradstyle2", StringType, true),
    StructField("tradstyle3", StringType, true),
    StructField("address1", StringType, true),
    StructField("address2", StringType, true),
    StructField("address3", StringType, true),
    StructField("address4", StringType, true),
    StructField("address5", StringType, true),
    StructField("postcode", StringType, true),
    StructField("mkr", StringType, true)
  ))

  val vatSchema = StructType(Array(
    StructField("entref", StringType, true),
    StructField("vatref", LongType, true),
    StructField("deathcode", StringType, true),
    StructField("birthdate", StringType, true),
    StructField("deathdate", StringType, true),
    StructField("sic92", StringType, true),
    StructField("turnover", LongType, true),
    StructField("turnover_date", StringType, true),
    StructField("record_type", StringType, true),
    StructField("status", IntegerType, true),
    StructField("actiondate", StringType, true),
    StructField("crn", StringType, true),
    StructField("marker", StringType, true),
    StructField("addressref", StringType, true),
    StructField("inqcode", StringType, true),
    StructField("name1", StringType, true),
    StructField("name2", StringType, true),
    StructField("name3", StringType, true),
    StructField("tradstyle1", StringType, true),
    StructField("tradstyle2", StringType, true),
    StructField("tradstyle3", StringType, true),
    StructField("address1", StringType, true),
    StructField("address2", StringType, true),
    StructField("address3", StringType, true),
    StructField("address4", StringType, true),
    StructField("address5", StringType, true),
    StructField("postcode", StringType, true),
    StructField("mkr", StringType, true)
  ))

  def extractRequiredFields(df: DataFrame) = {

    df.createOrReplaceTempView(tempTableName)

    val extract = spark.sql(
      s"""
        |SELECT *
        |FROM $tempTableName
        |""".stripMargin)
    extract
  }

  def readFromSourceFile(srcFilePath: String): DataFrame = {

    val df = spark.read
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .csv(srcFilePath)


    if (BiSparkDataFrames.isDfEmpty(df))
      log.warn(s"No data loaded from CSV file: $srcFilePath")
    else
      log.info(s"Loaded CSV file: $srcFilePath")

    val extracted = extractRequiredFields(df)

    extracted
  }

  def readFromAdminSourceFile(srcFilePath: String, biSource: BusinessDataSource): DataFrame = {

    val df = biSource match {
      case CH =>  spark.read.option("header","true").schema(companySchema).csv(srcFilePath)
      case PAYE =>  spark.read.option("header","true").schema(payeSchema).csv(srcFilePath)
      case VAT =>  spark.read.option("header","true").schema(vatSchema).csv(srcFilePath)
    }

    if (BiSparkDataFrames.isDfEmpty(df))
      log.warn(s"No data loaded from CSV file: $srcFilePath")
    else
      log.info(s"Loaded CSV file: $srcFilePath")

    val extracted = extractRequiredFields(df)

    extracted
  }

}

