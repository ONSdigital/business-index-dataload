package uk.gov.ons.bi.dataload.reader

import org.apache.spark.sql.DataFrame
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.model.{BiSparkDataFrames, BusinessDataSource}
import uk.gov.ons.bi.dataload.utils.ContextMgr

/**
  * Created by websc on 21/02/2017.
  */

class CsvReader(ctxMgr: ContextMgr, tempTableName: String) extends BIDataReader {

  val spark = ctxMgr.spark
  val log =  ctxMgr.log

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
      .schema(AdminDataSchema.tcnSchema)
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
      case CH =>  spark.read.option("header","true").schema(AdminDataSchema.companySchema).csv(srcFilePath)
      case PAYE =>  spark.read.option("header","true").schema(AdminDataSchema.payeSchema).csv(srcFilePath)
      case VAT =>  spark.read.option("header","true").schema(AdminDataSchema.vatSchema).csv(srcFilePath)
      case TCN => spark.read.option("header", "true").schema(AdminDataSchema.tcnSchema).csv(srcFilePath)
    }

    if (BiSparkDataFrames.isDfEmpty(df))
      log.warn(s"No data loaded from CSV file: $srcFilePath")
    else
      log.info(s"Loaded CSV file: $srcFilePath")

    val extracted = extractRequiredFields(df)

    extracted
  }

}

