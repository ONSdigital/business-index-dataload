package uk.gov.ons.bi.dataload.reader

import org.apache.spark.sql.DataFrame
import uk.gov.ons.bi.dataload.model.BiSparkDataFrames
import uk.gov.ons.bi.dataload.utils.ContextMgr
/**
  * Created by websc on 21/02/2017.
  */

class CsvReader(ctxMgr: ContextMgr, tempTableName: String)
  extends BIDataReader {

  val spark = ctxMgr.spark
  val log =  ctxMgr.log
  
  def fixSchema(df: DataFrame): DataFrame = {
    // We have some spaces in the column names, which makes it hard to query dataframe in SQL.
    // This code removes any spaces or dots in the column names
    var newDf = df
    for (col <- df.columns) {
      newDf = newDf.withColumnRenamed(col, col.trim.replaceAll("\\s", "").replaceAllLiterally(".", ""))
    }
    newDf
  }

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
      .option("inferSchema", "false") // Automatically infer data types
      .csv(srcFilePath)


    if (BiSparkDataFrames.isDfEmpty(df))
      log.warn(s"No data loaded from CSV file: $srcFilePath")
    else
      log.info(s"Loaded CSV file: $srcFilePath")

    // fix spaces etc in column names
    val fixedDf = fixSchema(df)

    val extracted = extractRequiredFields(fixedDf)

    extracted
  }

}

