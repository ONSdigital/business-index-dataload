package uk.gov.ons.bi.dataload.reader

import com.google.inject.Singleton
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

/**
  * Created by websc on 21/02/2017.
  */


abstract class CsvReader(implicit sc: SparkContext)
  extends BIDataReader {

  def fixSchema(df: DataFrame): DataFrame = {

    // We have some spaces in the column names, which makes it hard to query dataframe
    // This code removes any spaces or dots in the column names
    var newDf = df
    for (col <- df.columns) {
      newDf = newDf.withColumnRenamed(col, col.replaceAll("\\s", "").replaceAllLiterally(".", ""))
    }
    newDf
  }

  def extractRequiredFields(df: DataFrame): DataFrame

  def readFromSourceFile(srcFilePath: String): DataFrame = {

    println(s"Reading from CSV files: $srcFilePath")

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(srcFilePath)
    // fix spaces etc in column names
    val fixedDf = fixSchema(df)

    val extracted = extractRequiredFields(fixedDf)

    extracted
  }

}

@Singleton
class CompaniesHouseCsvReader(implicit sc: SparkContext)
  extends CsvReader {

  val rawTable = "raw_companies"

  def extractRequiredFields(df: DataFrame) = {

    df.registerTempTable("raw_companies")

    val extract = sqlContext.sql(
      """
        |SELECT *
        |FROM raw_companies
        |""".stripMargin)

    extract
  }
}

@Singleton
class PayeCsvReader(implicit sc: SparkContext)
  extends CsvReader {

  val rawTable = "raw_paye"

  def extractRequiredFields(df: DataFrame) = {
    // allows us to include/exclude specific fields here

    df.registerTempTable(rawTable)

    val extract = sqlContext.sql(
      s"""
         |SELECT *
         |FROM ${rawTable}
         |""".stripMargin)

    extract
  }
}

@Singleton
class VatCsvReader(implicit sc: SparkContext)
  extends CsvReader {

  val rawTable = "raw_vat"

  def extractRequiredFields(df: DataFrame): DataFrame = {

    df.registerTempTable(rawTable)

    val extract: DataFrame = sqlContext.sql(
      s"""
         |SELECT *
         |FROM ${rawTable}
         |""".stripMargin)

    extract
  }

}

