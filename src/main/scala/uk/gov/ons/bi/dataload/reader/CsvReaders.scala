package uk.gov.ons.bi.dataload.reader

import com.google.inject.Singleton
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

/**
  * Created by websc on 21/02/2017.
  */

class CsvReader(sc: SparkContext, tempTableName: String)
  extends BIDataReader(sc: SparkContext) {

  def fixSchema(df: DataFrame): DataFrame = {

    // We have some spaces in the column names, which makes it hard to query dataframe
    // This code removes any spaces or dots in the column names
    var newDf = df
    for (col <- df.columns) {
      newDf = newDf.withColumnRenamed(col, col.trim.replaceAll("\\s", "").replaceAllLiterally(".", ""))
    }
    newDf
  }

  def extractRequiredFields(df: DataFrame) = {

    df.registerTempTable(tempTableName)

    val extract = sqlContext.sql(
      s"""
        |SELECT *
        |FROM $tempTableName
        |""".stripMargin)

    extract
  }

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

