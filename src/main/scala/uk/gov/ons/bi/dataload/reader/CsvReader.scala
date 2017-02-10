package uk.gov.ons.bi.dataload.reader

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by websc on 09/02/2017.
  */
abstract class CsvReader (val srcDir: String, val srcFile: String)
                         (implicit val sc: SparkContext){

  val sqlContext = new SQLContext(sc)


  def fixSchema(df: DataFrame): DataFrame = {

    // We have some spaces in the column names, which makes it hard to query dataframe
    // This code removes any spaces or dots in the column names
    var newDf = df
    for(col <- df.columns){
      newDf = newDf.withColumnRenamed(col,col.replaceAll("\\s", "").replaceAllLiterally(".", ""))
    }
    newDf
  }

  def readFromCsv: DataFrame = {
    val src = s"${srcDir}/${srcFile}"

    println(s"Reading CSV from: $src")

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(src)
    // fix spaces etc in column names
    fixSchema(df)
  }

  def extractRequiredFields(df: DataFrame):DataFrame

  def extractFromCsv: DataFrame = {
    val df = readFromCsv
    val extracted = extractRequiredFields(df)
    extracted
  }

  def writeParquet(df: DataFrame, targetDir: String, targetFile: String):Unit = {
    val path = s"${targetDir}/${targetFile}"
    println(s"Writing Parquet to: $path")

    df.write.mode("overwrite").parquet(path)
  }

}
