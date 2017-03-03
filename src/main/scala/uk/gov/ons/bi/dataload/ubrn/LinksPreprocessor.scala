package uk.gov.ons.bi.dataload.ubrn

import com.google.inject.Singleton
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import uk.gov.ons.bi.dataload.reader.LinkJsonReader
import uk.gov.ons.bi.dataload.utils.AppConfig

import scala.util.{Success, Try}

/**
  * Created by websc on 03/03/2017.
  */

@Singleton
class LinksPreprocessor(sc: SparkContext) {

  def getMaxUbrn(df: DataFrame, ubrnColName: String = "UBRN"): Option[Long] = {
    // This will scan the DF column to extract the max value, assumes values are numeric.
    // Defaults to zero.
    Try {
      val row = df.agg(max(df(ubrnColName))).collect.head
      row.getLong(0)
    }
    match {
      case Success(n: Long) => Some(n)
      case _ => Some(0L)
    }
  }

  def applyNewUbrn(df: DataFrame, baseUbrn: Option[Long] = None) = {
    // Set the base UBRN for adding to the monotonic sequential value
    val base = baseUbrn.getOrElse(0L)

    // First drop any rogue UBRN column (if any) from the input JSON file
    val noUbrn = df.drop("UBRN")
    // Now add a new generated UBRN column
    noUbrn.withColumn("UBRN", monotonicallyIncreasingId + base)
  }

  def preProcessLinks(df: DataFrame, baseUbrn: Option[Long] = None): DataFrame = {
    // Eventually this will involve comparing old/new links and setting UBRN.
    // For now, we just set the UBRN.
    // Base UBRN will default to 0 for now as we do not check previous link files.
    val withUbrn = applyNewUbrn(df, baseUbrn)

    withUbrn
  }

  def loadAndPreprocessLinks(appConfig: AppConfig) = {

    // Get source/target directories
    val sourceDataConfig = appConfig.SourceDataConfig
    val srcPath = sourceDataConfig.dir
    val dataDir = sourceDataConfig.linksDir
    val srcFile = sourceDataConfig.links
    val srcFilePath = s"$srcPath/$dataDir/$srcFile"

    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetFile = parquetDataConfig.links
    val targetFilePath = s"$parquetPath/$parquetFile"

    val reader = new LinkJsonReader(sc)

    // Load the JSON links data
    println(s"Reading from: $srcFilePath")
    val data = reader.readFromSourceFile(srcFilePath)

    // Do pre-processing
    val preproc = preProcessLinks(data)

    // Write the data to a Parquet output file
    println(s"Writing to: $targetFilePath")
    reader.writeParquet(preproc, targetFilePath)
  }

}
