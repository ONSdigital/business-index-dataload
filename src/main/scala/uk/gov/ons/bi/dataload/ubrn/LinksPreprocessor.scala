package uk.gov.ons.bi.dataload.ubrn

import com.google.inject.Singleton
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import uk.gov.ons.bi.dataload.reader.LinkJsonReader
import uk.gov.ons.bi.dataload.utils.AppConfig

import scala.util.{Success, Try}

/**
  * Created by websc on 03/03/2017.
  */

@Singleton
class LinksPreprocessor(sc: SparkContext) {

  val defaultBaseUbrn = 1L

  def getMaxUbrn(df: DataFrame, ubrnColName: String = "UBRN"): Option[Long] = {
    // This will scan the DF column to extract the max value, assumes values are numeric.
    // Defaults to zero.
    Try {
      val row = df.agg(max(df(ubrnColName))).collect.head
      row.getLong(0)
    }
    match {
      case Success(n: Long) => Some(n)
      case _ => Some(defaultBaseUbrn)
    }
  }

  def applyNewUbrn(df: DataFrame, baseUbrn: Option[Long] = None) = {
    // First drop any rogue UBRN column (if any) from the input DF
    val noUbrn = df.drop("UBRN")

    // Set the base UBRN for adding to the monotonic sequential value
    val base = baseUbrn.getOrElse(defaultBaseUbrn)

    // Repartition to one partition so sequence is a fairly continuous range.
    // This will force data to be shuffled, which is inefficient.
    val numPartitions = df.rdd.getNumPartitions

    val df1partition = df.repartition(1)

    // Now add the new generated UBRN column and sequence value
    val df1partWithUbrn = df1partition.withColumn("UBRN", monotonicallyIncreasingId + base)

    // Repartition back to original num partitions (more data shuffling)
    df1partWithUbrn.repartition(numPartitions)
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
