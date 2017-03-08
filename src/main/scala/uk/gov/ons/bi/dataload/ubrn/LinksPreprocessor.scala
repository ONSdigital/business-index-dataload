package uk.gov.ons.bi.dataload.ubrn

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import com.google.inject.Singleton
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import uk.gov.ons.bi.dataload.reader.{BIDataReader, LinkJsonReader, ParquetReader}
import uk.gov.ons.bi.dataload.utils.AppConfig

import scala.util.{Success, Try}

/**
  * Created by websc on 03/03/2017.
  */

@Singleton
class LinksPreprocessor(sc: SparkContext) {

  val defaultBaseUbrn = 100000000000L
  val defaultUbrnColName = "UBRN"

  def getMaxUbrn(df: DataFrame, ubrnColName: String = defaultUbrnColName): Option[Long] = {
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
    val noUbrn = df.drop(defaultUbrnColName)

    // Set the base UBRN for adding to the monotonic sequential value
    val base = baseUbrn.getOrElse(defaultBaseUbrn)

    // Repartition to one partition so sequence is a fairly continuous range.
    // This will force data to be shuffled, which is inefficient.
    val numPartitions = df.rdd.getNumPartitions

    val df1partition = df.repartition(1)

    // Now add the new generated UBRN column and sequence value
    val df1partWithUbrn = df1partition.withColumn(defaultUbrnColName, monotonicallyIncreasingId + base)

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

  def getNewLinksDataFromJson(reader: LinkJsonReader, appConfig: AppConfig): DataFrame = {
    // Get source/target directories
    val sourceDataConfig = appConfig.SourceDataConfig
    val srcPath = sourceDataConfig.dir
    val dataDir = sourceDataConfig.linksDir
    val srcFile = sourceDataConfig.links
    val srcFilePath = s"$srcPath/$dataDir/$srcFile"

    // Load the JSON links data
    reader.readFromSourceFile(srcFilePath)
  }

  def writeAsPrevLinks(appConfig: AppConfig, df: DataFrame, timestamped: Boolean = false) = {
    // Use timestamp as YYYYMMDD
    val ts = if (timestamped) {
                val fmt = DateTimeFormat.forPattern("yyyyMMddHHmm")

                val now = DateTime.now()
                now.toString(fmt)
              }
              else ""

    // Parquet file locations from configuration (or runtime params)
    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetFile = parquetDataConfig.links
    val prevDir = parquetDataConfig.prevDir
    val prevLinksFile = s"$prevDir/$ts/$parquetFile"

    // We will also write a copy of the preprocessed Links data to the "previous" dir
    df.write.mode("overwrite").parquet(prevLinksFile)
  }

  def loadAndPreprocessLinks(appConfig: AppConfig) = {

    // Load the new Links from JSON
    val jsonReader = new LinkJsonReader(sc)
    val newLinks = getNewLinksDataFromJson(jsonReader, appConfig)

    // Parquet file locations from configuration (or runtime params)
    val parquetDataConfig = appConfig.ParquetDataConfig
    val parquetPath = parquetDataConfig.dir
    val parquetFile = parquetDataConfig.links
    val targetFilePath = s"$parquetPath/$parquetFile"
    val prevDir = parquetDataConfig.prevDir
    val prevLinksFile = s"$prevDir/$parquetFile"

    // Do pre-processing
    val preproc = preProcessLinks(newLinks)

    // Cache the results because we want to write them to multiple files
    preproc.cache()

    // Write preprocessed Links data to a Parquet output file ready for subsequent processing
    jsonReader.writeParquet(preproc, targetFilePath)

    // We will also write a copy of the new preprocessed Links data to the "previous" dir:
    // 1. As e.g. LINKS_Output.parquet so we can easily pick it up next time
    writeAsPrevLinks(appConfig, preproc)

    // 2. As e.g. 201703081145/LINKS_Output.parquet so it does not get over-written later
    writeAsPrevLinks(appConfig, preproc, true)

    // Free the cached data
    preproc.unpersist()
  }

}
