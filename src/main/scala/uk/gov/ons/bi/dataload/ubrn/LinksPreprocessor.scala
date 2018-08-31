package uk.gov.ons.bi.dataload.ubrn

import java.util.UUID

import com.google.inject.Singleton

import uk.gov.ons.bi.dataload.reader.{BIDataReader, PreviousLinkStore}
import uk.gov.ons.bi.dataload.utils.{AppConfig, ContextMgr}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Created by websc on 03/03/2017.
  */

@Singleton
class LinksPreprocessor(ctxMgr: ContextMgr) extends PreviousLinkStore(ctxMgr) with BIDataReader{

  // Create UDF to generate a UUID
  val generateUuid: UserDefinedFunction = udf(() => UUID.randomUUID().toString)

  def getNewLinksDataFromParquet(appConfig: AppConfig): DataFrame = {

    // get source/target directories
    val linksDataConfig = appConfig.OnsDataConfig.linksDataConfig
    val dataDir = linksDataConfig.dir
    val parquetFile = linksDataConfig.file
    val parquetFilePath = s"$dataDir/$parquetFile"

    // Load the JSON links data
    readFromSourceFile(parquetFilePath)
  }

  // apply ubrn and write to parquet
  def applyUBRN(prevLinks: DataFrame, parquetLinks: DataFrame) = {

    val maxUrbn = UbrnManager.getMaxUbrn(prevLinks)

    //Set new UBRN for these (start from max value from previous links)
    val withNewUbrn: DataFrame = UbrnManager.applyNewUbrn(parquetLinks, maxUrbn)
    withNewUbrn
  }

  def writeLinksToParquet(withNewUbrn: DataFrame, outputPath: String, appConfig: AppConfig) = {
    //write output to hdfs
    writeParquet(withNewUbrn, outputPath)

    // We will also write a copy of the new preprocessed Links data to the "previous" dir:
    // 1. As e.g. LINKS_Output.parquet so we can easily pick it up next time
    writeAsPrevLinks(appConfig, withNewUbrn)

    // 2. As e.g. 201703081145/LINKS_Output.parquet so it does not get over-written later
    writeAsPrevLinks(appConfig, withNewUbrn, true)
  }

  def loadLinksToDF(appConfig: AppConfig): (DataFrame, DataFrame, String) = {
    // Lot of caching needed here, so we cache to disk and memory
    // Load the new Links from JSON
    val appDataConfig = appConfig.AppDataConfig

    // get directory
    val workingDir = appDataConfig.workingDir
    val prevDir = appDataConfig.prevDir
    val linksFile = appDataConfig.links

    val linksDataConfig = appConfig.OnsDataConfig.linksDataConfig
    val dataDir = linksDataConfig.dir
    val parquetFile = linksDataConfig.parquet

    //concatenate strings to create path
    val inputPath = s"$dataDir/$parquetFile"
    val outputPath = s"$workingDir/$linksFile"
    val prevLinksFileParquetPath = s"$prevDir/$linksFile"

    // read links from parquet
    val parquetLinks = getNewLinksDataFromParquet(appConfig)

    // Get previous links
    val prevLinks = readFromSourceFile(prevLinksFileParquetPath)
    (parquetLinks, prevLinks, outputPath)
  }

  def loadAndPreprocessLinks(appConfig: AppConfig) = {

    val (parquetLinks, prevLinks, outputPath) = loadLinksToDF(appConfig)

    // WARNING:
    // UUID is generated when data is materialised e.g. in a SELECT statement,
    // so we need to PERSIST this data once we've added GID to fix it in place.
    val newLinks = parquetLinks.withColumn("GID", generateUuid())
    newLinks.persist(StorageLevel.MEMORY_AND_DISK)

    // Initialise LinkMatcher
    val matcher = new LinkMatcher(ctxMgr)

    // Apply all matching rules and get (matched, unmatched) records back
    val (withOldUbrn, needUbrn) = matcher.applyAllMatchingRules(newLinks, prevLinks)
    val withNewUbrn = applyUBRN(prevLinks, parquetLinks)

    // Finally, reconstruct full set of Links so we can save them all to Parquet
    val linksToSave = matcher.combineLinksToSave(withOldUbrn, withNewUbrn)
    // Cache the results because we want to write them to multiple files
    linksToSave.persist(StorageLevel.MEMORY_AND_DISK)

    writeLinksToParquet(withNewUbrn, outputPath, appConfig)

    linksToSave.unpersist()
    newLinks.unpersist()
    withNewUbrn.unpersist()
  }
}