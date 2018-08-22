package uk.gov.ons.bi.dataload.ubrn

import java.util.UUID

import com.google.inject.Singleton
import org.apache.spark.sql.DataFrame
import uk.gov.ons.bi.dataload.reader.{LinksParquetReader, PreviousLinkStore}
import uk.gov.ons.bi.dataload.utils.{AppConfig, ContextMgr}
import org.apache.spark.sql.functions.udf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Created by websc on 03/03/2017.
  */

@Singleton
class LinksPreprocessor(ctxMgr: ContextMgr) {

  // Create UDF to generate a UUID
  val generateUuid: UserDefinedFunction = udf(() => UUID.randomUUID().toString)

  def getNewLinksDataFromParquet(reader: LinksParquetReader , appConfig: AppConfig, inputPath: String): DataFrame = {
    // Load the Parquet links data
    reader.readFromSourceFile(inputPath)
  }

  def loadAndPreprocessLinks(appConfig: AppConfig) = {

    val appDataConfig = appConfig.AppDataConfig

    // get directory
    val workingDir = appDataConfig.workingDir
    val linksFile = appDataConfig.links
    val prevDir = appDataConfig.prevDir

    val linksDataConfig = appConfig.OnsDataConfig.linksDataConfig
    val dataDir = linksDataConfig.dir
    val parquetFile = linksDataConfig.parquet

    //concatenate strings to create path
    val inputPath = s"$dataDir/$parquetFile"
    val outputPath = s"$workingDir/$linksFile"
    val prevLinksFileParquetPath = s"$prevDir/$linksFile"

    // read links from parquet
    val parquetReader = new LinksParquetReader(ctxMgr)
    val parquetLinks  =  getNewLinksDataFromParquet(parquetReader, appConfig, inputPath)

    // Get previous links
    val previousLinkStore = new PreviousLinkStore(ctxMgr)
    val prevLinks = previousLinkStore.readFromSourceFile(prevLinksFileParquetPath)

    // WARNING:
    // UUID is generated when data is materialised e.g. in a SELECT statement,
    // so we need to PERSIST this data once we've added GID to fix it in place.
    val newLinks = parquetLinks.withColumn("GID", generateUuid())
    newLinks.persist(StorageLevel.MEMORY_AND_DISK)

    // Initialise LinkMatcher
    val matcher = new LinkMatcher(ctxMgr)

    // Apply all matching rules and get (matched, unmatched) records back
    val (withOldUbrn, needUbrn) = matcher.applyAllMatchingRules(newLinks, prevLinks)

    val maxUrbn = UbrnManager.getMaxUbrn(prevLinks)

    //Set new UBRN for these (start from max value from previous links)
    val withNewUbrn: DataFrame = UbrnManager.applyNewUbrn(parquetLinks, maxUrbn)

    // Finally, reconstruct full set of Links so we can save them all to Parquet
    val linksToSave = matcher.combineLinksToSave(withOldUbrn, withNewUbrn)
    // Cache the results because we want to write them to multiple files
    linksToSave.persist(StorageLevel.MEMORY_AND_DISK)

    //write output to hdfs
    parquetReader.writeParquet(withNewUbrn, outputPath)

    // We will also write a copy of the new preprocessed Links data to the "previous" dir:
    // 1. As e.g. LINKS_Output.parquet so we can easily pick it up next time
    previousLinkStore.writeAsPrevLinks(appConfig, withNewUbrn)

    // 2. As e.g. 201703081145/LINKS_Output.parquet so it does not get over-written later
    previousLinkStore.writeAsPrevLinks(appConfig, withNewUbrn, true)

    linksToSave.unpersist()
    newLinks.unpersist()
    withNewUbrn.unpersist()
  }

  def readWriteParquet(appConfig: AppConfig, parquetReader: LinksParquetReader, inputPath: String, outputPath: String) = {
    val parquetLinks = getNewLinksDataFromParquet(parquetReader, appConfig, inputPath)
    val withNewUbrn: DataFrame = UbrnManager.applyNewUbrn(parquetLinks)
    parquetReader.writeParquet(withNewUbrn, outputPath)
    withNewUbrn.unpersist()
  }
}
