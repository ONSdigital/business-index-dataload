package uk.gov.ons.bi.dataload.ubrn

import java.util.UUID

import com.google.inject.Singleton
import org.apache.spark.sql.DataFrame
import uk.gov.ons.bi.dataload.reader.LinksParquetReader
import uk.gov.ons.bi.dataload.utils.{AppConfig, ContextMgr}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Created by websc on 03/03/2017.
  */

@Singleton
class LinksPreprocessor(ctxMgr: ContextMgr) {

  // Create UDF to generate a UUID
  val generateUuid: UserDefinedFunction = udf(() => UUID.randomUUID().toString)

  def getNewLinksDataFromParquet(reader: LinksParquetReader , appConfig: AppConfig, inputPath: String): DataFrame = {
    // get source/target directories
//    val linksDataConfig = appConfig.OnsDataConfig.linksDataConfig
//    val dataDir = linksDataConfig.dir
//    val parquetFile = linksDataConfig.parquet
//    val parquetFilePath = s"$dataDir/$parquetFile"

    // Load the JSON links data
    reader.readFromSourceFile(inputPath)
  }

  def loadAndPreprocessLinks(appConfig: AppConfig) = {

    // Lot of caching needed here, so we cache to disk and memory

    // WARNING:
    // UUID is generated when data is materialised e.g. in a SELECT statement,
    // so we need to PERSIST this data once we've added GID to fix it in place.

    // Parquet file locations from configuration (or runtime params)
    val appDataConfig = appConfig.AppDataConfig
    val workingDir = appDataConfig.workingDir
    val linksFile = appDataConfig.links

    val linksDataConfig = appConfig.OnsDataConfig.linksDataConfig
    val dataDir = linksDataConfig.dir
    val parquetFile = linksDataConfig.parquet
    val parquetFilePath = s"$dataDir/$parquetFile"
    val newLinksFileParquetPath = s"$workingDir/$linksFile"

    val parquetReader = new LinksParquetReader(ctxMgr)

    readWriteParquet(appConfig, parquetReader,parquetFilePath, newLinksFileParquetPath)

    //val parquetLinks = getNewLinksDataFromParquet(parquetReader, appConfig, parquetFilePath)

//    val withNewUbrn: DataFrame = UbrnManager.applyNewUbrn(parquetLinks)
//    parquetReader.writeParquet(withNewUbrn, newLinksFileParquetPath)
//    withNewUbrn.unpersist()

    //val newLinks = parquetLinks.withColumn("GID", generateUuid())
    //newLinks.persist(StorageLevel.MEMORY_AND_DISK)

    // Previous and current Links file  have same name but diff location
    //val prevDir = appDataConfig.prevDir
    //val prevLinksFileParquetPath = s"$prevDir/$linksFile"

    // Get previous links
    //val previousLinkStore = new PreviousLinkStore(ctxMgr)
    //val prevLinks = previousLinkStore.readFromSourceFile(prevLinksFileParquetPath)

    // Initialise LinkMatcher
    //val matcher = new LinkMatcher(ctxMgr)

    // Apply all matching rules and get (matched, unmatched) records back
    //val (withOldUbrn, needUbrn) = matcher.applyAllMatchingRules(newLinks, prevLinks)

    // ------------------------------
    // Now we can set new UBRNs for unmatched records
    // ------------------------------
    // Set new UBRN for these (start from max value from previous links)

    //val maxUrbn = UbrnManager.getMaxUbrn(prevLinks)

//    val withNewUbrn: DataFrame = UbrnManager.applyNewUbrn(parquetLinks)

    // Finally, reconstruct full set of Links so we can save them all to Parquet
    //val linksToSave = matcher.combineLinksToSave(withOldUbrn, parquetLinks)

    // Cache the results because we want to write them to multiple files
    //linksToSave.persist(StorageLevel.MEMORY_AND_DISK)

    // Write preprocessed Links data to a Parquet output file ready for subsequent processing
//    parquetReader.writeParquet(withNewUbrn, newLinksFileParquetPath)

    // We will also write a copy of the new preprocessed Links data to the "previous" dir:
    // 1. As e.g. LINKS_Output.parquet so we can easily pick it up next time
    //previousLinkStore.writeAsPrevLinks(appConfig, withNewUbrn)

    // 2. As e.g. 201703081145/LINKS_Output.parquet so it does not get over-written later
    //previousLinkStore.writeAsPrevLinks(appConfig, withNewUbrn, true)

    // Clear cached data we no longer need
    //withNewUbrn.unpersist()
//    prevLinks.unpersist()
//    newLinks.unpersist()
  }

  def readWriteParquet(appConfig: AppConfig, parquetReader: LinksParquetReader, inputPath: String, outputPath: String) = {
    println(outputPath)
    val parquetLinks = getNewLinksDataFromParquet(parquetReader, appConfig, inputPath)
    val withNewUbrn: DataFrame = UbrnManager.applyNewUbrn(parquetLinks)
    parquetReader.writeParquet(withNewUbrn, outputPath)
    withNewUbrn.unpersist()
  }

}
