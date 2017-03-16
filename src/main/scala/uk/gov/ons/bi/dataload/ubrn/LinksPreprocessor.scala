package uk.gov.ons.bi.dataload.ubrn

import com.google.inject.Singleton
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import uk.gov.ons.bi.dataload.reader.{LinkJsonReader, PreviousLinkStore}
import uk.gov.ons.bi.dataload.utils.AppConfig

/**
  * Created by websc on 03/03/2017.
  */

@Singleton
class LinksPreprocessor(sc: SparkContext) {

  def getNewLinksDataFromJson(reader: LinkJsonReader, appConfig: AppConfig): DataFrame = {
    // get source/target directories
    val linksDataConfig = appConfig.LinksDataConfig
    val dataDir = linksDataConfig.dir
    val jsonfile = linksDataConfig.json
    val jsonFilePath = s"$dataDir/$jsonfile"

    // Load the JSON links data
    reader.readFromSourceFile(jsonFilePath)
  }

  def loadAndPreprocessLinks(appConfig: AppConfig) = {

    // Load the new Links from JSON
    val jsonReader = new LinkJsonReader(sc)
    val newLinks = getNewLinksDataFromJson(jsonReader, appConfig)
    newLinks.cache()

    // Parquet file locations from configuration (or runtime params)
    val appDataConfig = appConfig.AppDataConfig
    val workingDir = appDataConfig.workingDir
    val linksFile = appDataConfig.links

    // Previous and current Links file  have same name but diff location
    val prevDir = appDataConfig.prevDir
    val prevLinksFileParquetPath = s"$prevDir/$linksFile"
    val newLinksFileParquetPath = s"$workingDir/$linksFile"

    // Get previous links
    val previousLinkStore = new PreviousLinkStore(sc)
    val prevLinks = previousLinkStore.readFromSourceFile(prevLinksFileParquetPath)
    prevLinks.cache()

    // Initialise LinkMatcher
    val matcher = new LinkMatcher(sc)

    // Get easy matches first (CH=CH, or no CH but other contents same)
    val (simpleChMatches, unmatched1) = matcher.simpleMatches(newLinks, prevLinks)

    // ----------------
    // CALL EXTRA MATCHING RULES HERE ...
    // ----------------
    // Only try to match records that we have not already matched
    val (otherMatches, unmatched2) = matcher.matchedOnOtherRules(unmatched1, prevLinks)

    // Finally we should have :
    // - one sub-set of new links that we have matched, so they now have a UBRN:
    val withOldUbrn: DataFrame = matcher.combineLinksToSave(simpleChMatches, otherMatches)
    // - and one sub-set of new links that we could not match, so they need new UBRN:
    val needUbrn: DataFrame = unmatched2

    // ------------------------------
    // Now we can set new UBRNs for unmatched records
    // ------------------------------
    // Set new UBRN for these (start from max value from previous links)

    val ubrnManager = new UbrnManager(sc)

    val maxUrbn = ubrnManager.getMaxUbrn(prevLinks)

    val withNewUbrn: DataFrame = ubrnManager.applyNewUbrn(needUbrn, maxUrbn)


    // Finally, reconstruct full set of Links so we can save them all to Parquet
    val linksToSave = matcher.combineLinksToSave(withOldUbrn, withNewUbrn)

    // Cache the results because we want to write them to multiple files
    linksToSave.cache()

    // Clear other cached data we no longer need
    prevLinks.unpersist()
    newLinks.unpersist()

    // Write preprocessed Links data to a Parquet output file ready for subsequent processing
    jsonReader.writeParquet(linksToSave, newLinksFileParquetPath)

    // We will also write a copy of the new preprocessed Links data to the "previous" dir:
    // 1. As e.g. LINKS_Output.parquet so we can easily pick it up next time
    previousLinkStore.writeAsPrevLinks(appConfig, linksToSave)

    // 2. As e.g. 201703081145/LINKS_Output.parquet so it does not get over-written later
    previousLinkStore.writeAsPrevLinks(appConfig, linksToSave, true)

    // Clear cached data we no longer need
    linksToSave.unpersist()
  }

}
