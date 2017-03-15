package uk.gov.ons.bi.dataload.ubrn

import com.google.inject.Singleton
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import uk.gov.ons.bi.dataload.reader.{LinkJsonReader, PreviousLinksReader}
import uk.gov.ons.bi.dataload.utils.AppConfig

import scala.util.{Success, Try}

/**
  * Created by websc on 03/03/2017.
  */

@Singleton
class LinksPreprocessor(sc: SparkContext) {

  val defaultBaseUbrn = 100000000000L
  val defaultUbrnColName = "UBRN"

  // Use getOrCreate in case SQLContext already exists (only want one)
  val sqlContext: SQLContext = SQLContext.getOrCreate(sc)

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

  def applyNewUbrn(df: DataFrame, baseUbrn: Option[Long] = None): DataFrame = {
    // First drop any rogue UBRN column (if any) from the input DF
    val noUbrn = df.drop(defaultUbrnColName)

    // Set the base UBRN for adding to the monotonic sequential value
    val base = baseUbrn.getOrElse(defaultBaseUbrn) + 1

    // Repartition to one partition so sequence is a fairly continuous range.
    // This will force data to be shuffled, which is inefficient.
    val numPartitions = df.rdd.getNumPartitions

    val df1partition = df.repartition(1)

    // Now add the new generated UBRN column and sequence value
    val df1partWithUbrn = df1partition.withColumn(defaultUbrnColName, monotonicallyIncreasingId + base)

    // Repartition back to original num partitions (more data shuffling)
    df1partWithUbrn.repartition(numPartitions)
  }

  def getNewLinksDataFromJson(reader: LinkJsonReader, appConfig: AppConfig): DataFrame = {
    // Get source/target directories
    val linksDataConfig = appConfig.LinksDataConfig
    val dataDir = linksDataConfig.dir
    val jsonfile = linksDataConfig.json
    val jsonFilePath = s"$dataDir/$jsonfile"

    // Load the JSON links data
    reader.readFromSourceFile(jsonFilePath)
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
    val appDataConfig = appConfig.AppDataConfig
    val linksFile = appDataConfig.links
    val prevDir = appDataConfig.prevDir
    val prevLinksFile = s"$prevDir/$ts/$linksFile"

    // We will also write a copy of the preprocessed Links data to the "previous" dir
    df.write.mode("overwrite").parquet(prevLinksFile)
  }

  def getChMatches(oldLinks: DataFrame, newLinks: DataFrame):DataFrame = {

    // Get old/new links where Company No is the same
    oldLinks.registerTempTable("old_links")
    newLinks.registerTempTable("new_links")

    val chMatchesDf = sqlContext.sql(
      """
        | SELECT old.UBRN AS UBRN, old.CH AS CH, new.VAT, new.PAYE
        | FROM old_links AS old LEFT JOIN new_links AS new ON (old.CH[0] = new.CH[0])
        | WHERE old.CH[0] IS NOT NULL AND new.CH[0] IS NOT NULL
      """.stripMargin)

    chMatchesDf
  }

  def getContentMatchesNoCh(oldLinks: DataFrame, newLinks: DataFrame):DataFrame = {

    // Get old/new links that have no Company No, but where other contents are same
    oldLinks.registerTempTable("old_links")
    newLinks.registerTempTable("new_links")

    // Exclude records where old/new contents are all empty
    val matchesDf = sqlContext.sql(
      """SELECT old.UBRN AS UBRN, old.CH AS CH, new.VAT, new.PAYE
        |FROM old_links AS old
        |LEFT JOIN new_links AS new ON (old.VAT = new.VAT AND old.PAYE = new.PAYE)
        |WHERE old.CH[0] IS NULL
        | AND new.CH[0] IS NULL
        | AND (new.VAT IS NOT NULL OR new.PAYE IS NOT NULL)
        | AND (old.VAT IS NOT NULL OR old.PAYE IS NOT NULL)
      """.stripMargin)

    matchesDf
  }

  def excludeMatches(newLinks: DataFrame, matches:DataFrame):DataFrame = {

    newLinks.registerTempTable("new_links")
    matches.registerTempTable("matches")

    sqlContext.sql(
      """
        | SELECT new.CH, new.VAT, new.PAYE
        | FROM new_links AS new
        | EXCEPT
        | SELECT m.CH, m.VAT, m.PAYE
        | FROM matches AS m
      """.stripMargin)
  }

  def combineLinksToSave(oldUbrn: DataFrame, newUbrn:DataFrame):DataFrame = {

    oldUbrn.registerTempTable("old_ubrn")
    newUbrn.registerTempTable("new_ubrn")

    sqlContext.sql(
      """
        | SELECT old.UBRN, old.CH, old.VAT, old.PAYE
        | FROM old_ubrn AS old
        | UNION ALL
        | SELECT m.UBRN, m.CH, m.VAT, m.PAYE
        | FROM new_ubrn AS m
      """.stripMargin)
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
    val previousLinksReader = new PreviousLinksReader(sc)
    val prevLinks = previousLinksReader.readFromSourceFile(prevLinksFileParquetPath)
    prevLinks.cache()

    // Get CH matches where CH is present in both sets
    val chMatchesDf: DataFrame = getChMatches(prevLinks, newLinks)
    chMatchesDf.cache()

    // Remove first set of matches from new links so we can reduce sdaerching?
    val newLinksCut1 = excludeMatches(newLinks, chMatchesDf)
    newLinksCut1.cache()

    // Get records where CH is absent from both sets but other contents are same
    val contentMatchesDf: DataFrame = getContentMatchesNoCh(prevLinks, newLinksCut1)
    contentMatchesDf.cache()

    // Additional matching rules will need to be applied in here?
    // - take another cut of new links to eliminate ones we just matched
    // ...
    // val newLinksCut2 = excludeMatches(newLinksCut1, contentMatchesDf)
    // newLinksCut2.cache()
    // newinksCut1.unpersist()
    // ...
    // - apply new rule to latest cut of new links...
    // ...

    // Build set of all new links that can use old UBRN
    // i.e. CH matches + content matches (+ any other matches?)
    val useOldUbrnDf: DataFrame = chMatchesDf.unionAll(contentMatchesDf)

    // Find the new links that have NOT been matched and will need a new UBRN
    // i.e. new links that do NOT occur in the "matches" we identified
    val needNewUbrnDf: DataFrame = excludeMatches(newLinks, useOldUbrnDf)

    // Set new UBRN for these (start from max value from previous links)
    val maxUrbn = getMaxUbrn(prevLinks)
    val withNewUbrn: DataFrame = applyNewUbrn(needNewUbrnDf, maxUrbn)

    // Reconstruct full set of Links so we can save them all to Parquet
    val linksToSave = combineLinksToSave(useOldUbrnDf, withNewUbrn)

    // Cache the results because we want to write them to multiple files
    linksToSave.cache()

    // Clear other cached data we no longer need
    contentMatchesDf.unpersist()
    chMatchesDf.unpersist()
    prevLinks.unpersist()
    newLinks.unpersist()

    // Write preprocessed Links data to a Parquet output file ready for subsequent processing
    jsonReader.writeParquet(linksToSave, newLinksFileParquetPath)

    // We will also write a copy of the new preprocessed Links data to the "previous" dir:
    // 1. As e.g. LINKS_Output.parquet so we can easily pick it up next time
    writeAsPrevLinks(appConfig, linksToSave)

    // 2. As e.g. 201703081145/LINKS_Output.parquet so it does not get over-written later
    writeAsPrevLinks(appConfig, linksToSave, true)

    // Clear cached data we no longer need
    linksToSave.unpersist()
  }

}
