package uk.gov.ons.bi.dataload.ubrn

import java.util.UUID

import com.google.inject.Singleton

import uk.gov.ons.bi.dataload.reader.{BIDataReader, PreviousLinkStore}
import uk.gov.ons.bi.dataload.writer.{BiParquetWriter, PreviousLinksWriter}
import uk.gov.ons.bi.dataload.utils.ContextMgr
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
  * Created by websc on 03/03/2017.
  */

@Singleton
class LinksPreprocessor(ctxMgr: ContextMgr) extends PreviousLinkStore(ctxMgr) with BIDataReader {

  // Create UDF to generate a UUID
  val generateUuid: UserDefinedFunction = udf(() => UUID.randomUUID().toString)

  def readNewLinks(linksFilePath: String): DataFrame = {
    // Load the JSON links data
    readFromSourceFile(linksFilePath)
  }

  def readPrevLinks(prevDir: String, linksFile: String): DataFrame = {
    val prevLinksFile = s"$prevDir/$linksFile"
    readFromSourceFile(prevLinksFile)
  }

  def preProcessLinks(newLinksDF: DataFrame, prevLinks: DataFrame) = {

    // WARNING:
    // UUID is generated when data is materialised e.g. in a SELECT statement,
    // so we need to PERSIST this data once we've added GID to fix it in place.
    val newLinks = newLinksDF.withColumn("GID", generateUuid())
    newLinks.persist(StorageLevel.MEMORY_AND_DISK)

    // Initialise LinkMatcher
    val matcher = new LinkMatcher(ctxMgr)

    // Apply all matching rules and get (matched, unmatched) records back
    val (withOldUbrn, needUbrn) = matcher.applyAllMatchingRules(newLinks, prevLinks)

    val maxUrbn = UbrnManager.getMaxUbrn(prevLinks)

    //Set new UBRN for these (start from max value from previous links)
    val withNewUbrn: DataFrame = UbrnManager.applyNewUbrn(needUbrn, maxUrbn)

    // Finally, reconstruct full set of Links so we can save them all to Parquet
    val linksToSave = matcher.combineLinksToSave(withOldUbrn, withNewUbrn)
    // Cache the results because we want to write them to multiple files
    linksToSave.persist(StorageLevel.MEMORY_AND_DISK)
  }

  def writeToParquet(prevDir: String, workingDir: String, linksFile: String, linksToSave: DataFrame) = {

    val outputPath = s"$workingDir/$linksFile"

    // Parquet file locations from configuration (or runtime params)
    BiParquetWriter.writeParquet(linksToSave, outputPath)

    // We will also write a copy of the new preprocessed Links data to the "previous" dir:
    // 1. As e.g. LINKS_Output.parquet so we can easily pick it up next time
    PreviousLinksWriter.writeAsPrevLinks(prevDir, linksFile, linksToSave)

    // 2. As e.g. 201703081145/LINKS_Output.parquet so it does not get over-written later
    PreviousLinksWriter.writeAsPrevLinks(prevDir, linksFile, linksToSave, true)
  }
}