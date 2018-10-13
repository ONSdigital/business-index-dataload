package uk.gov.ons.bi.dataload.ubrn

import java.util.UUID

import com.google.inject.Singleton
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import uk.gov.ons.bi.dataload.model.BiSparkDataFrames
import uk.gov.ons.bi.dataload.utils.ContextMgr

// HiveContext will be needed for fancy SQL in PAYE and VAT matching rules

case class LinkMatchResults(unmatchedOldLinks: DataFrame, unmatchedNewLinks: DataFrame, matched: DataFrame)

@Singleton
class LinkMatcher(ctxMgr: ContextMgr) {

  val sc = ctxMgr.sc
  val spark = ctxMgr.spark


  def excludeMatches(oldLinks: DataFrame, newLinks: DataFrame, matched: DataFrame): LinkMatchResults = {
    // Exclude matched UBRNs from oldLinks, and exclude matched GIDs from newLinks.
    // Return the unmatched sub-sets for old and new, plus matched set.

    // If no data was matched, just return the original old/new sub-sets
    val results: LinkMatchResults =
      if (BiSparkDataFrames.isDfEmpty(matched))
        LinkMatchResults(oldLinks, newLinks, matched)
      else {
        val unmatchedOldUbrns = oldLinks.select("UBRN").except(matched.select("UBRN"))
        val unmatchedNewGids = newLinks.select("GID").except(matched.select("GID"))

        val unmatchedOldLinks = unmatchedOldUbrns.join(oldLinks, usingColumn = "UBRN").select("UBRN", "CH", "VAT", "PAYE")
        val unmatchedNewLinks = unmatchedNewGids.join(newLinks, usingColumn = "GID").select("GID", "CH", "VAT", "PAYE")

        LinkMatchResults(unmatchedOldLinks, unmatchedNewLinks, matched)
      }
    results
  }

  def applySqlRule(matchQuery: String, oldLinks: DataFrame, newLinks: DataFrame) = {
    // Short-circuit to skip query if old frame is empty, as no matches will exist
    val matched: DataFrame =
      if (BiSparkDataFrames.isDfEmpty(oldLinks)) {
        BiSparkDataFrames.emptyMatchedLinkWithUbrnGidDf(ctxMgr)
      }
      else {

        // Set these each time
        oldLinks.createOrReplaceTempView("old_links")
        newLinks.createOrReplaceTempView("new_links")
        // Execute SQL rule and get matched records
        spark.sql(matchQuery)
      }
    // Remove matched data from sets of data to be processed
    excludeMatches(oldLinks, newLinks, matched)
  }

  def getChMatches(oldLinks: DataFrame, newLinks: DataFrame): LinkMatchResults = {
    val matchQuery =
      """
       SELECT p.UBRN AS UBRN,c.GID, c.CH, c.VAT, c.PAYE
    FROM old_links AS p
    INNER JOIN new_links AS c ON (p.CH = c.CH)
    WHERE p.CH IS NOT NULL
    AND c.CH IS NOT NULL
    AND p.CH[0] IS NOT NULL
    AND c.CH[0] IS NOT NULL
          """.stripMargin

    applySqlRule(matchQuery, oldLinks, newLinks)
  }


  def getContentMatchesNoCh(oldLinks: DataFrame, newLinks: DataFrame): LinkMatchResults = {
    // Different rule for each matching process
    val matchQuery =
      """
        SELECT old.UBRN AS UBRN, new.GID as GID, new.CH, new.VAT, new.PAYE
        |FROM old_links AS old
        |INNER JOIN new_links AS new ON (old.VAT = new.VAT AND old.PAYE = new.PAYE)
        |WHERE old.CH[0] IS NULL
        | AND new.CH[0] IS NULL
        | AND (new.VAT IS NOT NULL OR new.PAYE IS NOT NULL)
        | AND (old.VAT IS NOT NULL OR old.PAYE IS NOT NULL)
      """.stripMargin

    applySqlRule(matchQuery, oldLinks, newLinks)
  }

  def combineLinksToSave(linksWithUbrn1: DataFrame, linksWithUbrn2: DataFrame): DataFrame = {
    linksWithUbrn1.select("UBRN", "CH", "VAT", "PAYE")
      .union(linksWithUbrn2.select("UBRN", "CH", "VAT", "PAYE"))
  }

  def applyAllMatchingRules(newLinks: DataFrame, oldLinks: DataFrame, vatPath: String, payePath: String) = {

    // write new method for matching CH takes priority on UBRN

    val complex = getComplex(newLinks, oldLinks, vatPath, payePath)
    val matched = excludeMatches(oldLinks, newLinks, complex)

    val withOldUbrn = matched.matched.select("UBRN","GID","CH","VAT","PAYE")
    val needUbrn = matched.unmatchedNewLinks

    (withOldUbrn, needUbrn)
  }

  def applyAllMatchingRulesOld(newLinks: DataFrame, oldLinks: DataFrame, vatPath: String, payePath: String) = {

    val chResults = getChMatches(oldLinks, newLinks)

    val complex = getComplex(chResults.unmatchedNewLinks, chResults.unmatchedOldLinks, vatPath, payePath)
    val matched = excludeMatches(chResults.unmatchedOldLinks, chResults.unmatchedNewLinks, complex)

    val withOldUbrn = chResults.matched.union(matched.matched.select("UBRN","GID","CH","VAT","PAYE"))
    val needUbrn = matched.unmatchedNewLinks

    (withOldUbrn, needUbrn)
  }

  def getComplex(newLinks: DataFrame, oldLinks: DataFrame, vatPath: String, payePath: String): DataFrame = {

    val prevBirth = getPrevBirth(oldLinks, vatPath, payePath)
    val newBirth = getNewBirth(newLinks, vatPath, payePath)
    val birthMatched = newBirth.join(prevBirth, expr("array_contains(collected_Units,oldest_unit)"))
    birthMatched
  }

  def getPrevBirth(df: DataFrame, vatPath: String, payePath: String) = {

    // get birthdate of admin unit
    val (vatBirth, payeBirth) = assignBirth(df, vatPath, payePath)

    // for each UBRN assign oldest admin unit to it
      vatBirth
        .union(payeBirth)
        .groupBy("UBRN").agg(min("timestamp").as("timestamp"),
          min("vatref").as("oldest_unit")
      )
  }

  def getNewBirth(df: DataFrame, vatPath: String, payePath: String) = {

    // get birthdate of admin unit
    val (vatBirth, payeBirth) = assignBirth(df, vatPath, payePath)

    val unionDF = vatBirth.union(payeBirth).sort("timestamp")

    // concat admin units based on ID
      unionDF
        .groupBy("GID")
        .agg(
          collect_list("vatref") as "collected_Units",
          collect_list("timestamp") as "collect_Time",
          min("CH").as("CH"),
          min("VAT").as("VAT"),
          min("PAYE").as("PAYE")
        )
  }

  // ask about last period's birthdate for admin units since new month will have updated admin unit files
  def assignBirth(df: DataFrame, vatPath: String, payePath: String) = {

    // explode admin units
    val vat = df.withColumn("vatref", explode(df("VAT")))//.drop("CH", "VAT", "PAYE")
    val paye = df.withColumn("payeref", explode(df("PAYE")))//.drop("CH","VAT", "PAYE")

    // read in VAT and PAYE
    val pattern = "dd/MM/yyyy"
    val vatDf = spark.read.option("header", "true").csv(vatPath).select("vatref", "birthdate")
    val payeDf = spark.read.option("header", "true").csv(payePath).select("payeref", "birthdate")

    val vatStamp = vatDf.withColumn("timestamp", unix_timestamp(vatDf("birthdate"), pattern).cast("timestamp"))
    val payeStamp = payeDf.withColumn("timestamp", unix_timestamp(payeDf("birthdate"), pattern).cast("timestamp"))

    val vatWithBirth = vat.join(vatStamp, "vatref")
    val payeWithBirth = paye.join(payeStamp, "payeref")

    (vatWithBirth, payeWithBirth)
  }

}