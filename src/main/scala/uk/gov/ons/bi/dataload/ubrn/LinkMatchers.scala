package uk.gov.ons.bi.dataload.ubrn

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import uk.gov.ons.bi.dataload.model.BiSparkDataFrames
import com.google.inject.Singleton

/**
  * Created by websc on 16/03/2017.
  */

@Singleton
class LinkMatcher(sc: SparkContext) {

  // use existing SQLContext is available
  val sqlContext = SQLContext.getOrCreate(sc)

  def getChMatches(oldLinks: DataFrame, newLinks: DataFrame): DataFrame = {

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

  def getContentMatchesNoCh(oldLinks: DataFrame, newLinks: DataFrame): DataFrame = {

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

  def excludeMatches(newLinks: DataFrame, matches: DataFrame): DataFrame = {

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

  def combineLinksToSave(linksWithUbrn1: DataFrame, linksWithUbrn2: DataFrame): DataFrame = {

    linksWithUbrn1.registerTempTable("links1")
    linksWithUbrn2.registerTempTable("links2")

    sqlContext.sql(
      """
        | SELECT old.UBRN, old.CH, old.VAT, old.PAYE
        | FROM links1 AS old
        | UNION ALL
        | SELECT m.UBRN, m.CH, m.VAT, m.PAYE
        | FROM links2 AS m
      """.stripMargin)
  }

  def simpleMatches(newLinks: DataFrame, oldLinks: DataFrame): (DataFrame, DataFrame) = {

    // Get CH matches where CH is present in both sets
    val chMatchesDf: DataFrame = getChMatches(oldLinks, newLinks)

    // Remove the CH=CH matches from new links so we can reduce searching
    val newLinksCut1 = excludeMatches(newLinks, chMatchesDf)

    // Get records where CH is absent from both sets but other contents are same
    val contentMatchesDf: DataFrame = getContentMatchesNoCh(oldLinks, newLinksCut1)

    // Exclude the newly matched records from the set we still have to match
    val unmatched = excludeMatches(newLinksCut1, contentMatchesDf)

    // Build single set of records that we've matched so far (with UBRNs)
    val matched = combineLinksToSave(chMatchesDf, contentMatchesDf)

    // Return (matched new links with UBRNs, unmatched new links)
    (matched, unmatched)
  }

  def matchedOnOtherRules(newLinks: DataFrame, oldLinks: DataFrame): (DataFrame, DataFrame) = {
    // EXTRA RULES NOT YET DEFINED

    // ----------------
    // IMPLEMENT EXTRA MATCHING RULES HERE ...
    // ----------------

    // return an empty "matched" set and incoming new links (unmatched) for now
    val matched = BiSparkDataFrames.emptyLinkWithUbrnDf(sc, sqlContext)
    val unmatched = newLinks

    // Return (matched = new links with UBRNs, unmatched = new links we have not yet matched)
    (matched, unmatched)
  }

  def applyAllMatchingRules(newLinks: DataFrame, oldLinks: DataFrame): (DataFrame, DataFrame) = {
    // Get easy matches first (CH=CH, or no CH but other contents same)
    val (simpleChMatches, unmatched1) = simpleMatches(newLinks, oldLinks)
    // ----------------
    // CALL EXTRA MATCHING RULES HERE ...
    // ----------------
    // Only try to match records that we have not already matched
    val (otherMatches, unmatched2) = matchedOnOtherRules(unmatched1, oldLinks)

    // ...
    // Finally we should have :
    // - one sub-set of new links that we have matched, so they now have a UBRN:
    val allMatched: DataFrame = combineLinksToSave(simpleChMatches, otherMatches)
    // - and one sub-set of new links that we could not match, so they need new UBRN:
    val needUbrn: DataFrame = unmatched2

    (allMatched, needUbrn)
  }

}