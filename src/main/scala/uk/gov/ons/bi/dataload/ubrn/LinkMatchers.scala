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

  def simpleCompaniesHouseMatches(newLinks: DataFrame, oldLinks: DataFrame): (DataFrame, DataFrame) = {

    // Get CH matches where CH is present in both sets
    val chMatchesDf: DataFrame = getChMatches(oldLinks, newLinks)

    // Remove first set of matches from new links so we can reduce searching?
    val newLinksCut1 = excludeMatches(newLinks, chMatchesDf)

    // Get records where CH is absent from both sets but other contents are same
    val contentMatchesDf: DataFrame = getContentMatchesNoCh(oldLinks, newLinksCut1)

    // Build set of records that we've matched so far (with UBRNs)
    val matched = combineLinksToSave(chMatchesDf, contentMatchesDf)
    // and the ones we haven't matched
    val unmatched = excludeMatches(newLinks, matched)
    // Return (matched new links with UBRNs, unmatched new links)
    (matched, unmatched)
  }

  def matchedOnOtherRules(newLinks: DataFrame, oldLinks: DataFrame) = {
    // EXTRA RULES NOT YET DEFINED

    // return an empty "matched" set and incoming new links (unmatched)for now
    val matched = BiSparkDataFrames.emptyLinkWithUbrnDf(sc, sqlContext)
    val unmatched = newLinks

    // Return (matched new links with UBRNs, unmatched new links)
    (matched, unmatched)
  }
}