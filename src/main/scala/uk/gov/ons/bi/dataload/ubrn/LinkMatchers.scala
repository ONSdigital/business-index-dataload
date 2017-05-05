package uk.gov.ons.bi.dataload.ubrn

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import uk.gov.ons.bi.dataload.model.BiSparkDataFrames
import com.google.inject.Singleton
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import uk.gov.ons.bi.dataload.utils.ContextMgr

import scala.util.{Failure, Success, Try}

/**
  * Created by websc on 16/03/2017.
  */

case class LinkMatchResults(unmatchedOldLinks: DataFrame, unmatchedNewLinks: DataFrame, matched: DataFrame)

@Singleton
class LinkMatcher(ctxMgr: ContextMgr) {

  val sc = ctxMgr.sc
  val sqlContext = ctxMgr.sqlContext

  import sqlContext.implicits._

  def isDfEmpty(df: DataFrame): Boolean = {
    // If no first record, then it's empty
    Try {
      df.first()
    }
    match {
      case Success(t) => false
      case Failure(x) => true
    }
  }

  def excludeMatches(oldLinks: DataFrame, newLinks: DataFrame, matched: DataFrame): LinkMatchResults = {
    // Exclude matched UBRNs from oldLinks, and exclude matched GIDs from newLinks.
    // Return the unmatched sub-sets for old and new, plus matched set.

    // If no data was matched, just return the original old/new sub-sets
    val results: LinkMatchResults =
      if (isDfEmpty(matched))
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
      if (isDfEmpty(oldLinks)) {
        BiSparkDataFrames.emptyMatchedLinkWithUbrnGidDf(sc, sqlContext)
      }
      else {

        // Set these each time
        oldLinks.registerTempTable("old_links")
        newLinks.registerTempTable("new_links")
        // Execute SQL rule and get matched records
        sqlContext.sql(matchQuery)
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


  def getVatMatches(oldLinks: DataFrame, newLinks: DataFrame): LinkMatchResults = {
    /*
    * VAT matching logic:
    *
    * Take unmatched records.
    * Explode VAT arrays to create old/new Links with a single VAT reference in each.
    * Match on the individual VAT references.
    * It is possible that one new Link GID might match >1 old Link UBRN, or vice versa.
    * We therefore rank the old and new VATs (in order of VAT reference) within each UBRN or GID.
    * Then we take UBRN and GID for the first match only.
    */

    val matchQuery =
      """
              SELECT t1.UBRN, t1.GID, un.CH, un.VAT, un.PAYE
 |                FROM (
 |                SELECT exp_old.UBRN, exp_new.GID,
 |                        dense_rank() OVER (PARTITION BY exp_old.UBRN ORDER BY exp_old.exploded_vat) as rank_in_ubrn,
 |                        dense_rank() OVER (PARTITION BY exp_new.GID ORDER BY exp_new.exploded_vat) as rank_in_gid
 |                FROM
 |                (SELECT GID, CH, explode(VAT) AS exploded_vat FROM new_links) AS exp_new
 |                INNER JOIN
 |                (SELECT UBRN, CH, explode(VAT) AS exploded_vat FROM old_links) AS exp_old
 |                 ON (exp_old.exploded_vat = exp_new.exploded_vat)
 |                 ) t1 INNER JOIN new_links AS un ON (un.GID = t1.GID)
 |                 WHERE  t1.rank_in_ubrn = 1
 |                 AND t1.rank_in_gid = 1
               """.stripMargin

    applySqlRule(matchQuery, oldLinks, newLinks)
  }


  def getPayeMatches(oldLinks: DataFrame, newLinks: DataFrame): LinkMatchResults = {

    /*
    * PAYE matching logic:
    *
    * Take unmatched records.
    * Explode PAYE arrays to create old/new Links with a single PAYE reference in each.
    * Match on the individual PAYE references.
    * It is possible that one new Link GID might match >1 old Link UBRN, or vice versa.
    * We therefore rank the old and new PAYEs (in order of PAYE reference) within each UBRN or GID.
    * Then we take UBRN and GID for the first match only.
   */

    val matchQuery =
      """
        SELECT t1.UBRN, t1.GID, un.CH, un.VAT, un.PAYE
 |                FROM (
 |                SELECT exp_old.UBRN, exp_new.GID,
 |                        dense_rank() OVER (PARTITION BY exp_old.UBRN ORDER BY exp_old.exploded_paye) as rank_in_ubrn,
 |                        dense_rank() OVER (PARTITION BY exp_new.GID ORDER BY exp_new.exploded_paye) as rank_in_gid
 |                FROM
 |                (SELECT GID, CH, explode(PAYE) as exploded_paye FROM new_links) AS exp_new
 |                INNER JOIN
 |                (SELECT UBRN, CH, explode(PAYE) as exploded_paye FROM old_links) AS exp_old
 |                 ON (exp_old.exploded_paye = exp_new.exploded_paye)
 |                 ) t1 INNER JOIN new_links AS un ON (un.GID = t1.GID)
 |                 WHERE  t1.rank_in_ubrn = 1
 |                 AND t1.rank_in_gid = 1
               """.stripMargin

    applySqlRule(matchQuery, oldLinks, newLinks)
  }

  def combineLinksToSave(linksWithUbrn1: DataFrame, linksWithUbrn2: DataFrame): DataFrame = {
    linksWithUbrn1.select("UBRN", "CH", "VAT", "PAYE")
      .unionAll(linksWithUbrn2.select("UBRN", "CH", "VAT", "PAYE"))
  }

  def applyAllMatchingRules(newLinks: DataFrame, oldLinks: DataFrame): (DataFrame, DataFrame) = {
    // Each rule eliminates matching records from the set of links we still have to match,
    // so each step should have a smaller search space.
    // Cache intermediate sets temporarily so we don't have to keep re-materialising them.

    // Get CH matches where CH is present in both sets
    val chResults = getChMatches(oldLinks, newLinks)

    // Cache results as they will be re-used below
    chResults.unmatchedOldLinks.cache()
    chResults.unmatchedNewLinks.cache()
    chResults.matched.cache()

    // Get records where CH is absent from both sets but other contents are same
    val contentResults = getContentMatchesNoCh(chResults.unmatchedOldLinks, chResults.unmatchedNewLinks)

    // Reset cached data

    contentResults.unmatchedOldLinks.cache()
    contentResults.unmatchedNewLinks.cache()
    contentResults.matched.cache()

    chResults.unmatchedOldLinks.unpersist()
    chResults.unmatchedNewLinks.unpersist()

    // Get records where VAT ref matches
    val vatResults = getVatMatches(contentResults.unmatchedOldLinks, contentResults.unmatchedNewLinks)

    // Reset cached data

    vatResults.unmatchedOldLinks.cache()
    vatResults.unmatchedNewLinks.cache()
    vatResults.matched.cache()

    contentResults.unmatchedOldLinks.unpersist()
    contentResults.unmatchedNewLinks.unpersist()

    // Get records where PAYE ref matches
    val payeResults = getPayeMatches(vatResults.unmatchedOldLinks, vatResults.unmatchedNewLinks)

    // Reset cached data

    payeResults.unmatchedOldLinks.cache()
    payeResults.unmatchedNewLinks.cache()
    payeResults.matched.cache()

    vatResults.unmatchedOldLinks.unpersist()
    vatResults.unmatchedNewLinks.unpersist()

    // Finally we should have:
    // - one sub-set of new links that we have matched, so they now have a UBRN:
    val withOldUbrn: DataFrame = chResults.matched
      .unionAll(contentResults.matched)
      .unionAll(vatResults.matched)
      .unionAll(payeResults.matched)

    withOldUbrn.cache()

    // - and one sub-set of new links that we could not match, so they need new UBRN:
    val needUbrn: DataFrame = payeResults.unmatchedNewLinks
    needUbrn.cache()

    // Clear remaining cached data
    vatResults.matched.unpersist()
    payeResults.matched.unpersist()
    chResults.matched.unpersist()
    contentResults.matched.unpersist()
    payeResults.unmatchedNewLinks.unpersist()
    payeResults.unmatchedOldLinks.unpersist()

    // Return the stuff we want

    (withOldUbrn, needUbrn)
  }

  def processNewOldLinks(newLinks: DataFrame, oldLinks: DataFrame): (DataFrame, DataFrame) = {
    // We can skip all the checks if the old set is empty
    if (isDfEmpty(oldLinks))
      (BiSparkDataFrames.emptyLinkWithUbrnDf(sc, sqlContext), newLinks)
    else
      applyAllMatchingRules(newLinks, oldLinks)
  }

}