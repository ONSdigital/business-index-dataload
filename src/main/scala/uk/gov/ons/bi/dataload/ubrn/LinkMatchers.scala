package uk.gov.ons.bi.dataload.ubrn

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import uk.gov.ons.bi.dataload.model.BiSparkDataFrames
import com.google.inject.Singleton
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import uk.gov.ons.bi.dataload.utils.ContextMgr

/**
  * Created by websc on 16/03/2017.
  */

case class LinkMatchResults(unmatchedOldLinks: DataFrame, unmatchedNewLinks: DataFrame, matched: DataFrame)

@Singleton
class LinkMatcher(ctxMgr: ContextMgr) {

  val sc = ctxMgr.sc
  val sqlContext = ctxMgr.sqlContext

  import sqlContext.implicits._

  def excludeMatches(oldLinks: DataFrame, newLinks: DataFrame, matched: DataFrame): LinkMatchResults = {
    // Exclude matched UBRNs from oldLinks, and exclude matched GIDs from newLinks.
    // Return the unmatched sub-sets for old and new, plus matched set.
    val unmatchedOldUbrns = oldLinks.select("UBRN").except(matched.select("UBRN"))
    val unmatchedNewGids = newLinks.select("GID").except(matched.select("GID"))

    val unmatchedOldLinks = unmatchedOldUbrns.join(oldLinks, usingColumn = "UBRN").select("UBRN", "CH", "VAT", "PAYE")
    val unmatchedNewLinks = unmatchedNewGids.join(newLinks, usingColumn = "GID").select("GID", "CH", "VAT", "PAYE")

    LinkMatchResults(unmatchedOldLinks, unmatchedNewLinks, matched)
  }

  def getChMatches(oldLinks: DataFrame, newLinks: DataFrame): LinkMatchResults = {
    // Apply rule to get current matches
    // Get old/new links where Company No is the same
    oldLinks.registerTempTable("ch_old")
    newLinks.registerTempTable("ch_new")

    val matched = sqlContext.sql(
      """
        | SELECT old.UBRN AS UBRN, new.GID AS GID, new.CH AS CH, new.VAT, new.PAYE
        | FROM ch_old AS old LEFT JOIN ch_new AS new ON (old.CH[0] = new.CH[0])
        | WHERE old.CH[0] IS NOT NULL AND new.CH[0] IS NOT NULL
      """.stripMargin)

    // Remove matched records from the sets we need to process next time
    excludeMatches(oldLinks, newLinks, matched)
  }


  def getContentMatchesNoCh(oldLinks: DataFrame, newLinks: DataFrame): LinkMatchResults = {
    // Every time we match a record, we want to eliminate it from the sets of
    // old/new links that we use for the next set of checks.

    // Get old/new links where there is no Company No but other contents are same
    oldLinks.registerTempTable("con_old")
    newLinks.registerTempTable("con_new")

    val matched = sqlContext.sql(
      """SELECT old.UBRN AS UBRN, new.GID as GID, old.CH AS CH, new.VAT, new.PAYE
        |FROM con_old AS old
        |LEFT JOIN con_new AS new ON (old.VAT = new.VAT AND old.PAYE = new.PAYE)
        |WHERE old.CH[0] IS NULL
        | AND new.CH[0] IS NULL
        | AND (new.VAT IS NOT NULL OR new.PAYE IS NOT NULL)
        | AND (old.VAT IS NOT NULL OR old.PAYE IS NOT NULL)
      """.stripMargin)

    // Remove matched records from the sets we need to process next time
    excludeMatches(oldLinks, newLinks, matched)
  }


  /*
    def getVatMatches(oldLinks: DataFrame, newLinks: DataFrame): LinkMatchResults = {

      oldLinks.registerTempTable("unmatched_old")
      newLinks.registerTempTable("unmatched_new")

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

      // Window functions allow us to rank data within GID or UBRN
      val partitionByUbrnOrderByVat = org.apache.spark.sql.expressions.Window.partitionBy("UBRN").orderBy("exploded_vat")
      val partitionByGidOrderByVat = org.apache.spark.sql.expressions.Window.partitionBy("GID").orderBy("exploded_vat")

      val oldLinksRanked = oldLinks.select("UBRN","CH","VAT","PAYE")
        .withColumn("exploded_vat", explode(oldLinks("VAT"))).as("exploded_vat")
        .withColumn("rank_in_ubrn", dense_rank().over(partitionByUbrnOrderByVat))

      val newLinksRanked = newLinks.select("GID","CH","VAT","PAYE")
        .withColumn("exploded_vat", explode(newLinks("VAT"))).as("exploded_vat")
        .withColumn("rank_in_gid", dense_rank().over(partitionByGidOrderByVat))

      val matchIds = newLinksRanked.filter("rank_in_gid = 1")
        .join(oldLinksRanked.filter("rank_in_ubrn = 1"),newLinksRanked("exploded_vat") === oldLinksRanked("exploded_vat"))
        .select("UBRN","GID")

      val matched = matchIds.join(newLinks, "GID")

      // Remove matched records from the sets we need to process next time
      excludeMatches(oldLinks, newLinks, matched)
    }
  */
  def getVatMatches(oldLinks: DataFrame, newLinks: DataFrame): LinkMatchResults = {

    oldLinks.registerTempTable("unmatched_old")
    newLinks.registerTempTable("unmatched_new")

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

    val q =
      """
                SELECT t1.UBRN, t1.GID, un.CH, un.VAT, un.PAYE
                FROM (
                SELECT exp_old.UBRN, exp_new.GID,
                        dense_rank() OVER (PARTITION BY exp_old.UBRN ORDER BY exp_old.exploded_vat) as rank_in_ubrn,
                        dense_rank() OVER (PARTITION BY exp_new.GID ORDER BY exp_new.exploded_vat) as rank_in_gid
                FROM
                (SELECT GID, CH, explode(VAT) AS exploded_vat FROM unmatched_new) AS exp_new
                LEFT JOIN
                (SELECT UBRN, CH, explode(VAT) AS exploded_vat FROM unmatched_old) AS exp_old
                 ON (exp_old.exploded_vat = exp_new.exploded_vat)
                 ) t1 LEFT JOIN unmatched_new AS un ON (un.GID = t1.GID)
                 WHERE  t1.rank_in_ubrn = 1
                 AND t1.rank_in_gid = 1
               """.stripMargin
    val matched = sqlContext.sql(q)

    // Remove matched records from the sets we need to process next time
    excludeMatches(oldLinks, newLinks, matched)
  }


  def getPayeMatches(oldLinks: DataFrame, newLinks: DataFrame): LinkMatchResults = {

    oldLinks.registerTempTable("unmatched_old")
    newLinks.registerTempTable("unmatched_new")

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

    val q =
      """
                SELECT t1.UBRN, t1.GID, un.CH, un.VAT, un.PAYE
                FROM (
                SELECT exp_old.UBRN, exp_new.GID,
                        dense_rank() OVER (PARTITION BY exp_old.UBRN ORDER BY exp_old.exploded_paye) as rank_in_ubrn,
                        dense_rank() OVER (PARTITION BY exp_new.GID ORDER BY exp_new.exploded_paye) as rank_in_gid
                FROM
                (SELECT GID, CH, explode(PAYE) as exploded_paye FROM unmatched_new) AS exp_new
                LEFT JOIN
                (SELECT UBRN, CH, explode(PAYE) as exploded_paye FROM unmatched_old) AS exp_old
                 ON (exp_old.exploded_paye = exp_new.exploded_paye)
                 ) t1 LEFT JOIN unmatched_new AS un ON (un.GID = t1.GID)
                 WHERE  t1.rank_in_ubrn = 1
                 AND t1.rank_in_gid = 1
               """.stripMargin
    val matched = sqlContext.sql(q)

    // Remove matched records from the sets we need to process next time
    excludeMatches(oldLinks, newLinks, matched)
  }

  def combineLinksToSave(linksWithUbrn1: DataFrame, linksWithUbrn2: DataFrame): DataFrame = {
    linksWithUbrn1.select("UBRN", "CH", "VAT", "PAYE")
      .unionAll(linksWithUbrn2.select("UBRN", "CH", "VAT", "PAYE"))
  }

  def applyAllMatchingRules(newLinks: DataFrame, oldLinks: DataFrame): (DataFrame, DataFrame) = {
    // Each rule eliminates matching records from the set of links we still have to match,
    // so each step should have a smaller search space.

    // Get CH matches where CH is present in both sets
    val chResults = getChMatches(oldLinks, newLinks)
    val chMatchesDf: DataFrame = chResults.matched

    chMatchesDf.cache()

    // Get records where CH is absent from both sets but other contents are same
    val contentMatchResults = getContentMatchesNoCh(chResults.unmatchedOldLinks, chResults.unmatchedNewLinks)
    val contentMatchesDf: DataFrame = contentMatchResults.matched

    contentMatchesDf.cache()

    // Get records where VAT ref matches
    val vatMatchResults = getVatMatches(contentMatchResults.unmatchedOldLinks, contentMatchResults.unmatchedNewLinks)
    val vatMatchesDf: DataFrame = vatMatchResults.matched

    vatMatchesDf.cache()

    // Get records where PAYE ref matches
    val payeMatchResults = getPayeMatches(vatMatchResults.unmatchedOldLinks, vatMatchResults.unmatchedNewLinks)
    val payeMatchesDf: DataFrame = payeMatchResults.matched

    payeMatchesDf.cache()

    // Finally we should have:
    // - one sub-set of new links that we have matched, so they now have a UBRN:
    val allMatched: DataFrame = chMatchesDf
      .unionAll(contentMatchesDf)
      .unionAll(vatMatchesDf)
      .unionAll(payeMatchesDf)

    // - and one sub-set of new links that we could not match, so they need new UBRN:
    val needUbrn: DataFrame = payeMatchResults.unmatchedNewLinks

    vatMatchesDf.unpersist()
    payeMatchesDf.unpersist()
    chMatchesDf.unpersist()
    contentMatchesDf.unpersist()

    (allMatched, needUbrn)
  }

}