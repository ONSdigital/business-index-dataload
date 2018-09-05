package uk.gov.ons.bi.dataload

import org.apache.spark.sql.SparkSession

import uk.gov.ons.bi.dataload.utils.ContextMgr

trait SparkCreator {

  val spark = SparkSession.builder.master("local").appName("Business Index").getOrCreate()
  val ctxMgr = new ContextMgr(spark)
}